package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// datasetArchive returns a tar.gz with one incompressible mail file of size n.
func datasetArchive(t *testing.T, n int) []byte {
	t.Helper()
	body := make([]byte, n)
	_, _ = rand.Read(body)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "maildir/allen-p/inbox/1.", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(n)}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func post(t *testing.T, client *http.Client, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return resp.StatusCode, string(body)
}

// Scenario: Fresh download and extraction, through the wired service.
func TestRunFetchesTheDataset(t *testing.T) {
	t.Parallel()
	archive := datasetArchive(t, 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(archive)))
		_, _ = w.Write(archive)
	}))
	defer srv.Close()
	dataDir := t.TempDir()

	base, _, cancel, done, out := startRun(t, map[string]string{
		"HTTP_ADDR": "127.0.0.1:0", "DATA_DIR": dataDir, "DATASET_URL": srv.URL,
	})

	client := newClient()
	if code, body := post(t, client, base+"/fetch"); code != http.StatusAccepted || !strings.Contains(body, `"state":"downloading"`) {
		t.Fatalf("POST /fetch = %d %s, want 202 downloading", code, body)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, body := post(t, client, base+"/fetch")
		if code == http.StatusOK && strings.Contains(body, `"state":"ready"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dataset never became ready: %d %s\n%s", code, body, out.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "maildir", "allen-p", "inbox", "1.")); err != nil {
		t.Errorf("extracted file: %v", err)
	}
	if _, body := get(t, client, base+"/metrics"); !strings.Contains(body, `enron_fetch_state{state="ready"} 1`) {
		t.Errorf("/metrics does not report enron_fetch_state ready")
	}

	cancel()
	if code := <-done; code != 0 {
		t.Errorf("exit code = %d, want 0:\n%s", code, out.String())
	}
}

// Scenario: Shutdown during download. Canceling run's context (what SIGTERM
// does in main) cancels the download and removes the temporary directory.
func TestRunShutdownDuringDownloadRemovesTempDir(t *testing.T) {
	t.Parallel()
	archive := datasetArchive(t, 256<<10)
	sent := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(archive)))
		_, _ = w.Write(archive[:len(archive)/2])
		w.(http.Flusher).Flush() //nolint:forcetypeassert // httptest writers flush.
		close(sent)
		<-r.Context().Done() // stall until the client gives up.
	}))
	defer srv.Close()
	dataDir := t.TempDir()

	base, opener, cancel, done, out := startRun(t, map[string]string{
		"HTTP_ADDR": "127.0.0.1:0", "DATA_DIR": dataDir, "DATASET_URL": srv.URL,
	})

	if code, body := post(t, newClient(), base+"/fetch"); code != http.StatusAccepted {
		t.Fatalf("POST /fetch = %d %s", code, body)
	}
	<-sent
	if matches, _ := filepath.Glob(filepath.Join(dataDir, ".fetch-tmp-*")); len(matches) != 1 {
		t.Fatalf("temp dirs during download = %v, want 1", matches)
	}

	cancel()
	if code := <-done; code != 0 {
		t.Errorf("exit code = %d, want 0:\n%s", code, out.String())
	}
	if matches, _ := filepath.Glob(filepath.Join(dataDir, ".fetch-tmp-*")); len(matches) != 0 {
		t.Errorf("temp dirs after shutdown = %v, want none", matches)
	}
	if !opener.st.closed.Load() {
		t.Error("the store was not closed: the fetch closer must not stop the other closers")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "maildir")); !os.IsNotExist(err) {
		t.Errorf("maildir after canceled fetch: %v, want it absent", err)
	}
	if !strings.Contains(out.String(), `"msg":"fetch failed"`) || !strings.Contains(out.String(), "fetch canceled") {
		t.Errorf("no canceled fetch log line in:\n%s", out.String())
	}
}
