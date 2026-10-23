package httpapi

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	oas "github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/hrhelena/elisa-task/internal/fetch"
)

// TestHandlerResponsesMatchSpec checks the spec against what the handlers
// really send: for every documented (operation, status) pair, a request that
// produces it, whose body must validate against the documented schema. It
// catches struct tags that contradict the data (a minimum, an enum, a
// nullability) and statuses documented but never sent, or sent but not
// documented.
func TestHandlerResponsesMatchSpec(t *testing.T) {
	t.Parallel()

	specBody := getSpec(t, newRouterAPI(t, fullDeps(), slog.LevelDebug, nil))
	const specURL = "https://enron.test/openapi.json"
	specDocument, err := oas.UnmarshalJSON(bytes.NewReader(specBody))
	if err != nil {
		t.Fatal(err)
	}
	compiler := oas.NewCompiler()
	if err := compiler.AddResource(specURL, specDocument); err != nil {
		t.Fatal(err)
	}
	pointer := strings.NewReplacer("~", "~0", "/", "~1")

	failing := errors.New("connection refused")
	runID := int64(7)
	errText := "maildir vanished"
	t1 := t0.Add(90e9)
	readyDataset := fetch.Progress{
		State: fetch.StateReady, Path: "/data/maildir", BytesTotal: -1, StartedAt: &t0, FinishedAt: &t1,
	}
	downloading := fetch.Progress{
		State: fetch.StateDownloading, Path: "/data/maildir", BytesDownloaded: 1 << 20, BytesTotal: -1, StartedAt: &t0,
	}
	failedDataset := fetch.Progress{
		State: fetch.StateFailed, Path: "/data/maildir", BytesTotal: 443254787, Error: "HTTP 404", StartedAt: &t0, FinishedAt: &t1,
	}

	type tc struct {
		name         string
		deps         func(*Deps)
		method, path string
		status       int
	}
	withFetcher := func(p fetch.Progress, err error) func(*Deps) {
		return func(d *Deps) { d.Fetcher = &fakeFetcher{progress: p, err: err} }
	}
	withIngester := func(ing *fakeIngester, p fetch.Progress) func(*Deps) {
		return func(d *Deps) { d.Ingester, d.Datasets = ing, fakeDatasets{p: p} }
	}
	withSenders := func(s *fakeSenderStats) func(*Deps) { return func(d *Deps) { d.Senders = s } }
	cases := []tc{
		{"fetch ready", withFetcher(readyDataset, nil), http.MethodPost, "/fetch", 200},
		{"fetch started", withFetcher(downloading, nil), http.MethodPost, "/fetch", 202},
		{"fetch maildir not a directory", withFetcher(fetch.Progress{}, fetch.ErrMaildirNotDirectory), http.MethodPost, "/fetch", 409},
		{"fetch data dir not writable", withFetcher(fetch.Progress{}, fetch.ErrDataDirNotWritable), http.MethodPost, "/fetch", 500},
		{"fetch shutting down", withFetcher(fetch.Progress{}, fetch.ErrClosed), http.MethodPost, "/fetch", 503},

		{"start", withIngester(&fakeIngester{run: IngestionRun{ID: 4, State: StateRunning, StartedAt: t0}}, readyDataset), http.MethodPost, "/start", 202},
		{"start while running", withIngester(&fakeIngester{startErr: ErrIngestionRunning}, readyDataset), http.MethodPost, "/start", 409},
		{"start database down", withIngester(&fakeIngester{startErr: failing}, readyDataset), http.MethodPost, "/start", 503},

		{"status idle", withIngester(&fakeIngester{}, fetch.Progress{State: fetch.StateAbsent, Path: "/data/maildir", BytesTotal: -1}), http.MethodGet, "/status", 200},
		{"status failed", withIngester(&fakeIngester{status: IngestionStatus{
			State: StateFailed, RunID: &runID, StartedAt: &t0, FinishedAt: &t1, UpdatedAt: &t1, FilesSeen: 10,
			MessagesProcessed: 8, MessagesSkipped: 2, SkipReasons: map[string]int64{"empty": 2},
			FieldErrors: map[string]int64{"date": 1}, Error: &errText, TotalMessages: 8,
		}}, failedDataset), http.MethodGet, "/status", 200},
		{"status database down", withIngester(&fakeIngester{statErr: failing}, readyDataset), http.MethodGet, "/status", 503},

		{"top senders", withSenders(&fakeSenderStats{senders: []SenderCount{{Email: "kay.mann@enron.com", Count: 16735}}}), http.MethodGet, "/top-senders?limit=1", 200},
		{"top senders empty", withSenders(&fakeSenderStats{}), http.MethodGet, "/top-senders", 200},
		{"top senders bad limit", withSenders(&fakeSenderStats{}), http.MethodGet, "/top-senders?limit=0", 400},
		{"top senders database down", withSenders(&fakeSenderStats{err: failing}), http.MethodGet, "/top-senders", 503},

		{"healthz", nil, http.MethodGet, "/healthz", 200},
		{"readyz", nil, http.MethodGet, "/readyz", 200},
		{"readyz failing check", func(d *Deps) { d.Readiness = NewReadiness(fakeChecker{name: "postgres", err: failing}) }, http.MethodGet, "/readyz", 503},
		{"metrics", nil, http.MethodGet, "/metrics", 200},
	}

	doc := parseSpec(t, specBody)
	covered := map[string]bool{}
	for _, c := range cases {
		deps := Deps{}
		if c.deps != nil {
			c.deps(&deps)
		}
		api := newRouterAPI(t, deps, slog.LevelError, nil)
		rec := api.do(t, c.method, c.path, nil)
		if rec.Code != c.status {
			t.Errorf("%s: %s %s = %d, want %d: %s", c.name, c.method, c.path, rec.Code, c.status, rec.Body.String())
			continue
		}
		path, _, _ := strings.Cut(c.path, "?")
		pair := fmt.Sprintf("%s %s %d", c.method, path, c.status)
		covered[pair] = true
		resp, ok := doc.Paths[path][strings.ToLower(c.method)].Responses[fmt.Sprint(c.status)]
		if !ok {
			t.Errorf("%s: %s is sent but not documented", c.name, pair)
			continue
		}
		for name := range resp.Headers {
			if rec.Header().Get(name) == "" {
				t.Errorf("%s: documented header %s is not sent", c.name, name)
			}
		}
		if path == "/metrics" {
			if _, ok := resp.Content["text/plain"]; !ok || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
				t.Errorf("%s: Content-Type %q, documented %v", c.name, rec.Header().Get("Content-Type"), resp.Content)
			}
			continue
		}
		schema, err := compiler.Compile(specURL + "#/paths/" + pointer.Replace(path) + "/" + strings.ToLower(c.method) +
			"/responses/" + fmt.Sprint(c.status) + "/content/application~1json/schema")
		if err != nil {
			t.Fatalf("%s: compiling the documented schema: %v", c.name, err)
		}
		body, err := oas.UnmarshalJSON(bytes.NewReader(rec.Body.Bytes()))
		if err != nil {
			t.Fatalf("%s: body is not JSON: %v", c.name, err)
		}
		if err := schema.Validate(body); err != nil {
			t.Errorf("%s: %s body %s does not match the spec:\n%v", c.name, pair, rec.Body.String(), err)
		}
	}

	var documented []string
	for _, op := range specOperations(doc) {
		method, path, _ := strings.Cut(op, " ")
		for code := range doc.operation(t, op).Responses {
			documented = append(documented, fmt.Sprintf("%s %s %s", method, path, code))
		}
	}
	for _, pair := range documented {
		if !covered[pair] {
			t.Errorf("documented response %s is not exercised by this test", pair)
		}
	}
}
