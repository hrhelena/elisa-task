package httpapi

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The tests of scripts/update-swagger-ui.sh run it against a fake npm
// registry, so they need no network.

const updateScript = "../../scripts/update-swagger-ui.sh"

// vendoredFiles are the files the script copies from the package.
var vendoredFiles = []string{
	"swagger-ui-bundle.js", "swagger-ui.css", "favicon-32x32.png", "favicon-16x16.png",
	"LICENSE", "NOTICE", "swagger-ui-bundle.js.LICENSE.txt",
}

// fakeRegistry serves one swagger-ui-dist version. integrity overrides the
// tarball's real sha512 when set.
type fakeRegistry struct {
	version   string
	tarball   []byte
	integrity string
}

func (f fakeRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/swagger-ui-dist/" + f.version:
		integrity := f.integrity
		if integrity == "" {
			sum := sha512.Sum512(f.tarball)
			integrity = "sha512-" + base64.StdEncoding.EncodeToString(sum[:])
		}
		_, _ = fmt.Fprintf(w, `{"name":"swagger-ui-dist","version":%q,"dist":{"integrity":%q,"shasum":"x"}}`,
			f.version, integrity)
	case "/swagger-ui-dist/-/swagger-ui-dist-" + f.version + ".tgz":
		_, _ = w.Write(f.tarball)
	default:
		http.NotFound(w, r)
	}
}

// npmTarball builds a package tarball with the given files under package/.
func npmTarball(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, name := range slices.Sorted(maps.Keys(files)) {
		body := files[name]
		if err := tw.WriteHeader(&tar.Header{Name: "package/" + name, Mode: 0o644, Size: int64(len(body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func fullPackage() map[string]string {
	files := map[string]string{
		"swagger-ui-bundle.js.map": "not vendored", "swagger-ui-standalone-preset.js": "not vendored",
		"index.html": "upstream index, not ours", "package.json": "{}",
	}
	for _, f := range vendoredFiles {
		files[f] = "content of " + f
	}
	return files
}

// snapshot returns the SHA-256 of every file in dir.
func snapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(dir, e.Name())) //nolint:gosec // test directory
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		out[e.Name()] = hex.EncodeToString(sum[:])
	}
	return out
}

// runUpdateScript runs the script with the registry at registryURL. dest ""
// means the script's default, the real swaggerui directory.
func runUpdateScript(t *testing.T, registryURL, dest, version string) (string, error) {
	t.Helper()
	for _, tool := range []string{"bash", "curl", "tar", "openssl"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed: %v", tool, err)
		}
	}
	cmd := exec.CommandContext(t.Context(), "bash", updateScript, version) //nolint:gosec // fixed script, test input
	cmd.Env = append(os.Environ(), "SWAGGER_UI_REGISTRY="+registryURL)
	if dest != "" {
		cmd.Env = append(cmd.Env, "SWAGGER_UI_DIR="+dest)
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// newDest is a destination directory with our own files already in place.
func newDest(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range map[string]string{"index.html": "ours", "init.js": "ours", "VERSION": "1.0.0\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// Scenario: Update script rejects a bad download.
func TestUpdateScriptRejectsBadDownloads(t *testing.T) {
	t.Parallel()

	good := npmTarball(t, fullPackage())
	missing := fullPackage()
	delete(missing, "NOTICE")

	for _, tc := range []struct {
		name     string
		registry fakeRegistry
		version  string
		realDir  bool   // run against the real swaggerui directory
		wantOut  string // in the error message
	}{
		{"unknown version", fakeRegistry{version: "5.0.0", tarball: good}, "0.0.0-does-not-exist", true, "unknown version"},
		{"malformed version", fakeRegistry{version: "5.0.0", tarball: good}, "../5.0.0", true, "usage:"},
		{"integrity mismatch", fakeRegistry{version: "5.0.0", tarball: good, integrity: "sha512-AAAA"}, "5.0.0", false, "integrity mismatch"},
		{"file missing from the package", fakeRegistry{version: "5.0.0", tarball: npmTarball(t, missing)}, "5.0.0", false, "lacks an expected file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(tc.registry)
			t.Cleanup(srv.Close)
			dest := swaggerUIDir
			if !tc.realDir {
				dest = newDest(t)
			}
			before := snapshot(t, dest)
			destEnv := dest
			if tc.realDir {
				destEnv = "" // the script's default
			}
			out, err := runUpdateScript(t, srv.URL, destEnv, tc.version)
			if err == nil {
				t.Fatalf("the script succeeded, want a non-zero exit:\n%s", out)
			}
			if !strings.Contains(out, tc.wantOut) {
				t.Errorf("output %q does not contain %q", out, tc.wantOut)
			}
			if after := snapshot(t, dest); !maps.Equal(before, after) {
				t.Errorf("files under %s changed:\n%s", dest, out)
			}
		})
	}
}

func TestUpdateScriptVendorsOnlyAllowlistedFiles(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(fakeRegistry{version: "5.99.1", tarball: npmTarball(t, fullPackage())})
	t.Cleanup(srv.Close)
	dest := newDest(t)
	if out, err := runUpdateScript(t, srv.URL, dest, "5.99.1"); err != nil {
		t.Fatalf("the script failed: %v\n%s", err, out)
	}

	entries, err := os.ReadDir(dest)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, e.Name())
	}
	want := slices.Sorted(slices.Values(append([]string{"index.html", "init.js", "VERSION", "SHA256SUMS"}, vendoredFiles...)))
	if !slices.Equal(got, want) {
		t.Errorf("files = %v, want %v", got, want)
	}
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join(dest, name)) //nolint:gosec // test directory
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	if read("index.html") != "ours" || read("init.js") != "ours" {
		t.Error("the script overwrote our index.html or init.js")
	}
	if v := read("VERSION"); v != "5.99.1\n" {
		t.Errorf("VERSION = %q, want 5.99.1", v)
	}
	sums := read("SHA256SUMS")
	for _, f := range vendoredFiles {
		if read(f) != "content of "+f {
			t.Errorf("%s was not copied from the package", f)
		}
		sum := sha256.Sum256([]byte("content of " + f))
		if line := hex.EncodeToString(sum[:]) + "  " + f + "\n"; !strings.Contains(sums, line) {
			t.Errorf("SHA256SUMS lacks %q", line)
		}
	}
}
