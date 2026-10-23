package httpapi

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// httptestClient is a real HTTP server for h, with its client. The server
// (and the client's idle connections) are closed when the test ends.
func httptestClient(t *testing.T, h http.Handler) (baseURL string, client *http.Client) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL, srv.Client()
}

func embedded(t *testing.T, name string) []byte {
	t.Helper()
	b, err := fs.ReadFile(swaggerUI, swaggerUIDir+"/"+name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Scenario: /docs redirects to the UI directory.
func TestDocsRedirect(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	rec := api.do(t, http.MethodGet, "/docs", nil)
	if rec.Code != http.StatusMovedPermanently {
		t.Fatalf("status = %d, want 301", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "docs/" {
		t.Errorf("Location = %q, want docs/ (relative)", loc)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Error("no X-Content-Type-Options: nosniff")
	}
}

// Scenario: The UI index is served from the embedded files.
func TestDocsIndex(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	rec := api.do(t, http.MethodGet, "/docs/", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	for header, want := range map[string]string{
		"Content-Type":            "text/html; charset=utf-8",
		"Content-Security-Policy": docsCSP,
		"X-Content-Type-Options":  "nosniff",
		"Cache-Control":           "no-cache",
	} {
		if got := rec.Header().Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	if !strings.Contains(rec.Header().Get("Content-Security-Policy"), "default-src 'self'") {
		t.Error("the CSP does not restrict default-src to 'self'")
	}
	body := rec.Body.String()
	if body != string(embedded(t, "index.html")) {
		t.Error("body differs from the embedded index.html")
	}
	for _, ref := range []string{`"./swagger-ui-bundle.js"`, `"./swagger-ui.css"`, `"./init.js"`} {
		if !strings.Contains(body, ref) {
			t.Errorf("index does not reference %s", ref)
		}
	}
	if strings.Contains(body, "http://") || strings.Contains(body, "https://") {
		t.Error("index contains an absolute URL")
	}
	// Every <script> has a src and no content, so the CSP needs no
	// 'unsafe-inline' for scripts.
	scripts := regexp.MustCompile(`(?s)<script([^>]*)>(.*?)</script>`).FindAllStringSubmatch(body, -1)
	if len(scripts) != 2 {
		t.Errorf("found %d scripts, want 2", len(scripts))
	}
	for _, s := range scripts {
		if !strings.Contains(s[1], "src=") || strings.TrimSpace(s[2]) != "" {
			t.Errorf("inline script: %q", s[0])
		}
	}
}

// Scenario: The UI points at the spec with a same-origin relative URL and
// Try it out on.
func TestDocsInitScript(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	rec := api.do(t, http.MethodGet, "/docs/init.js", nil)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "text/javascript; charset=utf-8" {
		t.Fatalf("status %d, Content-Type %q; want 200 JavaScript", rec.Code, rec.Header().Get("Content-Type"))
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control = %q, want no-cache (init.js is ours)", cc)
	}
	for _, want := range []string{`url: "../openapi.json"`, "tryItOutEnabled: true", "validatorUrl: null"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("init.js does not contain %q", want)
		}
	}
}

// Scenario Outline: Embedded assets are served with the right content type.
func TestDocsAssets(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	for _, tc := range []struct{ file, contentType string }{
		{"swagger-ui-bundle.js", "text/javascript; charset=utf-8"},
		{"swagger-ui.css", "text/css; charset=utf-8"},
		{"favicon-32x32.png", "image/png"},
		{"favicon-16x16.png", "image/png"},
	} {
		rec := api.do(t, http.MethodGet, "/docs/"+tc.file, nil)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status = %d, want 200", tc.file, rec.Code)
			continue
		}
		for header, want := range map[string]string{
			"Content-Type":           tc.contentType,
			"Cache-Control":          "public, max-age=3600",
			"X-Content-Type-Options": "nosniff",
		} {
			if got := rec.Header().Get(header); got != want {
				t.Errorf("%s: %s = %q, want %q", tc.file, header, got, want)
			}
		}
		if !bytes.Equal(rec.Body.Bytes(), embedded(t, tc.file)) {
			t.Errorf("%s: body differs from the embedded file", tc.file)
		}
	}
}

// Every allowlisted asset is embedded (serveEmbedded's 500 cannot happen).
func TestDocsAllowlistIsEmbedded(t *testing.T) {
	t.Parallel()

	entries, err := fs.ReadDir(swaggerUI, swaggerUIDir)
	if err != nil {
		t.Fatal(err)
	}
	served := 0
	for _, e := range entries {
		if _, _, ok := docsAsset(e.Name()); ok {
			served++
		}
	}
	if served != 5 {
		t.Errorf("%d embedded files are served under /docs/, want 5 (bundle, css, init.js, two favicons)", served)
	}
	embedded(t, "index.html")
}

// Scenario Outline: Unknown or non-public names under /docs are 404 JSON.
func TestDocsUnknownNames(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	for _, name := range []string{
		"nope.js", "VERSION", "SHA256SUMS", "index.html", "swagger-ui-bundle.js.map", "LICENSE", "NOTICE",
		"%2e%2e%2fgo.mod", "..%2Fhttpapi.go", "%2e%2e%2f..%2fgo.mod", "swaggerui%2finit.js",
	} {
		rec := api.do(t, http.MethodGet, "/docs/"+name, nil)
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET /docs/%s: status = %d, want 404", name, rec.Code)
			continue
		}
		if got, want := rec.Body.String(), `{"error":{"code":"not_found","message":"no such endpoint"}}`+"\n"; got != want {
			t.Errorf("GET /docs/%s: body = %q, want %q", name, got, want)
		}
		if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("GET /docs/%s: no nosniff header", name)
		}
	}
}

// Scenario: Path traversal never escapes the embedded directory.
func TestDocsPathTraversal(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	base, _ := httptestClient(t, api.handler)
	addr := strings.TrimPrefix(base, "http://")

	for _, target := range []string{"/docs/../go.mod", "/docs/../../go.mod", "/docs/./../httpapi.go", "/docs/%2e%2e/go.mod"} {
		// A raw request, so that no client cleans the path.
		var d net.Dialer
		conn, err := d.DialContext(t.Context(), "tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		_, err = io.WriteString(conn, "GET "+target+" HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		if err != nil {
			t.Fatal(err)
		}
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
		resp, err := http.ReadResponse(bufio.NewReader(conn), req)
		if err != nil {
			t.Fatalf("%s: %v", target, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		_ = conn.Close()
		if resp.StatusCode == http.StatusOK {
			t.Errorf("GET %s: status 200, want anything else", target)
		}
		if bytes.Contains(body, []byte("module github.com/hrhelena/elisa-task")) || bytes.Contains(body, []byte("package httpapi")) {
			t.Errorf("GET %s: a source file leaked: %q", target, body)
		}
	}
}

// Scenario Outline: Write methods on docs routes are rejected.
func TestDocsWriteMethods(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/openapi.json"},
		{http.MethodDelete, "/docs/"},
		{http.MethodPut, "/docs/init.js"},
		{http.MethodPost, "/docs"},
	} {
		rec := api.do(t, tc.method, tc.path, nil)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: status = %d, want 405", tc.method, tc.path, rec.Code)
			continue
		}
		if allow := rec.Header().Get("Allow"); !strings.Contains(allow, "GET") {
			t.Errorf("%s %s: Allow = %q, want GET", tc.method, tc.path, allow)
		}
		if code := errorCode(t, rec); code != CodeMethodNotAllowed {
			t.Errorf("%s %s: code = %q, want %s", tc.method, tc.path, code, CodeMethodNotAllowed)
		}
	}
}

// Scenario: Docs requests are logged at DEBUG.
func TestDocsRequestsLoggedAtDebug(t *testing.T) {
	t.Parallel()

	requests := []string{"/openapi.json", "/docs/", "/docs/swagger-ui.css"}

	info := newRouterAPI(t, fullDeps(), slog.LevelInfo, nil)
	for _, path := range requests {
		info.do(t, http.MethodGet, path, nil)
	}
	// Rejected methods on the docs paths match no route (405) and stay quiet too.
	info.do(t, http.MethodPost, "/openapi.json", nil)
	info.do(t, http.MethodDelete, "/docs/", nil)
	info.do(t, http.MethodPut, "/docs/init.js", nil)
	if lines := info.logs.find(t, "http request"); len(lines) != 0 {
		t.Errorf("at INFO, docs requests wrote access log lines: %v", lines)
	}

	debug := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	for _, path := range requests {
		debug.do(t, http.MethodGet, path, nil)
	}
	lines := debug.logs.find(t, "http request")
	wantRoutes := []string{"GET /openapi.json", "GET /docs/{$}", "GET /docs/{file}"}
	if len(lines) != len(wantRoutes) {
		t.Fatalf("at DEBUG, got %d access log lines, want %d", len(lines), len(wantRoutes))
	}
	for i, want := range wantRoutes {
		if lines[i]["route"] != want || lines[i]["level"] != "DEBUG" {
			t.Errorf("line %d: route %v level %v, want %s at DEBUG", i, lines[i]["route"], lines[i]["level"], want)
		}
	}

	// The API routes themselves still log at INFO.
	info.do(t, http.MethodGet, "/status", nil)
	if lines := info.logs.find(t, "http request"); len(lines) != 1 || lines[0]["route"] != "GET /status" {
		t.Errorf("GET /status access log = %v, want one INFO line", lines)
	}
}

// Scenario: Docs requests are labeled by pattern in metrics.
func TestDocsMetricsLabels(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	api.do(t, http.MethodGet, "/docs/nope-1.js", nil)
	api.do(t, http.MethodGet, "/docs/nope-2.js", nil)
	api.do(t, http.MethodGet, "/openapi.json", nil)

	metrics := scrape(t, api.testAPI)
	for _, want := range []string{
		`enron_http_requests_total{code="404",method="GET",route="GET /docs/{file}"} 2`,
		`enron_http_requests_total{code="200",method="GET",route="GET /openapi.json"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("metrics do not contain %q", want)
		}
	}
	if strings.Contains(metrics, "nope-1.js") {
		t.Error("a raw asset name leaked into a metric label")
	}
}

// Scenario: No CDN is referenced anywhere in the served UI.
func TestDocsReferenceNoCDN(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	for _, path := range []string{"/docs/", "/docs/init.js"} {
		body := api.do(t, http.MethodGet, path, nil).Body.String()
		for _, host := range []string{"unpkg.com", "cdn.jsdelivr.net", "cdnjs", "validator.swagger.io"} {
			if strings.Contains(body, host) {
				t.Errorf("%s references %s", path, host)
			}
		}
	}
}

// Scenario: Vendored assets match their checksums and pinned version.
func TestVendoredSwaggerUIChecksumsAndVersion(t *testing.T) {
	t.Parallel()

	sums := string(embedded(t, "SHA256SUMS"))
	listed := map[string]bool{}
	for line := range strings.Lines(sums) {
		sum, name, ok := strings.Cut(strings.TrimRight(line, "\n"), "  ")
		if !ok {
			t.Fatalf("malformed SHA256SUMS line %q", line)
		}
		listed[name] = true
		got := sha256.Sum256(embedded(t, name))
		if hex.EncodeToString(got[:]) != sum {
			t.Errorf("%s: SHA-256 does not match SHA256SUMS", name)
		}
	}
	for _, name := range []string{
		"swagger-ui-bundle.js", "swagger-ui.css", "favicon-32x32.png", "favicon-16x16.png",
		"LICENSE", "NOTICE", "swagger-ui-bundle.js.LICENSE.txt",
	} {
		if !listed[name] {
			t.Errorf("SHA256SUMS does not cover %s", name)
		}
	}

	makefile, err := os.ReadFile("../../Makefile")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^SWAGGER_UI_VERSION \?= (\S+)$`).FindSubmatch(makefile)
	if m == nil {
		t.Fatal("no SWAGGER_UI_VERSION ?= line in the Makefile")
	}
	if got := strings.TrimSpace(string(embedded(t, "VERSION"))); got != string(m[1]) {
		t.Errorf("swaggerui/VERSION = %s, Makefile SWAGGER_UI_VERSION = %s; run make swagger-ui", got, m[1])
	}
}
