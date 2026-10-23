package httpapi

import (
	"embed"
	"io/fs"
	"net/http"
	"strconv"
)

// swaggerUI holds the vendored Swagger UI (see swaggerui/VERSION and
// SHA256SUMS, updated by `make swagger-ui`) and our own index.html and
// init.js (ADR 0016).
//
//go:embed swaggerui
var swaggerUI embed.FS

const (
	swaggerUIDir = "swaggerui"

	// docsCSP enforces "nothing from a CDN" in the browser: the UI may only
	// load scripts, styles and data from this origin. Swagger UI sets inline
	// styles, and renders some icons as data: images.
	docsCSP = "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; " +
		"connect-src 'self'; frame-ancestors 'none'"

	cacheNoCache = "no-cache"
	cacheAsset   = "public, max-age=3600"
)

// docsAsset returns how an allowlisted file under /docs/ is served. Every
// other name, including index.html (served at /docs/ only) and the VERSION,
// SHA256SUMS and license files, is not served.
func docsAsset(name string) (contentType, cacheControl string, ok bool) {
	switch name {
	case "swagger-ui-bundle.js":
		return "text/javascript; charset=utf-8", cacheAsset, true
	case "init.js":
		// Ours: it changes with the binary, not with the Swagger UI version.
		return "text/javascript; charset=utf-8", cacheNoCache, true
	case "swagger-ui.css":
		return "text/css; charset=utf-8", cacheAsset, true
	case "favicon-32x32.png", "favicon-16x16.png":
		return "image/png", cacheAsset, true
	default:
		return "", "", false
	}
}

// docsRoutes are the routes of the OpenAPI document and the Swagger UI. They
// are read-only, quiet and not documented in the spec itself. spec is the
// generated document, or nil if generation failed.
func docsRoutes(spec []byte) []route {
	return []route{
		{method: http.MethodGet, path: "/openapi.json", handler: openAPIHandler(spec), quiet: true},
		{method: http.MethodGet, path: "/docs", handler: http.HandlerFunc(docsRedirect), quiet: true},
		{method: http.MethodGet, path: "/docs/{$}", handler: http.HandlerFunc(docsIndex), quiet: true},
		{method: http.MethodGet, path: "/docs/{file}", handler: http.HandlerFunc(docsFile), quiet: true},
	}
}

// docsRedirect sends /docs to /docs/, so that the index page's relative
// asset URLs (./init.js) and spec URL (../openapi.json) resolve. The
// Location is relative, so it also works behind a proxy that rewrites the
// host.
func docsRedirect(w http.ResponseWriter, _ *http.Request) {
	h := w.Header()
	h.Set("Location", "docs/")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusMovedPermanently)
}

func docsIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Security-Policy", docsCSP)
	serveEmbedded(w, "index.html", "text/html; charset=utf-8", cacheNoCache)
}

func docsFile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("file")
	contentType, cacheControl, ok := docsAsset(name)
	if !ok {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		writeError(w, http.StatusNotFound, CodeNotFound, "no such endpoint")
		return
	}
	serveEmbedded(w, name, contentType, cacheControl)
}

// serveEmbedded writes an embedded file. name comes from the allowlist, so
// it is always a plain file name inside swaggerUIDir.
func serveEmbedded(w http.ResponseWriter, name, contentType, cacheControl string) {
	body, err := fs.ReadFile(swaggerUI, swaggerUIDir+"/"+name)
	if err != nil {
		// Only possible if an allowlisted file was not embedded, which the
		// tests rule out.
		writeError(w, http.StatusInternalServerError, CodeInternal, "the docs asset is missing from the binary")
		return
	}
	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Content-Length", strconv.Itoa(len(body)))
	h.Set("Cache-Control", cacheControl)
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
