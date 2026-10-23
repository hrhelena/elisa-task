package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	oas "github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/hrhelena/elisa-task/internal/fetch"
)

const specGolden = "testdata/openapi.golden.json"

// docsPatterns are the routes of the docs themselves, served but not
// documented in the spec.
var docsPatterns = []string{"GET /openapi.json", "GET /docs", "GET /docs/{$}", "GET /docs/{file}"}

// fullDeps sets every feature dependency (the Gherkin Background).
func fullDeps() Deps {
	return Deps{
		Fetcher:  &fakeFetcher{progress: fetch.Progress{State: fetch.StateReady}},
		Senders:  &fakeSenderStats{},
		Ingester: &fakeIngester{},
		Datasets: fakeDatasets{},
		Version:  "v1.2.3",
	}
}

// routerAPI is a testAPI that also exposes its router.
type routerAPI struct {
	*testAPI
	rt *router
}

// newRouterAPI builds the handler like New does, with a logger at level.
// setup runs on the router before the routes are registered.
func newRouterAPI(t *testing.T, deps Deps, level slog.Level, setup func(*router)) *routerAPI {
	t.Helper()
	logs := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: level}))
	if deps.Registry == nil {
		deps.Registry = prometheus.NewRegistry()
	}
	rt := newRouter(deps, logger)
	if setup != nil {
		setup(rt)
	}
	rt.routes(deps)
	return &routerAPI{testAPI: &testAPI{handler: rt.handler(), logs: logs, reg: deps.Registry}, rt: rt}
}

// getSpec serves GET /openapi.json and returns the body.
func getSpec(t *testing.T, api *routerAPI) []byte {
	t.Helper()
	rec := api.do(t, http.MethodGet, "/openapi.json", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /openapi.json: status = %d, body %s", rec.Code, rec.Body.String())
	}
	return rec.Body.Bytes()
}

// specDoc is the part of the OpenAPI document the tests inspect.
type specDoc struct {
	OpenAPI string `json:"openapi"`
	Info    struct {
		Title   string `json:"title"`
		Version string `json:"version"`
	} `json:"info"`
	Servers    []any                            `json:"servers"`
	Paths      map[string]map[string]specOp     `json:"paths"`
	Components struct{ Schemas map[string]any } `json:"components"`
}

type specOp struct {
	OperationID string `json:"operationId"`
	Parameters  []struct {
		Name     string         `json:"name"`
		In       string         `json:"in"`
		Required bool           `json:"required"`
		Schema   map[string]any `json:"schema"`
	} `json:"parameters"`
	RequestBody any                     `json:"requestBody"`
	Responses   map[string]specResponse `json:"responses"`
}

type specResponse struct {
	Description string                    `json:"description"`
	Headers     map[string]any            `json:"headers"`
	Content     map[string]map[string]any `json:"content"`
}

func parseSpec(t *testing.T, body []byte) specDoc {
	t.Helper()
	var doc specDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("spec is not JSON: %v", err)
	}
	return doc
}

// operation returns the operation "<METHOD> <path>" of doc.
func (d specDoc) operation(t *testing.T, pattern string) specOp {
	t.Helper()
	method, path, _ := strings.Cut(pattern, " ")
	op, ok := d.Paths[path][strings.ToLower(method)]
	if !ok {
		t.Fatalf("spec has no operation %s", pattern)
	}
	return op
}

// specOperations returns every "<METHOD> <path>" in doc.
func specOperations(doc specDoc) []string {
	var ops []string
	for path, item := range doc.Paths {
		for method := range item {
			ops = append(ops, strings.ToUpper(method)+" "+path)
		}
	}
	slices.Sort(ops)
	return ops
}

// routeDiff is the route-equality check: it compares the patterns
// registered on the mux, apart from the docs routes, with the operations of
// the spec.
func routeDiff(patterns map[string]bool, doc specDoc) (missingFromSpec, missingFromMux []string) {
	mux := map[string]bool{}
	for p := range patterns {
		if !slices.Contains(docsPatterns, p) {
			mux[p] = true
		}
	}
	spec := map[string]bool{}
	for _, op := range specOperations(doc) {
		spec[op] = true
	}
	for _, p := range slices.Sorted(maps.Keys(mux)) {
		if !spec[p] {
			missingFromSpec = append(missingFromSpec, p)
		}
	}
	for _, op := range slices.Sorted(maps.Keys(spec)) {
		if !mux[op] {
			missingFromMux = append(missingFromMux, op)
		}
	}
	return missingFromSpec, missingFromMux
}

func checkRoutesEqual(t *testing.T, api *routerAPI) specDoc {
	t.Helper()
	doc := parseSpec(t, getSpec(t, api))
	missingFromSpec, missingFromMux := routeDiff(api.rt.patterns, doc)
	if len(missingFromSpec) > 0 {
		t.Errorf("routes registered on the mux but missing from the spec: %v", missingFromSpec)
	}
	if len(missingFromMux) > 0 {
		t.Errorf("operations in the spec but not registered on the mux: %v", missingFromMux)
	}
	for _, p := range docsPatterns {
		if !api.rt.patterns[p] {
			t.Errorf("docs route %q is not registered", p)
		}
	}
	return doc
}

// Scenario: The spec is served as JSON.
func TestSpecServedAsJSON(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	rec := api.do(t, http.MethodGet, "/openapi.json", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	for header, want := range map[string]string{
		"Content-Type":           contentTypeJSON,
		"X-Content-Type-Options": "nosniff",
		"Cache-Control":          "no-cache",
		"Content-Length":         strconv.Itoa(rec.Body.Len()),
	} {
		if got := rec.Header().Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	doc := parseSpec(t, rec.Body.Bytes())
	if doc.OpenAPI != "3.1.0" {
		t.Errorf("openapi = %q, want 3.1.0", doc.OpenAPI)
	}
	if doc.Info.Title != "Enron email metrics service" || doc.Info.Version != "v1.2.3" {
		t.Errorf("info = %+v, want the service title and version v1.2.3", doc.Info)
	}
	if doc.Servers != nil {
		t.Errorf("servers = %v, want none (Swagger UI then targets the page's origin)", doc.Servers)
	}
	if !bytes.HasSuffix(rec.Body.Bytes(), []byte("}\n")) || !bytes.Contains(rec.Body.Bytes(), []byte("\n  \"info\"")) {
		t.Error("body is not indented JSON followed by a newline")
	}
}

func TestSpecVersionDefaultsToDev(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, Deps{}, slog.LevelDebug, nil)
	if v := parseSpec(t, getSpec(t, api)).Info.Version; v != "dev" {
		t.Errorf("info.version = %q, want dev", v)
	}
}

// Scenario: HEAD on the spec.
func TestSpecHead(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	// The recorder keeps what the handler writes; net/http drops the body of
	// a HEAD response on the wire, which the server test below checks.
	rec := api.do(t, http.MethodHead, "/openapi.json", nil)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != contentTypeJSON {
		t.Fatalf("HEAD /openapi.json: status %d, Content-Type %q", rec.Code, rec.Header().Get("Content-Type"))
	}

	base, client := httptestClient(t, api.handler)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodHead, base+"/openapi.json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body := new(bytes.Buffer)
	_, _ = body.ReadFrom(resp.Body)
	if resp.StatusCode != http.StatusOK || body.Len() != 0 || resp.Header.Get("Content-Type") != contentTypeJSON {
		t.Errorf("HEAD over HTTP: status %d, %d body bytes, Content-Type %q; want 200, empty, JSON",
			resp.StatusCode, body.Len(), resp.Header.Get("Content-Type"))
	}
}

// oasSchema compiles the official OpenAPI 3.1 schema (with schema
// validation, "schema-base"), vendored in testdata/oas31 so that the test
// runs offline. The compiler only loads the resources added here.
func oasSchema(t *testing.T) *oas.Schema {
	t.Helper()
	c := oas.NewCompiler()
	for file, id := range map[string]string{
		"schema-base-2022-10-07.json": "https://spec.openapis.org/oas/3.1/schema-base/2022-10-07",
		"schema-2022-10-07.json":      "https://spec.openapis.org/oas/3.1/schema/2022-10-07",
		"dialect-base.json":           "https://spec.openapis.org/oas/3.1/dialect/base",
		"meta-base.json":              "https://spec.openapis.org/oas/3.1/meta/base",
	} {
		f, err := os.Open(filepath.Join("testdata", "oas31", file)) //nolint:gosec // fixed test file names
		if err != nil {
			t.Fatal(err)
		}
		doc, err := oas.UnmarshalJSON(f)
		_ = f.Close()
		if err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		if err := c.AddResource(id, doc); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
	}
	schema, err := c.Compile("https://spec.openapis.org/oas/3.1/schema-base/2022-10-07")
	if err != nil {
		t.Fatalf("compiling the OAS 3.1 schema: %v", err)
	}
	return schema
}

// The validator is not vacuous: it rejects broken documents, including a
// broken schema inside a component.
func TestOASValidatorRejectsInvalidDocuments(t *testing.T) {
	t.Parallel()

	schema := oasSchema(t)
	for name, doc := range map[string]string{
		"no info":           `{"openapi":"3.1.0","paths":{}}`,
		"no description":    `{"openapi":"3.1.0","info":{"title":"t","version":"1"},"paths":{"/x":{"get":{"responses":{"200":{}}}}}}`,
		"bad schema type":   `{"openapi":"3.1.0","info":{"title":"t","version":"1"},"components":{"schemas":{"A":{"type":5}}}}`,
		"bad response code": `{"openapi":"3.1.0","info":{"title":"t","version":"1"},"paths":{"/x":{"get":{"responses":{"abc":{"description":"d"}}}}}}`,
	} {
		inst, err := oas.UnmarshalJSON(strings.NewReader(doc))
		if err != nil {
			t.Fatal(err)
		}
		if schema.Validate(inst) == nil {
			t.Errorf("%s: the invalid document was accepted", name)
		}
	}
}

// Scenario: The spec is valid OpenAPI 3.1.
func TestSpecIsValidOpenAPI31(t *testing.T) {
	t.Parallel()

	body := getSpec(t, newRouterAPI(t, fullDeps(), slog.LevelDebug, nil))
	inst, err := oas.UnmarshalJSON(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := oasSchema(t).Validate(inst); err != nil {
		t.Errorf("the document is not valid OpenAPI 3.1:\n%v", err)
	}

	// Every $ref resolves to a component schema.
	doc := parseSpec(t, body)
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	refs := 0
	walkJSON(raw, func(key string, v any) {
		if key != "$ref" {
			return
		}
		refs++
		ref, _ := v.(string)
		name, ok := strings.CutPrefix(ref, "#/components/schemas/")
		if !ok || doc.Components.Schemas[name] == nil {
			t.Errorf("$ref %q does not resolve to a component schema", ref)
		}
	})
	if refs == 0 {
		t.Error("no $ref found; the component schemas are not used")
	}

	// Every operationId is unique.
	seen := map[string]string{}
	for _, pattern := range specOperations(doc) {
		id := doc.operation(t, pattern).OperationID
		if id == "" {
			t.Errorf("%s has no operationId", pattern)
		}
		if other, dup := seen[id]; dup {
			t.Errorf("operationId %q is used by %s and %s", id, other, pattern)
		}
		seen[id] = pattern
	}
}

// walkJSON calls fn for every object member of v, recursively.
func walkJSON(v any, fn func(key string, v any)) {
	switch v := v.(type) {
	case map[string]any:
		for k, child := range v {
			fn(k, child)
			walkJSON(child, fn)
		}
	case []any:
		for _, child := range v {
			walkJSON(child, fn)
		}
	}
}

// Scenario: The spec matches the golden file.
// Run `go test ./internal/httpapi -run TestSpecGolden -update` after a
// deliberate change to the API.
func TestSpecGolden(t *testing.T) {
	t.Parallel()

	body := getSpec(t, newRouterAPI(t, fullDeps(), slog.LevelDebug, nil))
	if *updateGolden {
		if err := os.WriteFile(specGolden, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(specGolden)
	if err != nil {
		t.Fatalf("reading the golden file (run with -update to create it): %v", err)
	}
	if !bytes.Equal(body, want) {
		t.Errorf("GET /openapi.json differs from %s; run with -update if the change is deliberate", specGolden)
	}
	// Generation is deterministic: a second build gives the same bytes.
	if again := getSpec(t, newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)); !bytes.Equal(body, again) {
		t.Error("two builds of the spec differ")
	}
}

// Scenario: Spec operations equal the mux routes.
func TestSpecOperationsEqualMuxRoutes(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)
	doc := checkRoutesEqual(t, api)
	want := []string{
		"GET /healthz", "GET /metrics", "GET /readyz", "GET /status", "GET /top-senders", "POST /fetch", "POST /start",
	}
	if got := specOperations(doc); !slices.Equal(got, want) {
		t.Errorf("spec operations = %v, want %v", got, want)
	}
	for _, p := range docsPatterns {
		method, path, _ := strings.Cut(p, " ")
		if _, ok := doc.Paths[path][strings.ToLower(method)]; ok {
			t.Errorf("docs route %s is documented in the spec", p)
		}
	}
}

// Scenario Outline: Spec and mux stay equal when optional dependencies are nil.
func TestSpecAndMuxEqualWithNilDependency(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		unset  func(*Deps)
		absent []string
	}{
		{"Fetcher", func(d *Deps) { d.Fetcher = nil }, []string{"POST /fetch"}},
		{"Senders", func(d *Deps) { d.Senders = nil }, []string{"GET /top-senders"}},
		{"Ingester", func(d *Deps) { d.Ingester = nil }, []string{"POST /start", "GET /status"}},
		{"all", func(d *Deps) { *d = Deps{} }, []string{"POST /fetch", "GET /top-senders", "POST /start", "GET /status"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			deps := fullDeps()
			tc.unset(&deps)
			api := newRouterAPI(t, deps, slog.LevelDebug, nil)
			doc := checkRoutesEqual(t, api)
			for _, p := range tc.absent {
				if api.rt.patterns[p] {
					t.Errorf("%s is registered on the mux", p)
				}
				if slices.Contains(specOperations(doc), p) {
					t.Errorf("%s is in the spec", p)
				}
			}
		})
	}
}

// Scenario: A route added to the mux without a spec entry fails the test.
func TestRouteEqualityCheckReportsUndocumentedRoute(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	deps := fullDeps()
	rt := newRouter(deps, logger)
	table := append(rt.apiRoutes(deps), route{
		method: http.MethodGet, path: "/extra", documented: true, // but no op
		handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
	})
	spec, err := rt.buildSpec("dev", table)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range append(table, docsRoutes(spec)...) {
		rt.register(r)
	}

	missingFromSpec, missingFromMux := routeDiff(rt.patterns, parseSpec(t, spec))
	if !slices.Equal(missingFromSpec, []string{"GET /extra"}) || len(missingFromMux) != 0 {
		t.Errorf("routeDiff = missing from spec %v, missing from mux %v; want [GET /extra], []",
			missingFromSpec, missingFromMux)
	}
}

// Scenario: The top-senders limit parameter is described from the constants.
func TestSpecTopSendersLimitFromConstants(t *testing.T) {
	t.Parallel()

	doc := parseSpec(t, getSpec(t, newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)))
	op := doc.operation(t, "GET /top-senders")
	if len(op.Parameters) != 1 {
		t.Fatalf("parameters = %+v, want only limit", op.Parameters)
	}
	p := op.Parameters[0]
	if p.Name != "limit" || p.In != "query" || p.Required || p.Schema["type"] != "integer" {
		t.Errorf("limit parameter = %+v, want an optional integer query parameter", p)
	}
	for key, want := range map[string]float64{
		"minimum": 1, "maximum": MaxTopSendersLimit, "default": DefaultTopSendersLimit,
	} {
		if got := p.Schema[key]; got != want {
			t.Errorf("limit %s = %v, want %v", key, got, want)
		}
	}
	if op.RequestBody != nil {
		t.Errorf("GET /top-senders has a requestBody: %v", op.RequestBody)
	}
	assertStatuses(t, doc, "GET /top-senders", "200", "400", "503")
	assertErrorResponse(t, op.Responses["400"], CodeInvalidLimit)
}

func assertStatuses(t *testing.T, doc specDoc, pattern string, want ...string) {
	t.Helper()
	got := slices.Sorted(maps.Keys(doc.operation(t, pattern).Responses))
	if !slices.Equal(got, want) {
		t.Errorf("%s responses = %v, want %v", pattern, got, want)
	}
}

// assertErrorResponse checks that r is the Error envelope and that its
// description lists codes.
func assertErrorResponse(t *testing.T, r specResponse, codes ...string) {
	t.Helper()
	if ref := r.Content["application/json"]["schema"]; !equalJSON(ref, map[string]any{"$ref": "#/components/schemas/Error"}) {
		t.Errorf("error response schema = %v, want the Error component", ref)
	}
	for _, code := range codes {
		if !strings.Contains(r.Description, "`"+code+"`") {
			t.Errorf("error response description %q does not list %s", r.Description, code)
		}
	}
}

func equalJSON(a, b any) bool {
	ja, errA := json.Marshal(a)
	jb, errB := json.Marshal(b)
	return errA == nil && errB == nil && bytes.Equal(ja, jb)
}

// Scenario: Error responses are documented as they occur.
func TestSpecErrorResponses(t *testing.T) {
	t.Parallel()

	doc := parseSpec(t, getSpec(t, newRouterAPI(t, fullDeps(), slog.LevelDebug, nil)))

	assertStatuses(t, doc, "POST /start", "202", "409", "503")
	start := doc.operation(t, "POST /start")
	assertErrorResponse(t, start.Responses["409"], CodeIngestionRunning, CodeDatasetMissing)
	assertErrorResponse(t, start.Responses["503"], CodeUnavailable)

	assertStatuses(t, doc, "POST /fetch", "200", "202", "409", "500", "503")
	fetchOp := doc.operation(t, "POST /fetch")
	assertErrorResponse(t, fetchOp.Responses["409"], CodeMaildirNotDirectory, CodeMaildirUnreadable)
	assertErrorResponse(t, fetchOp.Responses["500"], CodeDataDirNotWritable, CodeInternal)
	assertErrorResponse(t, fetchOp.Responses["503"], CodeUnavailable)

	assertStatuses(t, doc, "GET /status", "200", "503")
	status := doc.operation(t, "GET /status")
	assertErrorResponse(t, status.Responses["503"], CodeUnavailable)
	if status.Responses["200"].Headers["Cache-Control"] == nil {
		t.Error("GET /status 200 does not declare Cache-Control")
	}

	for _, p := range []string{"POST /start", "POST /fetch"} {
		op := doc.operation(t, p)
		if op.Responses["202"].Headers["Location"] == nil {
			t.Errorf("%s 202 does not declare the Location header", p)
		}
		if op.RequestBody != nil {
			t.Errorf("%s documents a requestBody, but the body is ignored", p)
		}
	}

	assertStatuses(t, doc, "GET /healthz", "200")
	assertStatuses(t, doc, "GET /readyz", "200", "503")
	assertStatuses(t, doc, "GET /metrics", "200")
	if _, ok := doc.operation(t, "GET /metrics").Responses["200"].Content["text/plain"]; !ok {
		t.Error("GET /metrics 200 is not text/plain")
	}

	// The README examples.
	examples := map[string]string{
		"POST /start":      `{"run_id":4,"state":"running","started_at":"2026-10-23T10:00:00Z"}`,
		"GET /top-senders": `{"senders":[{"email":"kay.mann@enron.com","count":16735}]}`,
	}
	for p, want := range examples {
		code := "200"
		if p == "POST /start" {
			code = "202"
		}
		got := doc.operation(t, p).Responses[code].Content["application/json"]["example"]
		var wantValue any
		if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
			t.Fatal(err)
		}
		if !equalJSON(got, wantValue) {
			t.Errorf("%s %s example = %v, want %s", p, code, got, want)
		}
	}
}

// Scenario: Response schemas reflect the Go types.
func TestSpecSchemasReflectGoTypes(t *testing.T) {
	t.Parallel()

	body := getSpec(t, newRouterAPI(t, fullDeps(), slog.LevelDebug, nil))
	var doc struct {
		Components struct {
			Schemas map[string]struct {
				Properties map[string]map[string]any `json:"properties"`
				Required   []string                  `json:"required"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	schemas := doc.Components.Schemas

	wantNames := []string{
		"Dataset", "Error", "ErrorDetail", "HealthResponse", "ReadyResponse", "SenderCount", "StartResponse",
		"StatusResponse", "TopSendersResponse",
	}
	if got := slices.Sorted(maps.Keys(schemas)); !slices.Equal(got, wantNames) {
		t.Errorf("component schemas = %v, want %v", got, wantNames)
	}

	status := schemas["StatusResponse"]
	wantProps := []string{
		"state", "finished", "messages_processed", "total_messages", "messages_skipped", "files_seen",
		"skip_reasons", "field_errors", "run_id", "started_at", "finished_at", "updated_at", "error", "dataset",
	}
	if got := slices.Sorted(maps.Keys(status.Properties)); !slices.Equal(got, slices.Sorted(slices.Values(wantProps))) {
		t.Errorf("StatusResponse properties = %v, want %v", got, wantProps)
	}
	// Every field is always present except the optional dataset section.
	if want := wantProps[:len(wantProps)-1]; !slices.Equal(status.Required, want) {
		t.Errorf("StatusResponse required = %v, want %v", status.Required, want)
	}
	if !equalJSON(status.Properties["run_id"]["type"], []string{"null", "integer"}) {
		t.Errorf("run_id type = %v, want nullable integer", status.Properties["run_id"]["type"])
	}
	for _, name := range []string{"skip_reasons", "field_errors"} {
		p := status.Properties[name]
		if p["type"] != "object" || !equalJSON(p["additionalProperties"], map[string]any{"type": "integer", "format": "int64"}) {
			t.Errorf("%s = %v, want a non-null object with integer values", name, p)
		}
	}
	for _, name := range []string{"started_at", "finished_at", "updated_at"} {
		if f := status.Properties[name]["format"]; f != "date-time" {
			t.Errorf("StatusResponse %s format = %v, want date-time", name, f)
		}
	}
	if !equalJSON(status.Properties["state"]["enum"], []string{StateIdle, StateRunning, StateCompleted, StateFailed}) {
		t.Errorf("StatusResponse state enum = %v", status.Properties["state"]["enum"])
	}
	if !equalJSON(schemas["StartResponse"].Properties["state"]["enum"], []string{StateRunning}) {
		t.Errorf("StartResponse state enum = %v", schemas["StartResponse"].Properties["state"]["enum"])
	}

	if got := schemas["Dataset"].Properties["state"]["enum"]; !equalJSON(got, fetch.States) {
		t.Errorf("Dataset state enum = %v, want fetch.States %v", got, fetch.States)
	}
	if len(schemas["Dataset"].Required) != len(schemas["Dataset"].Properties) {
		t.Errorf("Dataset required = %v, want every property", schemas["Dataset"].Required)
	}

	sender := schemas["SenderCount"]
	if !slices.Equal(sender.Required, []string{"email", "count"}) ||
		sender.Properties["email"]["type"] != "string" || sender.Properties["count"]["type"] != "integer" {
		t.Errorf("SenderCount = %+v, want required string email and integer count", sender)
	}
	if senders := schemas["TopSendersResponse"].Properties["senders"]; senders["type"] != "array" {
		t.Errorf("TopSendersResponse senders = %v, want a non-null array", senders)
	}
	if !slices.Equal(schemas["ErrorDetail"].Required, []string{"code", "message"}) {
		t.Errorf("ErrorDetail required = %v", schemas["ErrorDetail"].Required)
	}
}

// The struct tags only describe the schema: the JSON on the wire is
// unchanged, including for the /healthz body that is now a named struct.
func TestSchemaTagsDoNotChangeTheWireFormat(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		v    any
		want string
	}{
		{healthResponse{Status: "ok"}, `{"status":"ok"}`},
		{readyResponse{Status: "ready"}, `{"status":"ready"}`},
		{errorBody{Error: errorDetail{Code: "c", Message: "m"}}, `{"error":{"code":"c","message":"m"}}`},
		{topSendersResponse{Senders: []SenderCount{{Email: "a@b", Count: 1}}}, `{"senders":[{"email":"a@b","count":1}]}`},
		{
			datasetJSON{State: fetch.StateAbsent, Path: "/d"},
			`{"state":"absent","path":"/d","bytes_downloaded":0,"bytes_total":0,"files_extracted":0,"error":null,"started_at":null,"finished_at":null}`,
		},
	} {
		got, err := json.Marshal(tc.v)
		if err != nil || string(got) != tc.want {
			t.Errorf("json.Marshal(%T) = %s, %v; want %s", tc.v, got, err, tc.want)
		}
	}
}

// Scenario: Spec generation failure is reported, not fatal.
func TestSpecGenerationFailure(t *testing.T) {
	t.Parallel()

	api := newRouterAPI(t, fullDeps(), slog.LevelInfo, func(rt *router) {
		rt.buildSpec = func(string, []route) ([]byte, error) { return nil, errors.New("boom") }
	})
	rec := api.do(t, http.MethodGet, "/openapi.json", nil)
	if rec.Code != http.StatusInternalServerError || errorCode(t, rec) != CodeInternal {
		t.Errorf("GET /openapi.json: status %d body %s, want 500 internal", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), specFailedMessage) {
		t.Errorf("body %s does not say the document could not be generated", rec.Body.String())
	}
	lines := api.logs.find(t, specFailedMessage)
	if len(lines) != 1 || lines[0]["level"] != "ERROR" || lines[0]["err"] != "boom" {
		t.Errorf("log lines = %v, want one ERROR with the cause", lines)
	}
	if rec := api.do(t, http.MethodGet, "/healthz", nil); rec.Code != http.StatusOK {
		t.Errorf("GET /healthz: status = %d, want 200", rec.Code)
	}
	if rec := api.do(t, http.MethodGet, "/docs/", nil); rec.Code != http.StatusOK {
		t.Errorf("GET /docs/: status = %d, want 200 (the UI still loads and shows the error)", rec.Code)
	}
	// Still logged once: the document is generated once, not per request.
	api.do(t, http.MethodGet, "/openapi.json", nil)
	if n := len(api.logs.find(t, specFailedMessage)); n != 1 {
		t.Errorf("%d failure log lines after two requests, want 1", n)
	}
}

// Scenario: Docs routes are read-only routes outside the mutating chain.
func TestDocsRoutesOutsideMutatingChain(t *testing.T) {
	t.Parallel()

	reject := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeError(w, http.StatusForbidden, "forbidden", "admin only")
		})
	}
	api := newRouterAPI(t, fullDeps(), slog.LevelDebug, func(rt *router) {
		rt.mutating = append(rt.mutating, reject)
	})
	for _, path := range []string{"/openapi.json", "/docs/", "/docs/init.js"} {
		if rec := api.do(t, http.MethodGet, path, nil); rec.Code != http.StatusOK {
			t.Errorf("GET %s: status = %d, want 200", path, rec.Code)
		}
	}
	for _, path := range []string{"/start", "/fetch"} {
		if rec := api.do(t, http.MethodPost, path, nil); rec.Code != http.StatusForbidden {
			t.Errorf("POST %s: status = %d, want 403 from the mutating middleware", path, rec.Code)
		}
	}
}
