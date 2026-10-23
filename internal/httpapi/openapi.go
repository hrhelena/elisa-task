package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/swaggest/jsonschema-go"
	openapi "github.com/swaggest/openapi-go"
	"github.com/swaggest/openapi-go/openapi31"
)

// The OpenAPI document (ADR 0016) is generated once, in New, from the route
// table: operations come from route.op, schemas are reflected from the Go
// types the handlers encode. It is served at GET /openapi.json.

const (
	specTitle = "Enron email metrics service"

	// specFailedMessage is logged once by New, and returned by GET
	// /openapi.json, when the document could not be generated.
	specFailedMessage = "the OpenAPI document could not be generated"

	specDescription = "Ingests the Enron maildir dataset into Postgres and serves metrics about it.\n\n" +
		"- There is no authentication (ADR 0005). \"Try it out\" sends real requests: `POST /fetch` and " +
		"`POST /start` really start work.\n" +
		"- Every error uses the envelope `{\"error\":{\"code\":\"<snake_case>\",\"message\":\"<text>\"}}` " +
		"(schema `Error`). Each response description lists the codes it can carry.\n" +
		"- An unknown path gives `404` with code `not_found`; a known path with the wrong method gives `405` " +
		"with code `method_not_allowed` and an `Allow` header.\n" +
		"- Every response carries an `X-Request-ID` header (a well-formed incoming one is reused)."
)

// Operation tags, in the order the UI lists them.
const (
	tagDataset    = "dataset"
	tagIngestion  = "ingestion"
	tagMetrics    = "metrics"
	tagOperations = "operations"
)

// operation documents one route in the OpenAPI document.
type operation struct {
	id, summary, description, tag string
	// request is a value whose type describes the parameters (query:"..."
	// tags); nil when the operation has none. Request bodies are ignored by
	// every handler, so none is documented.
	request   any
	responses []response
}

// response documents one status code of an operation.
type response struct {
	status int
	// body is a value whose type is the JSON body (reflected into a schema);
	// with contentType set, it is the body of that media type instead.
	body        any
	contentType string
	description string
	// headers maps a response header name to its description.
	headers map[string]string
	// example is a value of the body type shown as the media type example.
	example any
}

// errorResponse documents an error status whose body is the Error envelope
// and whose code is one of codes.
func errorResponse(status int, codes ...string) response {
	quoted := make([]string, len(codes))
	for i, c := range codes {
		quoted[i] = "`" + c + "`"
	}
	return response{
		status: status,
		body:   errorBody{},
		description: fmt.Sprintf("%s. Error %s: %s.", http.StatusText(status), plural(len(codes), "code"),
			strings.Join(quoted, ", ")),
	}
}

func plural(n int, word string) string {
	if n == 1 {
		return word
	}
	return word + "s"
}

func unavailableResponse(what string) response {
	r := errorResponse(http.StatusServiceUnavailable, CodeUnavailable)
	r.description += " " + what
	return r
}

func locationHeader() map[string]string {
	return map[string]string{"Location": "Always `/status`, where progress is reported."}
}

func fetchOperation() *operation {
	return &operation{
		id: "fetchDataset", tag: tagDataset,
		summary: "Download and extract the dataset",
		description: "Starts downloading and extracting the Enron maildir archive into the data directory, " +
			"without waiting for it. Idempotent: 200 when the dataset is already there, 202 when a fetch was " +
			"started or is already running. Progress is in the `dataset` section of `GET /status`. " +
			"Any request body is ignored.",
		responses: []response{
			{status: http.StatusOK, body: datasetJSON{}, description: "The dataset is ready (`state` is `ready`)."},
			{
				status: http.StatusAccepted, body: datasetJSON{}, headers: locationHeader(),
				description: "A fetch was started, or one is already running.",
			},
			errorResponse(http.StatusConflict, CodeMaildirNotDirectory, CodeMaildirUnreadable),
			errorResponse(http.StatusInternalServerError, CodeDataDirNotWritable, CodeInternal),
			unavailableResponse("The service is shutting down, or the maildir could not be checked " +
				"because of a temporary system error; retry later."),
		},
	}
}

func startOperation() *operation {
	return &operation{
		id: "startIngestion", tag: tagIngestion,
		summary: "Start an ingestion run",
		description: "Starts ingesting the maildir into the database in the background, unless a run is in " +
			"progress on any replica. A new run replaces the data of the previous one. " +
			"Any request body is ignored.",
		responses: []response{
			{
				status: http.StatusAccepted, body: startResponse{}, headers: locationHeader(),
				description: "The run was started.",
				example: startResponse{
					RunID: 4, State: StateRunning, StartedAt: time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC),
				},
			},
			errorResponse(http.StatusConflict, CodeIngestionRunning, CodeDatasetMissing),
			unavailableResponse("The database is unavailable, the service is shutting down, or the dataset " +
				"could not be checked because of a temporary system error; retry later."),
		},
	}
}

func statusOperation() *operation {
	return &operation{
		id: "getStatus", tag: tagIngestion,
		summary: "Ingestion (and dataset) progress",
		description: "Reports the current or last ingestion run, the number of stored messages and, when the " +
			"service can fetch the dataset, the dataset state.",
		responses: []response{
			{
				status: http.StatusOK, body: statusResponse{},
				headers:     map[string]string{"Cache-Control": "Always `no-store`."},
				description: "The current or last run (`idle` when none has ever started).",
			},
			unavailableResponse("The database is unavailable; retry later."),
		},
	}
}

func topSendersOperation() *operation {
	return &operation{
		id: "getTopSenders", tag: tagMetrics,
		summary: "Top senders by message count",
		description: "Ranks sender addresses by the number of stored messages, count descending, then email " +
			"ascending. Messages without a valid sender are not counted.",
		request: topSendersRequest{},
		responses: []response{
			{
				status: http.StatusOK, body: topSendersResponse{}, description: "The ranking.",
				example: topSendersResponse{Senders: []SenderCount{{Email: "kay.mann@enron.com", Count: 16735}}},
			},
			errorResponse(http.StatusBadRequest, CodeInvalidLimit),
			unavailableResponse("The database is unavailable; retry later."),
		},
	}
}

func healthzOperation() *operation {
	return &operation{
		id: "getHealthz", tag: tagOperations,
		summary:     "Liveness probe",
		description: "Succeeds whenever the process serves HTTP; it checks no dependency.",
		responses: []response{
			{status: http.StatusOK, body: healthResponse{}, description: "The process is alive."},
		},
	}
}

func readyzOperation() *operation {
	return &operation{
		id: "getReadyz", tag: tagOperations,
		summary:     "Readiness probe",
		description: "Checks the database; fails once shutdown has begun.",
		responses: []response{
			{status: http.StatusOK, body: readyResponse{}, description: "Ready (`status` is `ready`)."},
			{
				status: http.StatusServiceUnavailable, body: readyResponse{},
				description: "Not ready: `status` is `not_ready` (with the failed `checks`) or `shutting_down`.",
			},
		},
	}
}

func metricsOperation() *operation {
	return &operation{
		id: "getMetrics", tag: tagOperations,
		summary:     "Prometheus metrics",
		description: "HTTP, ingestion and fetch metrics, plus the Go runtime and process collectors.",
		responses: []response{
			{
				status: http.StatusOK, body: "", contentType: "text/plain",
				description: "The Prometheus text exposition format.",
			},
		},
	}
}

// schemaNames are the component schema names of the reflected types; the
// names are part of the document's contract (and of the golden file).
func schemaNames() map[reflect.Type]string {
	return map[reflect.Type]string{
		reflect.TypeFor[errorBody]():          "Error",
		reflect.TypeFor[errorDetail]():        "ErrorDetail",
		reflect.TypeFor[datasetJSON]():        "Dataset",
		reflect.TypeFor[startResponse]():      "StartResponse",
		reflect.TypeFor[statusResponse]():     "StatusResponse",
		reflect.TypeFor[topSendersResponse](): "TopSendersResponse",
		reflect.TypeFor[SenderCount]():        "SenderCount",
		reflect.TypeFor[healthResponse]():     "HealthResponse",
		reflect.TypeFor[readyResponse]():      "ReadyResponse",
	}
}

// buildSpec generates the OpenAPI 3.1 document of the documented routes, as
// indented JSON followed by a newline. Routes that are not documented, or
// have no operation, are left out (the route-equality test catches the
// latter).
func buildSpec(version string, routes []route) ([]byte, error) {
	r := openapi31.NewReflector()
	names := schemaNames()
	r.DefaultOptions = append(r.DefaultOptions, jsonschema.InterceptDefName(func(t reflect.Type, def string) string {
		if name, ok := names[t]; ok {
			return name
		}
		return def
	}))

	r.Spec.Info.WithTitle(specTitle).WithVersion(version).WithDescription(specDescription)
	r.Spec.WithTags(
		openapi31.Tag{Name: tagDataset, Description: strPtr("Download the Enron dataset.")},
		openapi31.Tag{Name: tagIngestion, Description: strPtr("Ingest the dataset into the database.")},
		openapi31.Tag{Name: tagMetrics, Description: strPtr("Metrics computed from the ingested messages.")},
		openapi31.Tag{Name: tagOperations, Description: strPtr("Probes and Prometheus metrics.")},
	)

	for _, rt := range routes {
		if !rt.documented || rt.op == nil {
			continue
		}
		if err := addOperation(r, rt); err != nil {
			return nil, fmt.Errorf("documenting %s: %w", rt.pattern(), err)
		}
	}

	// Not json.MarshalIndent: it escapes <, > and & in the descriptions.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r.Spec); err != nil { // Encode appends the final newline.
		return nil, fmt.Errorf("encoding the OpenAPI document: %w", err)
	}
	return buf.Bytes(), nil
}

func addOperation(r *openapi31.Reflector, rt route) error {
	op := rt.op
	oc, err := r.NewOperationContext(rt.method, rt.path)
	if err != nil {
		return fmt.Errorf("creating the operation: %w", err)
	}
	oc.SetID(op.id)
	oc.SetSummary(op.summary)
	oc.SetDescription(op.description)
	oc.SetTags(op.tag)
	if op.request != nil {
		oc.AddReqStructure(op.request)
	}
	for _, resp := range op.responses {
		oc.AddRespStructure(resp.body, responseOptions(resp)...)
	}
	if err := r.AddOperation(oc); err != nil {
		return fmt.Errorf("adding the operation: %w", err)
	}
	return nil
}

func responseOptions(resp response) []openapi.ContentOption {
	opts := []openapi.ContentOption{
		openapi.WithHTTPStatus(resp.status),
		func(cu *openapi.ContentUnit) { cu.Description = resp.description },
	}
	if resp.contentType != "" {
		opts = append(opts, openapi.WithContentType(resp.contentType))
	}
	if len(resp.headers) == 0 && resp.example == nil {
		return opts
	}
	return append(opts, openapi.WithCustomize(func(cor openapi.ContentOrReference) {
		ror, ok := cor.(*openapi31.ResponseOrReference)
		if !ok || ror.Response == nil {
			return
		}
		res := ror.Response
		for name, desc := range resp.headers {
			if res.Headers == nil {
				res.Headers = map[string]openapi31.HeaderOrReference{}
			}
			res.Headers[name] = openapi31.HeaderOrReference{Header: &openapi31.Header{
				Description: strPtr(desc),
				Schema:      map[string]any{"type": "string"},
			}}
		}
		if resp.example != nil {
			ct := resp.contentType
			if ct == "" {
				ct = "application/json"
			}
			mt := res.Content[ct]
			example := resp.example
			mt.Example = &example
			res.Content[ct] = mt
		}
	}))
}

func strPtr(s string) *string { return &s }

// openAPIHandler serves the generated document, or a 500 when generation
// failed (spec is nil).
func openAPIHandler(spec []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if spec == nil {
			writeError(w, http.StatusInternalServerError, CodeInternal, specFailedMessage)
			return
		}
		h := w.Header()
		h.Set("Content-Type", contentTypeJSON)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Cache-Control", "no-cache")
		h.Set("Content-Length", strconv.Itoa(len(spec)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(spec)
	})
}
