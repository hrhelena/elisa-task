package httpapi

import (
	"encoding/json"
	"net/http"
)

// Error codes shared by every endpoint (ADR 0011). Feature handlers add their
// own snake_case codes next to these.
const (
	CodeNotFound         = "not_found"
	CodeMethodNotAllowed = "method_not_allowed"
	CodeInternal         = "internal"
)

const contentTypeJSON = "application/json; charset=utf-8"

// errorBody is the error envelope of every endpoint:
// {"error":{"code":"<code>","message":"<message>"}}.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeJSON writes v as a JSON response with the given status. If v cannot be
// encoded, it writes a 500 internal error instead, so a response is always sent.
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		status = http.StatusInternalServerError
		// A fixed envelope cannot fail to encode.
		body, _ = json.Marshal(errorBody{Error: errorDetail{Code: CodeInternal, Message: "failed to encode response"}})
	}
	h := w.Header()
	h.Set("Content-Type", contentTypeJSON)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Del("Content-Length")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

// writeError writes the standard JSON error envelope.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}
