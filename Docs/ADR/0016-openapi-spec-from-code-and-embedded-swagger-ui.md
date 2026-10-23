# 0016. OpenAPI 3.1 spec generated from code, with an embedded Swagger UI

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#54](https://github.com/hrhelena/elisa-task/issues/54), [0005](0005-no-authentication-or-authorization.md), [0008](0008-project-local-pinned-tooling-and-ci.md), [0009](0009-extensible-layered-architecture.md), [0011](0011-http-api-contract-and-start-semantics.md)

## Summary

The service serves an OpenAPI 3.1 document at `GET /openapi.json`, generated at startup from the Go request and response
types with `github.com/swaggest/openapi-go`, and a Swagger UI at `GET /docs` whose assets are vendored at a pinned
version and embedded with `go:embed`. Both are always on and unauthenticated. A test fails if the spec and the mux routes differ.

## Context

- The API contract ([0011](0011-http-api-contract-and-start-semantics.md), [0004](0004-add-fetch-endpoint-for-dataset-download.md))
  is documented only in prose in the README. A machine-readable spec and an interactive UI make the service easier to
  try and review.
- Handlers are plain `net/http` `ServeMux` handlers in `internal/httpapi` ([0009](0009-extensible-layered-architecture.md)),
  registered with method-qualified patterns (`GET /top-senders`). Some routes are registered only when their dependency
  is set, so the set of routes depends on the wiring.
- The service runs in kind and in clusters that may have no internet egress ([0006](0006-cloud-native-deployment-with-kustomize-and-kind.md)),
  in a distroless image.
- There is no authentication ([0005](0005-no-authentication-or-authorization.md)), and the docs are read-only.
- Swagger UI `swagger-ui-dist` 5.x: `swagger-ui-bundle.js` is about 1.7 MB and `swagger-ui.css` about 190 KB; the other
  files (source maps, ES bundles, standalone preset) are not needed.

## Decision

**Spec source.** Generate the document from code with `github.com/swaggest/openapi-go` (package `openapi31`):

- A small route table in `internal/httpapi` lists every route (method, path, handler, mutating or read, and the
  operation metadata: summary, tag, request type, responses). The same table registers the routes on the mux and
  feeds the reflector, so a route whose dependency is nil is neither served nor documented.
- Schemas are reflected from the types the handlers already encode (`statusResponse`, `datasetJSON`,
  `topSendersResponse`, `errorBody`, ...). Constraints use struct tags on those `internal/httpapi` types only
  (`enum`, `minimum`, `maximum`, `default`, `description`); there are no comment annotations and no generate or CLI step.
- The handlers stay unchanged `net/http` handlers on `http.ServeMux`.
- The document is built once in `httpapi.New` and the bytes are cached.
- A unit test fails if the spec's operations and the mux's registered routes differ (apart from the docs routes
  themselves). Another test validates the document against the official OAS 3.1 JSON Schema.

**Format.** JSON only, at `GET /openapi.json` with `Content-Type: application/json; charset=utf-8`. JSON needs no
extra dependency (`encoding/json`), Swagger UI and OpenAPI tooling read it natively, and one format means one route
and one golden file. YAML can be produced locally with the pinned `yq`.

**UI.** Swagger UI with "Try it out" enabled (`tryItOutEnabled: true`). `GET /docs` redirects to `/docs/`, which
serves an index page that loads the spec from the same-origin relative URL `../openapi.json`. The default online
validator is disabled (`validatorUrl: null`).

**Assets.** `swagger-ui-bundle.js`, `swagger-ui.css`, the favicons and the license files from the npm package
`swagger-ui-dist` are vendored under `internal/httpapi/swaggerui/` at a pinned version, recorded in `VERSION` with a
`SHA256SUMS` file, and embedded with `go:embed`. Our own `index.html` and initializer script sit next to them. A script
behind `make swagger-ui` re-vendors a given version and checks the npm integrity hash. Nothing is loaded from a CDN.

**Exposure.** Always on, with no config flag. `GET /openapi.json` and `GET /docs*` are read-only routes outside the
mutating middleware chain, with no auth, consistent with [0005](0005-no-authentication-or-authorization.md). Their
access log lines are at DEBUG and their metrics carry the route pattern, as for `/healthz`, `/readyz` and `/metrics`.

## Rationale

Spec source:

| Option | Pros | Cons |
|---|---|---|
| swaggest/openapi-go, reflected from the handler types, plus a route table (chosen) | OpenAPI 3.1; schemas come from the types that are actually encoded, so field names cannot drift; no annotations or generate step; handlers unchanged; the route-equality test catches a missing or stale operation | A runtime dependency; operation metadata (status codes, descriptions) is still written by hand in the route table |
| Hand-written spec, checked by a contract test | No dependency; full control over wording | Every field is written twice; the contract test only catches what it checks, so the spec drifts quietly |
| swaggo/swag (comment annotations plus `swag init`) | Popular; familiar | The stable release emits Swagger 2.0, not OpenAPI 3; comments are not type-checked and drift from the code; adds a generate step and generated files |
| huma | OpenAPI 3.1 and validation built in | Every handler must be rewritten to huma's signatures; a framework change far larger than documentation |

UI:

| Option | Pros | Cons |
|---|---|---|
| Swagger UI (chosen) | The reference UI; "Try it out" is mature; widely known by reviewers | The largest bundle (about 1.7 MB JS) |
| Scalar | Modern look; built-in client | Younger; the self-hosted bundle is also large; less familiar |
| Redoc | Clean read-only docs; small | No "Try it out" in the open-source version |

Assets:

| Option | Pros | Cons |
|---|---|---|
| Vendored at a pinned version, embedded (chosen) | Works offline and in clusters without egress; reproducible; reviewable diff on upgrade; no third-party request from the browser | About 1.9 MB added to the binary and repo; upgrades are manual (`make swagger-ui`) |
| Loaded from a CDN (unpkg, jsDelivr) | Nothing in the repo; tiny binary | Breaks offline and in locked-down clusters; a supply-chain and privacy exposure; version drift unless SRI hashes are maintained |

Exposure:

| Option | Pros | Cons |
|---|---|---|
| Always on (chosen) | Nothing to configure; the docs always match the running binary | Every deployment exposes the docs; acceptable since they are read-only and reveal only the public contract |
| Config flag (`DOCS_ENABLED`) | Can be turned off in production | Another env var and test matrix for a service without auth, where every endpoint is already open ([0005](0005-no-authentication-or-authorization.md)) |

## Consequences

- Positive: the contract is browsable and executable from a browser, offline; the spec cannot silently lose or gain
  an endpoint, and field names come from the encoded types; the README can link to the spec instead of repeating it.
- Negative: a new runtime dependency (`swaggest/openapi-go` and `swaggest/jsonschema-go`); the binary and image grow by
  about 2 MB; vendored JavaScript must be bumped by hand for security fixes; "Try it out" on `POST /fetch` and
  `POST /start` starts real work, like any client (there is no auth).
- Follow-ups: [#54](https://github.com/hrhelena/elisa-task/issues/54) implements this. Possible later work: serving
  under a path prefix (a `servers` entry), YAML output, request validation driven by the spec.
