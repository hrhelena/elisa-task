# 0005. No authentication or authorization (extension point only)

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#3](https://github.com/hrhelena/elisa-task/issues/3), [#14](https://github.com/hrhelena/elisa-task/issues/14), [0006](0006-cloud-native-deployment-with-kustomize-and-kind.md), [0009](0009-extensible-layered-architecture.md)

## Summary

The service implements no authentication, authorization or accounting (AAA/IAM). The HTTP layer is built around a
middleware chain so that authn/authz can be added later without changing handlers.

## Context

- This is a toy assignment; the task description does not mention security.
- `POST /fetch` and `POST /start` trigger expensive work, so a real deployment would need to protect them.

## Decision

- No login, tokens, API keys, roles or audit log are implemented.
- HTTP handlers are wrapped by an explicit middleware chain (`func(http.Handler) http.Handler`) composed in one place
  in `internal/httpapi`: request ID, structured access logging, panic recovery, metrics. An auth middleware slots into
  the same chain.
- Handlers make no assumptions about the caller's identity. If identity is needed later, it is read from the request
  context through a small accessor, set by the middleware.
- Routes are registered so that mutating endpoints (`POST /fetch`, `POST /start`) and read endpoints can get different
  middleware (for example, admin-only for mutations).
- In Kubernetes, auth can alternatively be enforced outside the service: at the ingress (OAuth2 proxy, cloud IAP) or in a
  service mesh (mTLS plus authorization policies).

## Rationale

| Option | Pros | Cons |
|---|---|---|
| None, with middleware extension point (chosen) | No scope creep; easy to add later | Endpoints are open when deployed |
| Static API key | Cheap | Secret management for a demo; still not real IAM |
| OIDC / JWT | Realistic | Needs an identity provider; out of scope |

## Consequences

- Positive: less code, easier demo; adding auth is a new middleware plus config.
- Negative: anyone who reaches the service can trigger a fetch or an ingestion. Deployments must not be publicly exposed
  (the k8s manifests use a `ClusterIP` service by default).
- Follow-ups: the README lists authn/authz as an extension.
