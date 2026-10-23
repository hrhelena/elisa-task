# syntax=docker/dockerfile:1
# Container image for enron-service (ADR 0006, ticket #11).
# Stage 1 builds a static binary; stage 2 is distroless (no shell, no package manager) running as non-root.
# Base images are pinned by digest; bump the tag and the digest together
# (`docker buildx imagetools inspect <image>:<tag>` prints the index digest).

# The Go version must match go.mod.
FROM golang:1.27.2-alpine3.24@sha256:f92b6ef800e499660581efdabdf25d9d817a9d124eaf900924f0504e7e27e12d AS build

WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly

# Module downloads are cached separately from the source, so a source-only change skips them.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd/ cmd/
COPY internal/ internal/

ARG VERSION=dev
# TARGETOS/TARGETARCH are set by BuildKit, so `docker buildx build --platform` cross-compiles natively.
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/enron-service ./cmd/enron-service

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

ARG VERSION=dev
ARG REVISION=unknown
ARG CREATED=unknown
LABEL org.opencontainers.image.title="enron-service" \
      org.opencontainers.image.description="Ingests the Enron maildir dataset into Postgres and serves email metrics over HTTP" \
      org.opencontainers.image.source="https://github.com/hrhelena/elisa-task" \
      org.opencontainers.image.url="https://github.com/hrhelena/elisa-task" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.base.name="gcr.io/distroless/static-debian12:nonroot"

COPY --from=build /out/enron-service /enron-service

# The dataset volume is mounted here (compose: ./Dataset, k8s: a PVC).
ENV DATA_DIR=/data
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/enron-service"]
