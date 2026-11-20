# syntax=docker/dockerfile:1
#
# One image holds every binary: api, scheduler, worker and jobctl. Each
# Compose service picks one with its command.

ARG GO_VERSION=1.27

# ---- build ----------------------------------------------------------------
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

# Dependencies first, so editing code doesn't download them again.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd ./cmd
COPY internal ./internal
# Static binaries (no cgo), with no local paths or debug symbols in them.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# ---- run ------------------------------------------------------------------
# Distroless: no shell or package manager, and a non-root user (65532).
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/ /usr/local/bin/

# Workers and schedulers touch this file whenever they get through to Redis.
# Their `healthcheck` subcommand reads it.
ENV HEALTH_FILE=/tmp/healthy
EXPOSE 8080
USER nonroot:nonroot
CMD ["jobctl", "--help"]
