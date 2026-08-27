# syntax=docker/dockerfile:1

# Build on the runner architecture while cross-compiling for the requested
# image platform. Keeping build stages on BUILDPLATFORM avoids QEMU for this
# pure-Go binary.
FROM --platform=$BUILDPLATFORM golang:1.26.7-alpine3.24 AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG GIT_REVISION=unknown
ARG GIT_BRANCH=unknown

WORKDIR /src

# Cache module downloads independently from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
    go build -mod=readonly -trimpath \
    -ldflags="-X main.Branch=${GIT_BRANCH} -X main.Revision=${GIT_REVISION} -X main.Version=${VERSION} -s -w" \
    -o /out/plex-exporter ./cmd/plex-exporter

# Certificates and timezone data are architecture-independent, so this stage
# also runs natively on the builder.
FROM --platform=$BUILDPLATFORM alpine:3.24.1 AS runtime-data
# The Alpine minor release is pinned while these packages intentionally track
# its security updates; exact package pins would make future rebuilds brittle.
# hadolint ignore=DL3018
RUN apk add --no-cache ca-certificates tzdata

FROM scratch

ARG VERSION=dev
ARG GIT_REVISION=unknown

LABEL org.opencontainers.image.title="plex-exporter" \
      org.opencontainers.image.description="Prometheus exporter for Plex Media Server" \
      org.opencontainers.image.source="https://github.com/naterator/plex-exporter" \
      org.opencontainers.image.licenses="BSD-3-Clause" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$GIT_REVISION"

COPY --from=runtime-data /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=runtime-data /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=runtime-data /usr/share/zoneinfo/UTC /etc/localtime
COPY --from=build /out/plex-exporter /plex-exporter
COPY LICENSE /licenses/LICENSE

EXPOSE 9000
USER 65534:65534
ENTRYPOINT ["/plex-exporter"]
