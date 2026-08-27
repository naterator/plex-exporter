# plex-exporter

[![CI](https://github.com/naterator/plex-exporter/actions/workflows/ci.yml/badge.svg)](https://github.com/naterator/plex-exporter/actions/workflows/ci.yml)
[![License: BSD-3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)

`plex-exporter` exposes Plex Media Server metrics in Prometheus format. It
collects server and library statistics through the Plex API and tracks playback
sessions through Plex's WebSocket event stream.

It reports:

- CPU, memory, bandwidth, storage, duration, and media counts
- Playback time and play counts by user, device, title, and library
- Stream, transcode, resolution, bitrate, and subtitle details

## Run with Docker

```bash
docker run -d \
  --name plex-exporter \
  --restart unless-stopped \
  -p 127.0.0.1:9000:9000 \
  -e PLEX_SERVER="http://192.168.1.100:32400" \
  -e PLEX_TOKEN="your-plex-token" \
  ghcr.io/naterator/plex-exporter:latest
```

Metrics are available at `http://127.0.0.1:9000/metrics`.

Add the exporter to Prometheus:

```yaml
scrape_configs:
  - job_name: plex
    static_configs:
      - targets: ["exporter-host:9000"]
```

The exporter does not authenticate `/metrics`. Playback labels can expose Plex
usernames, devices, and media titles, so bind it to localhost or a trusted
network, or place it behind an authenticated reverse proxy.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `PLEX_SERVER` | required | Full `http` or `https` URL of the Plex server. |
| `PLEX_TOKEN` | required | Plex [authentication token](https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/). |
| `LIBRARY_REFRESH_INTERVAL` | `15` | Minutes between expensive library-count queries. `0` disables caching and queries on every five-second refresh. |
| `LIBRARY_METADATA_REFRESH_INTERVAL` | `15` | Minutes between library inventory and storage-total queries. `0` queries on every five-second refresh. |
| `PLEX_CLIENT_TIMEOUT_SECONDS` | `10` | Timeout for Plex HTTP requests. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error`. |
| `LOG_FORMAT` | JSON | Set to `console` for human-readable output. |
| `ENVIRONMENT` | unset | Set to `development` to use console logging. |
| `TZ` | system timezone | IANA timezone such as `America/Chicago`. |
| `SKIP_TLS_VERIFICATION` | `false` | Skip TLS certificate validation. Insecure; use only on trusted networks. |

The binary does not load `.env` files. Pass one with Docker's `--env-file`
option or configure variables through your service manager.

### Library refresh behavior

The exporter discovers the library inventory at startup, waits about 15 seconds
before its first full library count, and refreshes current server data every
five seconds. Library inventory and storage totals are reused until
`LIBRARY_METADATA_REFRESH_INTERVAL` expires because Plex's
`/media/providers?includeStorage=1` endpoint can scan a substantial portion of
large library databases. Expensive item, episode, and track counts are reused
until `LIBRARY_REFRESH_INTERVAL` expires. Failed metadata or count requests are
not retried until their corresponding interval expires, preventing rapid
retries against an unavailable Plex server.

Keep the default intervals unless you need fresher library metadata or counts.
Setting either interval to `0` can create substantial Plex and storage I/O on
large libraries.

## Metrics

| Metric | Description |
| --- | --- |
| `server_info` | Plex version and platform information. |
| `host_cpu_util` | Host CPU utilization reported by Plex. |
| `host_mem_util` | Host memory utilization reported by Plex. |
| `library_duration_total` | Library duration in milliseconds. |
| `library_storage_total` | Library storage in bytes. |
| `plex_library_items` | Items in each library, including a `content_type` label. |
| `plex_media_movies` | Total movies. |
| `plex_media_episodes` | Total TV episodes. |
| `plex_media_music` | Total music tracks. |
| `plex_media_photos` | Total photos. |
| `plex_media_other_videos` | Total home and other videos. |
| `plays_total` | Play count by session and media attributes. |
| `play_seconds_total` | Playback time by session and media attributes. |
| `transmit_bytes_total` | Bytes transmitted according to Plex statistics. |
| `estimated_transmit_bytes_total` | Estimated transmitted bytes. |

Server series use `server_type`, `server`, and `server_id` labels. Library
series add `library_type`, `library`, and `library_id`. Playback series also
include media titles, stream properties, device, user, session, transcode, and
subtitle labels.

## Build from source

The project uses the Go version in [`.go-version`](.go-version).

```bash
git clone https://github.com/naterator/plex-exporter.git
cd plex-exporter
go build -o plex-exporter ./cmd/plex-exporter

PLEX_SERVER="http://192.168.1.100:32400" \
PLEX_TOKEN="your-plex-token" \
./plex-exporter
```

Make targets are also available:

```bash
make build      # current OS and architecture
make build-all  # Linux, macOS, and FreeBSD; amd64 and arm64
docker build -t plex-exporter:local .
```

Make writes binaries to `bin/<os>/plex-exporter-<arch>`.

## Development

```bash
gofmt -s -w $(git ls-files '*.go')
go mod verify
go mod tidy -diff
golangci-lint run ./... --timeout=5m
go vet -mod=readonly ./...
go test -race -mod=readonly ./...
```

Pull requests run formatting, module, lint, vet, race-test, cross-build, and
multi-platform container checks in GitHub Actions.

## Releases

A successful `main` build publishes Linux amd64 and arm64 images to
[GHCR](https://github.com/naterator/plex-exporter/pkgs/container/plex-exporter)
as `latest` and a short commit tag. CI also stores Linux, macOS, and FreeBSD
binaries for amd64 and arm64 as workflow artifacts. Pin a commit tag or image
digest for reproducible deployments.

## License

[BSD 3-Clause](LICENSE)
