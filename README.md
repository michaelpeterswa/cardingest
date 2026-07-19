# cardingest

An SD / CFexpress **ingest appliance**: a single Go binary, deployed in Docker on a
Linux host, that detects cards inserted into a Lexar RW530 dual-slot USB reader,
copies wanted files to a NAS, organizes them by type and date, verifies every copy
by reading it back, then erases the ingested originals from the card.

See [INSTRUCTIONS.md](INSTRUCTIONS.md) for the full specification.

> **Status:** Milestone 4 — completion/error notifications with job stats,
> delivered to the [pulsar-notification-pipeline](https://github.com/michaelpeterswa/pulsar-notifcation-pipeline)
> writer (the only backend today; the `Notifier` interface + `notify.Build`
> factory make new backends drop-in). Built on the M1–M3 detect/mount, copy →
> verify → erase pipeline with SQLite idempotence, and the ordered rules engine.
> Real EXIF/MP4 dating and the web UI land in later milestones.

## How it runs

Every OS/hardware touchpoint (device detection, mounting, card/NAS file I/O) sits
behind an interface with two implementations:

- **`real`** — the deployed appliance on Linux: polls `/dev/disk/by-path/*usb*`,
  matches the reader by USB vendor:product ID, and mounts via syscalls.
- **`mock`** — local development on any OS (including macOS, where USB passthrough
  and mount syscalls are unavailable): a simulated reader "inserts" a card from a
  fixture directory and a fake mounter exposes it through an in-memory filesystem.

`READER_MODE` selects between them.

## Configuration

Bootstrap configuration (immutable, set at deploy time) comes from environment
variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `READER_MODE` | `real` (Linux appliance) or `mock` (dev) | `real` |
| `MOCK_FIXTURE_DIR` | Fixture card directory root (mock mode) | - |
| `CONFIG_PATH` | Path to the YAML policy config | `/config/config.yaml` |
| `STATE_PATH` | SQLite state DB (jobs, files, hash index) | `/state/cardingest.db` |
| `MOUNT_ROOT` | Where cards are mounted | `/run/cardingest/mnt` |
| `HTTP_PORT` | API + UI port | `8080` |
| `POLL_INTERVAL` | Device poll interval | `2s` |
| `LOG_LEVEL` | `debug`/`info`/`warn`/`error` | `error` |
| `METRICS_ENABLED` | Enable Prometheus metrics | `true` |
| `METRICS_PORT` | Metrics endpoint port | `8081` |
| `LOCAL` | Use OTLP gRPC exporter instead of Prometheus | `false` |
| `TRACING_ENABLED` | Enable distributed tracing | `false` |
| `TRACING_SAMPLERATE` | Trace sampling rate | `0.01` |
| `TRACING_SERVICE` | Service name for traces | `cardingest` |

Runtime **policy** (destination layout, categories, rules, reader USB IDs,
notifiers) lives in the YAML file at `CONFIG_PATH` and is edited by the web UI.
See [INSTRUCTIONS.md](INSTRUCTIONS.md) for its schema.

## API

The HTTP API is OpenAPI-first: [`api/openapi.yaml`](api/openapi.yaml) is the source
of truth, and Go handlers/types are generated from it with
[`oapi-codegen`](https://github.com/oapi-codegen/oapi-codegen) (`make generate`).
All routes are versioned under `/api/v1/`.

## Getting Started

### Run locally in mock mode (no hardware)

```bash
make generate   # regenerate API handlers from api/openapi.yaml
READER_MODE=mock MOCK_FIXTURE_DIR=./testdata/cards LOG_LEVEL=info \
  go run ./cmd/cardingest
# in another shell, simulate inserting a card:
curl -XPOST localhost:8080/api/v1/dev/insert \
  -d '{"slot":"A","dir":"./testdata/cards/card1"}'
```

`make run-mock` wraps the run command.

### Deploy the appliance (Linux)

```bash
docker compose -f docker-compose.appliance.yml up -d
```

### Local observability stack (dev)

```bash
docker compose up
```

Starts the app plus the Grafana LGTM (Loki, Grafana, Tempo, Mimir) stack:

- **Grafana UI**: http://localhost:3000
- **OTLP gRPC/HTTP**: ports 4317 / 4318

## Project Structure

```
.
├── api/openapi.yaml         # API contract (source of truth)
├── cmd/cardingest/          # entrypoint
├── internal/
│   ├── card/                # shared domain types
│   ├── config/              # env bootstrap + YAML policy config
│   ├── detect/              # /dev poller, USB matching, per-slot state (real + mock)
│   ├── mounter/             # mount ro → rw → eject lifecycle (real + fake)
│   ├── app/                 # orchestrator: per-slot ingest workers
│   ├── pipeline/            # scan/copy/verify/erase  (stub in M1)
│   ├── rules/ exifdate/ store/ notify/   # stubs in M1
│   └── web/                 # HTTP server + generated /api/v1 handlers
├── Dockerfile               # multi-stage distroless build
├── docker-compose.yml       # dev + observability stack
└── docker-compose.appliance.yml   # production deployment
```

## CI/CD

Pull requests are validated with commitlint, golangci-lint, yamllint, hadolint,
and `go test`.
