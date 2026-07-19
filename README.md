# cardingest

An SD / CFexpress **ingest appliance**: a single Go binary, deployed in Docker on a
Linux host, that detects cards inserted into a Lexar RW530 dual-slot USB reader,
copies wanted files to a NAS, organizes them by type and date, verifies every copy
by reading it back, then erases the ingested originals from the card.

See [INSTRUCTIONS.md](INSTRUCTIONS.md) for the full specification.

> **Status:** Milestone 5 — the embedded web UI is in. A `go:embed` single-page
> UI (served at `/`) shows live stats, per-slot progress, and job history, and
> round-trips the YAML policy. It's backed by REST (`/api/v1/jobs`, `stats`,
> `status`, `config`) and a hand-written SSE stream (`/api/v1/events`) fed by a
> job-history table in SQLite and a live pipeline progress callback. This
> completes the M1–M4 core (detect/mount, copy → verify → erase with SQLite
> idempotence, the ordered rules engine, and notifications), plus the polish
> pass: real EXIF/MP4 capture-date foldering, per-file ingest resilience (one
> bad file no longer strands the card), and a fixture-tested Linux enumerator.

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

## Validating the reader on the appliance

The mock path is covered by tests; the real Linux enumeration is tested against a
fixture sysfs tree (`internal/detect/detect_linux_test.go`, runs in CI). The one
thing that can only be confirmed on the actual hardware is the reader's USB IDs
and slot topology:

1. Plug in the reader and list USB devices: `lsusb` (note the `ID vvvv:pppp` of
   each **mass-storage** function — the RW530 exposes one bridge per slot, so
   you'll see two).
2. Insert a card and confirm a block device appears: `ls -l /dev/disk/by-path/ | grep usb`.
   Read its IDs from sysfs, e.g.
   `cat /sys/class/block/sdX/device/../../idVendor /sys/class/block/sdX/device/../../idProduct`.
3. Put those `vendor:product` pairs in `reader.usb_ids` in the config, set
   `READER_MODE=real`, and start the appliance. An empty slot keeps its
   `/dev/sdX` node but reports size 0 — cardingest ignores it (only a card with a
   real medium triggers ingest).

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
│   ├── pipeline/            # scan → rules → copy → verify → erase
│   ├── rules/               # ordered keep/skip engine
│   ├── store/               # SQLite: hash index + job history/stats
│   ├── notify/              # pulsar notifier (extensible)
│   ├── events/              # in-process pub/sub for live SSE
│   ├── exifdate/            # EXIF/MP4 capture-date extraction (mtime fallback)
│   └── web/                 # REST + SSE + go:embed UI (web/ui/)
├── Dockerfile               # multi-stage distroless build
├── docker-compose.yml       # dev + observability stack
└── docker-compose.appliance.yml   # production deployment
```

## CI/CD

Pull requests are validated with commitlint, golangci-lint, yamllint, hadolint,
and `go test`.
