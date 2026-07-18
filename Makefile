# Makefile
all: setup hooks

# requires `nvm use --lts` or `nvm use node`
.PHONY: setup
setup:
	npm install -g @commitlint/config-conventional @commitlint/cli


.PHONY: hooks
hooks:
	@git config --local core.hooksPath .githooks/

# Regenerate API handlers/types from api/openapi.yaml.
.PHONY: generate
generate:
	go generate ./...

.PHONY: build
build:
	go build ./...

.PHONY: test
test:
	go test ./...

# Run locally in mock mode against the fixture cards (no hardware). Ingests land
# in ./.dev-ingest and the hash index in ./.dev-state (both gitignored).
.PHONY: run-mock
run-mock:
	READER_MODE=mock MOCK_FIXTURE_DIR=./testdata/cards \
	CONFIG_PATH=./config/config.dev.yaml STATE_PATH=./.dev-state/cardingest.db \
	LOG_LEVEL=info METRICS_ENABLED=false go run ./cmd/cardingest