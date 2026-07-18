package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// ReaderMode selects how cardingest talks to the reader hardware.
type ReaderMode string

const (
	// ReaderModeReal polls /dev and mounts via syscalls. Linux only.
	ReaderModeReal ReaderMode = "real"
	// ReaderModeMock simulates a reader from a fixture directory. Any OS.
	ReaderModeMock ReaderMode = "mock"
)

// Config is the immutable bootstrap configuration, sourced entirely from the
// environment. It decides how and where the process boots (mode, filesystem
// paths, ports, telemetry) and is never rewritten by the UI. Hot ingest policy
// (rules, destination, categories, USB IDs, notifiers) lives in the YAML File.
type Config struct {
	LogLevel string `env:"LOG_LEVEL" envDefault:"error"`

	// Reader / ingest lifecycle.
	ReaderMode     ReaderMode    `env:"READER_MODE" envDefault:"real"`
	MockFixtureDir string        `env:"MOCK_FIXTURE_DIR"`
	ConfigPath     string        `env:"CONFIG_PATH" envDefault:"/config/config.yaml"`
	StatePath      string        `env:"STATE_PATH" envDefault:"/state/cardingest.db"`
	MountRoot      string        `env:"MOUNT_ROOT" envDefault:"/run/cardingest/mnt"`
	HTTPPort       int           `env:"HTTP_PORT" envDefault:"8080"`
	PollInterval   time.Duration `env:"POLL_INTERVAL" envDefault:"2s"`

	MetricsEnabled bool `env:"METRICS_ENABLED" envDefault:"true"`
	MetricsPort    int  `env:"METRICS_PORT" envDefault:"8081"`

	Local bool `env:"LOCAL" envDefault:"false"`

	TracingEnabled    bool    `env:"TRACING_ENABLED" envDefault:"false"`
	TracingSampleRate float64 `env:"TRACING_SAMPLERATE" envDefault:"0.01"`
	TracingService    string  `env:"TRACING_SERVICE" envDefault:"cardingest"`
	TracingVersion    string  `env:"TRACING_VERSION"`
}

func NewConfig() (*Config, error) {
	var cfg Config

	err := env.Parse(&cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	switch cfg.ReaderMode {
	case ReaderModeReal, ReaderModeMock:
	default:
		return nil, fmt.Errorf("invalid READER_MODE %q (want real|mock)", cfg.ReaderMode)
	}

	return &cfg, nil
}
