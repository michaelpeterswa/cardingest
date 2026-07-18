package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"alpineworks.io/ootel"
	"github.com/michaelpeterswa/cardingest/internal/app"
	"github.com/michaelpeterswa/cardingest/internal/config"
	"github.com/michaelpeterswa/cardingest/internal/detect"
	"github.com/michaelpeterswa/cardingest/internal/logging"
	"github.com/michaelpeterswa/cardingest/internal/mounter"
	"github.com/michaelpeterswa/cardingest/internal/notify"
	"github.com/michaelpeterswa/cardingest/internal/pipeline"
	"github.com/michaelpeterswa/cardingest/internal/web"
	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
)

func main() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "error"
	}

	slogLevel, err := logging.LogLevelToSlogLevel(logLevel)
	if err != nil {
		log.Fatalf("could not convert log level: %s", err)
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slogLevel,
	})))

	c, err := config.NewConfig()
	if err != nil {
		slog.Error("could not create config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Cancel on SIGINT/SIGTERM for a graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, c); err != nil {
		slog.Error("fatal", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(ctx context.Context, c *config.Config) error {
	// Telemetry (metrics/tracing) exactly as the template wired it.
	shutdown, err := initTelemetry(ctx, c)
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(ctx) }()

	log := slog.Default()

	// Policy config (rules, destination, reader USB IDs, ...). A missing file is
	// not fatal: a fresh deployment is configured through the UI.
	store, err := config.NewStore(c.ConfigPath)
	if err != nil {
		return fmt.Errorf("load policy config: %w", err)
	}
	policy := store.Get()

	mock := c.ReaderMode == config.ReaderModeMock

	det, err := detect.New(detect.Config{
		Mock:         mock,
		USBIDs:       policy.Reader.USBIDs,
		PollInterval: c.PollInterval,
		FixtureDir:   c.MockFixtureDir,
	}, log)
	if err != nil {
		return fmt.Errorf("init detector: %w", err)
	}
	defer det.Close()

	mnt, err := mounter.New(mounter.Config{Mock: mock, MountRoot: c.MountRoot}, log)
	if err != nil {
		return fmt.Errorf("init mounter: %w", err)
	}

	// The mock detector doubles as the web dev-trigger controller.
	var mockCtl web.MockController
	if mc, ok := det.(web.MockController); ok {
		mockCtl = mc
	}

	// HTTP API server.
	httpSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", c.HTTPPort),
		Handler:           web.NewServer(log, mockCtl).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("http api listening", slog.Int("port", c.HTTPPort))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", slog.String("error", err.Error()))
		}
	}()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutCtx)
	}()

	application := app.New(app.Config{
		Detector:  det,
		Mounter:   mnt,
		Pipeline:  pipeline.New(log),
		Notifier:  notify.Logger{Log: log},
		MountRoot: c.MountRoot,
		Log:       log,
	})

	log.Info("cardingest starting", slog.String("mode", string(c.ReaderMode)))
	return application.Run(ctx)
}

func initTelemetry(ctx context.Context, c *config.Config) (func(context.Context) error, error) {
	exporterType := ootel.ExporterTypePrometheus
	if c.Local {
		exporterType = ootel.ExporterTypeOTLPGRPC
	}

	ootelClient := ootel.NewOotelClient(
		ootel.WithMetricConfig(
			ootel.NewMetricConfig(
				c.MetricsEnabled,
				exporterType,
				c.MetricsPort,
			),
		),
		ootel.WithTraceConfig(
			ootel.NewTraceConfig(
				c.TracingEnabled,
				c.TracingSampleRate,
				c.TracingService,
				c.TracingVersion,
			),
		),
	)

	shutdown, err := ootelClient.Init(ctx)
	if err != nil {
		return nil, fmt.Errorf("init ootel: %w", err)
	}

	if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(5 * time.Second)); err != nil {
		return nil, fmt.Errorf("init runtime metrics: %w", err)
	}
	if err := host.Start(); err != nil {
		return nil, fmt.Errorf("init host metrics: %w", err)
	}
	return shutdown, nil
}
