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
	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/michaelpeterswa/cardingest/internal/config"
	"github.com/michaelpeterswa/cardingest/internal/detect"
	"github.com/michaelpeterswa/cardingest/internal/events"
	"github.com/michaelpeterswa/cardingest/internal/exifdate"
	"github.com/michaelpeterswa/cardingest/internal/logging"
	"github.com/michaelpeterswa/cardingest/internal/mounter"
	"github.com/michaelpeterswa/cardingest/internal/notify"
	"github.com/michaelpeterswa/cardingest/internal/pipeline"
	"github.com/michaelpeterswa/cardingest/internal/rules"
	"github.com/michaelpeterswa/cardingest/internal/store"
	"github.com/michaelpeterswa/cardingest/internal/web"
	"github.com/spf13/afero"
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
	policyStore, err := config.NewStore(c.ConfigPath)
	if err != nil {
		return fmt.Errorf("load policy config: %w", err)
	}
	policy := policyStore.Get()

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
	defer func() { _ = det.Close() }()

	mnt, err := mounter.New(mounter.Config{Mock: mock, MountRoot: c.MountRoot}, log)
	if err != nil {
		return fmt.Errorf("init mounter: %w", err)
	}

	// Verified-hash index (SQLite) for idempotence/dedupe.
	hashStore, err := store.OpenSQLite(c.StatePath)
	if err != nil {
		return fmt.Errorf("open state db: %w", err)
	}
	defer func() { _ = hashStore.Close() }()

	// Destination shares. Each category routes to its own path/layout/marker,
	// inheriting the top-level destination when unset. A distinct afero.Fs is
	// created per share path (deduped) so writes and atomic renames stay within
	// one filesystem. The host mounts the shares; we write under them.
	defaultDest := policy.Destination.Path
	if defaultDest == "" {
		defaultDest = "/data/dest"
	}

	destFS := map[string]afero.Fs{}
	getDest := func(path string) afero.Fs {
		if fs, ok := destFS[path]; ok {
			return fs
		}
		if err := os.MkdirAll(path, 0o755); err != nil {
			log.Warn("could not create destination dir (ok if the share mounts it)",
				slog.String("path", path), slog.String("error", err.Error()))
		}
		fs := afero.NewBasePathFs(afero.NewOsFs(), path)
		destFS[path] = fs
		return fs
	}

	var routes []pipeline.Route
	for name, cat := range policy.Categories {
		dp := cat.Dest
		if dp == "" {
			dp = defaultDest
		}
		layout := cat.Layout
		if layout == "" {
			layout = policy.Destination.Layout
		}
		marker := cat.Marker
		if marker == "" {
			marker = policy.Destination.Marker
		}
		routes = append(routes, pipeline.Route{
			Category: name, Exts: cat.Ext, Dest: getDest(dp), Layout: layout, Marker: marker,
		})
	}

	// Fail-loud if a share's marker is absent at startup — a strong hint it isn't
	// mounted. Not fatal here (it may mount momentarily); each job re-checks and
	// refuses to erase against a share whose marker is still missing.
	warned := map[string]bool{}
	for _, r := range routes {
		if r.Marker == "" || warned[r.Category] {
			continue
		}
		if ok, _ := afero.Exists(r.Dest, r.Marker); !ok {
			log.Warn("destination marker missing at startup; share may not be mounted",
				slog.String("category", r.Category), slog.String("marker", r.Marker))
		}
		warned[r.Category] = true
	}

	// Compile the ordered keep/skip rules from the policy config.
	ruleEngine, err := rules.Compile(policy.Rules)
	if err != nil {
		return fmt.Errorf("compile rules: %w", err)
	}

	// Live-event hub: pipeline progress and app job events flow here and out to
	// the UI via SSE.
	hub := events.NewHub()

	pipe := pipeline.New(pipeline.Deps{
		Routes: routes,
		Store:  hashStore,
		Rules:  ruleEngine,
		DateFn: func(fs afero.Fs, f card.FileEntry) time.Time { t, _ := exifdate.DateOf(fs, f); return t },
		OnProgress: func(p pipeline.Progress) {
			hub.Publish(events.Event{Type: "progress", Slot: p.Slot, Data: p})
		},
		Log: log,
	})

	// Notifiers from policy (empty => dev Logger). A misconfigured notifier is
	// logged but never blocks ingest.
	notifier, nerr := notify.Build(policy.Notify, log)
	if nerr != nil {
		log.Warn("some notifiers could not be built", slog.String("error", nerr.Error()))
	}

	application := app.New(app.Config{
		Detector:  det,
		Mounter:   mnt,
		Pipeline:  pipe,
		Notifier:  notifier,
		MountRoot: c.MountRoot,
		Policy: app.ErasePolicy{
			EraseIngested: policy.Card.EraseIngested,
			EraseSkipped:  policy.Card.EraseSkipped,
			EjectWhenDone: policy.Card.EjectWhenDone,
		},
		Events: hub,
		Jobs:   hashStore,
		Log:    log,
	})

	// The mock detector doubles as the web dev-trigger controller.
	var mockCtl web.MockController
	if mc, ok := det.(web.MockController); ok {
		mockCtl = mc
	}

	// HTTP API + UI server.
	httpSrv := &http.Server{
		Addr: fmt.Sprintf(":%d", c.HTTPPort),
		Handler: web.NewServer(web.Deps{
			Log:    log,
			Mock:   mockCtl,
			Jobs:   hashStore,
			Config: policyStore,
			Status: application,
			Events: hub,
		}).Handler(),
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
