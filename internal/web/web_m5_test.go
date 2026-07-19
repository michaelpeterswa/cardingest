package web

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/app"
	"github.com/michaelpeterswa/cardingest/internal/config"
	"github.com/michaelpeterswa/cardingest/internal/events"
	"github.com/michaelpeterswa/cardingest/internal/store"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type fakeJobs struct {
	jobs  []store.Job
	stats store.Stats
}

func (f fakeJobs) ListJobs(context.Context, int) ([]store.Job, error) { return f.jobs, nil }
func (f fakeJobs) Stats(context.Context) (store.Stats, error)         { return f.stats, nil }

type fakeStatus struct{ s []app.SlotState }

func (f fakeStatus) Status() []app.SlotState { return f.s }

func TestGetStatus(t *testing.T) {
	h := NewServer(Deps{Log: discardLog(), Status: fakeStatus{s: []app.SlotState{
		{Slot: "A", State: "ingesting", JobID: 7},
	}}}).Handler()
	rec := do(t, h, http.MethodGet, "/api/v1/status", "")
	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"state":"ingesting"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGetJobsAndStats(t *testing.T) {
	fj := fakeJobs{
		jobs:  []store.Job{{ID: 1, Slot: "A", Serial: "c", Status: store.JobComplete, Copied: 3, Bytes: 99}},
		stats: store.Stats{TotalJobs: 1, CompleteJobs: 1, FilesCopied: 3, Bytes: 99},
	}
	h := NewServer(Deps{Log: discardLog(), Jobs: fj}).Handler()

	rec := do(t, h, http.MethodGet, "/api/v1/jobs", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"copied":3`) {
		t.Fatalf("jobs: %d %s", rec.Code, rec.Body.String())
	}
	rec = do(t, h, http.MethodGet, "/api/v1/stats", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"filesCopied":3`) {
		t.Fatalf("stats: %d %s", rec.Code, rec.Body.String())
	}
}

func TestConfigRoundTripAPI(t *testing.T) {
	cs, err := config.NewStore(filepath.Join(t.TempDir(), "config.yaml"))
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	h := NewServer(Deps{Log: discardLog(), Config: cs}).Handler()

	// GET returns YAML.
	rec := do(t, h, http.MethodGet, "/api/v1/config", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"yaml"`) {
		t.Fatalf("get config: %d %s", rec.Code, rec.Body.String())
	}

	// PUT valid config saves.
	valid := "{\"yaml\":\"categories:\\n  photos: [\\\".arw\\\"]\\nrules:\\n  - name: keep\\n    action: keep\\n    match:\\n      ext: [\\\".arw\\\"]\\n  - default: skip\\n\"}"
	rec = do(t, h, http.MethodPut, "/api/v1/config", valid)
	if rec.Code != 200 {
		t.Fatalf("put valid: %d %s", rec.Code, rec.Body.String())
	}
	if got := cs.Get(); len(got.Rules) != 2 || got.Categories["photos"][0] != ".arw" {
		t.Fatalf("config not saved: %+v", got)
	}

	// PUT with an invalid rule action is rejected.
	bad := `{"yaml":"rules:\n  - action: purge\n"}`
	rec = do(t, h, http.MethodPut, "/api/v1/config", bad)
	if rec.Code != 400 {
		t.Fatalf("put invalid: %d %s", rec.Code, rec.Body.String())
	}
}

func TestSSEUnavailableWithoutHub(t *testing.T) {
	h := NewServer(Deps{Log: discardLog()}).Handler()
	rec := do(t, h, http.MethodGet, "/api/v1/events", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestSSEStream(t *testing.T) {
	hub := events.NewHub()
	srv := httptest.NewServer(NewServer(Deps{Log: discardLog(), Events: hub}).Handler())
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/v1/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}

	// Wait for the subscription to register, then publish.
	deadline := time.Now().Add(2 * time.Second)
	for hub.Subscribers() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	hub.Publish(events.Event{Type: "job_start", Slot: "A"})

	lines := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "event: ") {
				lines <- sc.Text()
				return
			}
		}
	}()

	select {
	case l := <-lines:
		if !strings.Contains(l, "job_start") {
			t.Fatalf("first event = %q", l)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no SSE event received")
	}
}
