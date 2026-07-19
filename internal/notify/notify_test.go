package notify

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/config"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPulsarNotifierPostsCanonicalPayload(t *testing.T) {
	t.Setenv("NOTIFY_PULSAR_TOKEN", "secret-token")

	var (
		gotAuth        string
		gotContentType string
		gotPath        string
		gotBody        payload
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"data":{"notificationId":"x"},"meta":{}}`))
	}))
	defer srv.Close()

	n, err := newPulsar(config.Notifier{
		Type: "pulsar", URL: srv.URL,
		UserKey: "uQiRzpo4DXghDmr9QzzfQu27cmVRsG", Priority: 1,
	})
	if err != nil {
		t.Fatalf("newPulsar: %v", err)
	}

	err = n.Notify(context.Background(), Notification{
		Event: EventComplete, Slot: "A",
		Copied: 3, Skipped: 1, Erased: 3, BytesCopied: 2048, Duration: 1500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}

	if gotPath != "/notifications" {
		t.Errorf("path = %q, want /notifications", gotPath)
	}
	if gotAuth != "Bearer secret-token" {
		t.Errorf("auth = %q", gotAuth)
	}
	if gotContentType != "application/json" {
		t.Errorf("content-type = %q", gotContentType)
	}
	if gotBody.Pushover.UserOrGroupKey != "uQiRzpo4DXghDmr9QzzfQu27cmVRsG" {
		t.Errorf("userOrGroupKey = %q", gotBody.Pushover.UserOrGroupKey)
	}
	if gotBody.Content.Priority != 1 {
		t.Errorf("priority = %d, want 1", gotBody.Content.Priority)
	}
	if !strings.Contains(gotBody.Content.Title, "slot A complete") {
		t.Errorf("title = %q", gotBody.Content.Title)
	}
	if !strings.Contains(gotBody.Content.Body, "copied 3") || !strings.Contains(gotBody.Content.Body, "2.0 KiB") {
		t.Errorf("body = %q", gotBody.Content.Body)
	}
	if gotBody.Content.Data["copied"] != "3" {
		t.Errorf("data.copied = %q", gotBody.Content.Data["copied"])
	}
}

func TestPulsarNotifierSurfacesErrorStatus(t *testing.T) {
	t.Setenv("NOTIFY_PULSAR_TOKEN", "tok")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"type":"about:blank","title":"unauthorized"}`))
	}))
	defer srv.Close()

	n, err := newPulsar(config.Notifier{Type: "pulsar", URL: srv.URL, UserKey: "k"})
	if err != nil {
		t.Fatalf("newPulsar: %v", err)
	}
	if err := n.Notify(context.Background(), Notification{Event: EventError, Slot: "A", Err: "boom"}); err == nil {
		t.Fatal("expected error on 401")
	}
}

func TestNewPulsarRequiresTokenAndFields(t *testing.T) {
	// Missing token.
	t.Setenv("NOTIFY_PULSAR_TOKEN", "")
	if _, err := newPulsar(config.Notifier{URL: "http://x", UserKey: "k"}); err == nil {
		t.Error("expected error when token env empty")
	}
	// Missing url.
	t.Setenv("NOTIFY_PULSAR_TOKEN", "t")
	if _, err := newPulsar(config.Notifier{UserKey: "k"}); err == nil {
		t.Error("expected error when url missing")
	}
	// Missing user key.
	if _, err := newPulsar(config.Notifier{URL: "http://x"}); err == nil {
		t.Error("expected error when user_key missing")
	}
}

// countingNotifier records how many notifications it received.
type countingNotifier struct{ events []EventType }

func (c *countingNotifier) Notify(_ context.Context, n Notification) error {
	c.events = append(c.events, n.Event)
	return nil
}

func TestEventFilter(t *testing.T) {
	c := &countingNotifier{}
	f := withEvents(c, []string{"complete", "error"})

	_ = f.Notify(context.Background(), Notification{Event: EventStart})
	_ = f.Notify(context.Background(), Notification{Event: EventComplete})
	_ = f.Notify(context.Background(), Notification{Event: EventError})

	if len(c.events) != 2 {
		t.Fatalf("received %v, want [complete error]", c.events)
	}
}

func TestMultiFanOut(t *testing.T) {
	a, b := &countingNotifier{}, &countingNotifier{}
	m := Multi{a, b}
	if err := m.Notify(context.Background(), Notification{Event: EventComplete}); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if len(a.events) != 1 || len(b.events) != 1 {
		t.Fatalf("fan-out failed: a=%v b=%v", a.events, b.events)
	}
}

func TestBuildEmptyIsLogger(t *testing.T) {
	n, err := Build(nil, testLogger())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if _, ok := n.(Logger); !ok {
		t.Fatalf("empty config should build Logger, got %T", n)
	}
}

func TestBuildUnknownTypeErrorsButKeepsGood(t *testing.T) {
	t.Setenv("NOTIFY_PULSAR_TOKEN", "t")
	n, err := Build([]config.Notifier{
		{Type: "smoke-signals"},
		{Type: "pulsar", URL: "http://x", UserKey: "k", On: []string{"error"}},
	}, testLogger())
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
	if _, ok := n.(Multi); !ok {
		t.Fatalf("should still return the good notifier as Multi, got %T", n)
	}
}
