package web

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/michaelpeterswa/cardingest/internal/detect"
)

// fakeMock is a scriptable MockController.
type fakeMock struct {
	insertErr error
	removeErr error
	inserted  []card.Slot
}

func (f *fakeMock) Insert(slot card.Slot, _ string) error {
	if f.insertErr == nil {
		f.inserted = append(f.inserted, slot)
	}
	return f.insertErr
}
func (f *fakeMock) Remove(card.Slot) error { return f.removeErr }

func newTestServer(mock MockController) http.Handler {
	return NewServer(slog.New(slog.NewTextHandler(io.Discard, nil)), mock).Handler()
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	rec := do(t, newTestServer(nil), http.MethodGet, "/api/v1/healthz", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestDevInsertRequiresMockMode(t *testing.T) {
	rec := do(t, newTestServer(nil), http.MethodPost, "/api/v1/dev/insert", `{"slot":"A"}`)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rec.Code)
	}
}

func TestDevInsertAccepted(t *testing.T) {
	mock := &fakeMock{}
	rec := do(t, newTestServer(mock), http.MethodPost, "/api/v1/dev/insert", `{"slot":"A","dir":"/tmp/card"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	if len(mock.inserted) != 1 || mock.inserted[0] != card.SlotA {
		t.Fatalf("inserted = %v", mock.inserted)
	}
}

func TestDevInsertConflict(t *testing.T) {
	mock := &fakeMock{insertErr: detect.ErrSlotOccupied}
	rec := do(t, newTestServer(mock), http.MethodPost, "/api/v1/dev/insert", `{"slot":"A"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
}

func TestDevInsertMissingSlot(t *testing.T) {
	rec := do(t, newTestServer(&fakeMock{}), http.MethodPost, "/api/v1/dev/insert", `{}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestDevRemoveNotFound(t *testing.T) {
	mock := &fakeMock{removeErr: detect.ErrSlotEmpty}
	rec := do(t, newTestServer(mock), http.MethodPost, "/api/v1/dev/remove", `{"slot":"A"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
