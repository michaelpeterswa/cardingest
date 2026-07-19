package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// sseHandler streams live ingest events to the browser as Server-Sent Events.
// This endpoint is intentionally hand-written (not generated) because OpenAPI
// does not model SSE; it is mounted alongside the generated REST handlers.
func (s *Server) sseHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.deps.Events == nil {
			http.Error(w, "events unavailable", http.StatusServiceUnavailable)
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ch, cancel := s.deps.Events.Subscribe()
		defer cancel()

		if _, err := fmt.Fprint(w, ": connected\n\n"); err != nil {
			return
		}
		flusher.Flush()

		ping := time.NewTicker(15 * time.Second)
		defer ping.Stop()

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ping.C:
				if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
					return // client disconnected
				}
				flusher.Flush()
			case ev, ok := <-ch:
				if !ok {
					return
				}
				b, err := json.Marshal(ev)
				if err != nil {
					continue
				}
				if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, b); err != nil {
					return // client disconnected
				}
				flusher.Flush()
			}
		}
	})
}
