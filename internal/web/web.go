// Package web serves the cardingest HTTP API. Handlers and request/response
// types are generated from api/openapi.yaml (see generate.go); this file
// implements the generated StrictServerInterface and builds the http.Handler.
package web

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/michaelpeterswa/cardingest/internal/detect"
	"github.com/michaelpeterswa/cardingest/internal/web/api"
)

// MockController lets the dev endpoints drive the mock reader. In real mode it
// is nil and the dev endpoints return 501. *detect.MockDetector satisfies it.
type MockController interface {
	Insert(slot card.Slot, dir string) error
	Remove(slot card.Slot) error
}

// Server implements api.StrictServerInterface.
type Server struct {
	log  *slog.Logger
	mock MockController // nil in real mode
}

// NewServer builds the API server. Pass a non-nil mock only in mock mode.
func NewServer(log *slog.Logger, mock MockController) *Server {
	return &Server{log: log, mock: mock}
}

// Handler returns the routed http.Handler for the API.
func (s *Server) Handler() http.Handler {
	strict := api.NewStrictHandler(s, nil)
	return api.HandlerWithOptions(strict, api.StdHTTPServerOptions{})
}

// GetHealthz implements the liveness probe.
func (s *Server) GetHealthz(_ context.Context, _ api.GetHealthzRequestObject) (api.GetHealthzResponseObject, error) {
	return api.GetHealthz200JSONResponse{Status: "ok"}, nil
}

// DevInsert simulates a card insertion in mock mode.
func (s *Server) DevInsert(_ context.Context, req api.DevInsertRequestObject) (api.DevInsertResponseObject, error) {
	if s.mock == nil {
		return api.DevInsert501JSONResponse{Error: "dev endpoints require READER_MODE=mock"}, nil
	}
	if req.Body == nil || req.Body.Slot == "" {
		return api.DevInsert400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{Error: "slot is required"}}, nil
	}

	dir := ""
	if req.Body.Dir != nil {
		dir = *req.Body.Dir
	}

	err := s.mock.Insert(card.Slot(req.Body.Slot), dir)
	switch {
	case err == nil:
		return api.DevInsert202JSONResponse{Status: "accepted", Slot: req.Body.Slot}, nil
	case errors.Is(err, detect.ErrSlotOccupied):
		return api.DevInsert409JSONResponse{Error: err.Error()}, nil
	default: // ErrNoFixtureDir and any other bad input
		return api.DevInsert400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{Error: err.Error()}}, nil
	}
}

// DevRemove simulates a card removal in mock mode.
func (s *Server) DevRemove(_ context.Context, req api.DevRemoveRequestObject) (api.DevRemoveResponseObject, error) {
	if s.mock == nil {
		return api.DevRemove501JSONResponse{Error: "dev endpoints require READER_MODE=mock"}, nil
	}
	if req.Body == nil || req.Body.Slot == "" {
		return api.DevRemove400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{Error: "slot is required"}}, nil
	}

	err := s.mock.Remove(card.Slot(req.Body.Slot))
	switch {
	case err == nil:
		return api.DevRemove202JSONResponse{Status: "accepted", Slot: req.Body.Slot}, nil
	case errors.Is(err, detect.ErrSlotEmpty):
		return api.DevRemove404JSONResponse{Error: err.Error()}, nil
	default:
		return api.DevRemove400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{Error: err.Error()}}, nil
	}
}
