// Package web serves the cardingest HTTP API and the embedded UI. REST handlers
// and request/response types are generated from api/openapi.yaml (see
// generate.go); this file implements the generated StrictServerInterface and
// composes the router (generated REST + hand-written SSE + embedded UI).
package web

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/michaelpeterswa/cardingest/internal/app"
	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/michaelpeterswa/cardingest/internal/config"
	"github.com/michaelpeterswa/cardingest/internal/detect"
	"github.com/michaelpeterswa/cardingest/internal/events"
	"github.com/michaelpeterswa/cardingest/internal/rules"
	"github.com/michaelpeterswa/cardingest/internal/store"
	"github.com/michaelpeterswa/cardingest/internal/web/api"
)

// MockController lets the dev endpoints drive the mock reader. In real mode it
// is nil and the dev endpoints return 501. *detect.MockDetector satisfies it.
type MockController interface {
	Insert(slot card.Slot, dir string) error
	Remove(slot card.Slot) error
}

// JobsProvider supplies job history and stats. *store.SQLite satisfies it.
type JobsProvider interface {
	ListJobs(ctx context.Context, limit int) ([]store.Job, error)
	Stats(ctx context.Context) (store.Stats, error)
}

// StatusProvider supplies the live per-slot state. *app.App satisfies it.
type StatusProvider interface {
	Status() []app.SlotState
}

// Deps are the web server's collaborators. Any may be nil (the corresponding
// endpoints then return empty results or 501/500).
type Deps struct {
	Log    *slog.Logger
	Mock   MockController
	Jobs   JobsProvider
	Config *config.Store
	Status StatusProvider
	Events *events.Hub
}

// Server implements api.StrictServerInterface.
type Server struct {
	deps Deps
}

func NewServer(d Deps) *Server { return &Server{deps: d} }

// Handler builds the routed http.Handler: hand-written SSE, generated REST under
// /api/v1, and the embedded UI at the root.
func (s *Server) Handler() http.Handler {
	strict := api.NewStrictHandler(s, nil)
	apiHandler := api.HandlerWithOptions(strict, api.StdHTTPServerOptions{})

	mux := http.NewServeMux()
	mux.Handle("/api/v1/events", s.sseHandler()) // more specific than /api/v1/
	mux.Handle("/api/v1/", apiHandler)
	mux.Handle("/", s.uiHandler())
	return mux
}

// --- generated StrictServerInterface implementation ---

func (s *Server) GetHealthz(_ context.Context, _ api.GetHealthzRequestObject) (api.GetHealthzResponseObject, error) {
	return api.GetHealthz200JSONResponse{Status: "ok"}, nil
}

func (s *Server) GetStatus(_ context.Context, _ api.GetStatusRequestObject) (api.GetStatusResponseObject, error) {
	out := api.GetStatus200JSONResponse{}
	if s.deps.Status != nil {
		for _, st := range s.deps.Status.Status() {
			out = append(out, toAPISlot(st))
		}
	}
	return out, nil
}

func (s *Server) GetJobs(ctx context.Context, req api.GetJobsRequestObject) (api.GetJobsResponseObject, error) {
	limit := 50
	if req.Params.Limit != nil {
		limit = *req.Params.Limit
	}
	out := api.GetJobs200JSONResponse{}
	if s.deps.Jobs == nil {
		return out, nil
	}
	jobs, err := s.deps.Jobs.ListJobs(ctx, limit)
	if err != nil {
		return nil, err
	}
	for _, j := range jobs {
		out = append(out, toAPIJob(j))
	}
	return out, nil
}

func (s *Server) GetStats(ctx context.Context, _ api.GetStatsRequestObject) (api.GetStatsResponseObject, error) {
	if s.deps.Jobs == nil {
		return api.GetStats200JSONResponse{}, nil
	}
	st, err := s.deps.Jobs.Stats(ctx)
	if err != nil {
		return nil, err
	}
	return api.GetStats200JSONResponse(toAPIStats(st)), nil
}

func (s *Server) GetConfig(_ context.Context, _ api.GetConfigRequestObject) (api.GetConfigResponseObject, error) {
	if s.deps.Config == nil {
		return api.GetConfig200JSONResponse{Yaml: ""}, nil
	}
	data, err := s.deps.Config.Marshal()
	if err != nil {
		return nil, err
	}
	return api.GetConfig200JSONResponse{Yaml: string(data)}, nil
}

func (s *Server) PutConfig(_ context.Context, req api.PutConfigRequestObject) (api.PutConfigResponseObject, error) {
	if s.deps.Config == nil || req.Body == nil {
		return api.PutConfig400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{Error: "config editing unavailable"}}, nil
	}
	f, err := config.ParseFile([]byte(req.Body.Yaml))
	if err != nil {
		return api.PutConfig400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{Error: err.Error()}}, nil
	}
	// Validate the rules compile before persisting.
	if _, err := rules.Compile(f.Rules); err != nil {
		return api.PutConfig400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{Error: err.Error()}}, nil
	}
	if err := s.deps.Config.Save(f); err != nil {
		return nil, err
	}
	data, _ := s.deps.Config.Marshal()
	return api.PutConfig200JSONResponse{Yaml: string(data)}, nil
}

func (s *Server) DevInsert(_ context.Context, req api.DevInsertRequestObject) (api.DevInsertResponseObject, error) {
	if s.deps.Mock == nil {
		return api.DevInsert501JSONResponse{Error: "dev endpoints require READER_MODE=mock"}, nil
	}
	if req.Body == nil || req.Body.Slot == "" {
		return api.DevInsert400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{Error: "slot is required"}}, nil
	}
	dir := ""
	if req.Body.Dir != nil {
		dir = *req.Body.Dir
	}
	err := s.deps.Mock.Insert(card.Slot(req.Body.Slot), dir)
	switch {
	case err == nil:
		return api.DevInsert202JSONResponse{Status: "accepted", Slot: req.Body.Slot}, nil
	case errors.Is(err, detect.ErrSlotOccupied):
		return api.DevInsert409JSONResponse{Error: err.Error()}, nil
	default:
		return api.DevInsert400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{Error: err.Error()}}, nil
	}
}

func (s *Server) DevRemove(_ context.Context, req api.DevRemoveRequestObject) (api.DevRemoveResponseObject, error) {
	if s.deps.Mock == nil {
		return api.DevRemove501JSONResponse{Error: "dev endpoints require READER_MODE=mock"}, nil
	}
	if req.Body == nil || req.Body.Slot == "" {
		return api.DevRemove400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{Error: "slot is required"}}, nil
	}
	err := s.deps.Mock.Remove(card.Slot(req.Body.Slot))
	switch {
	case err == nil:
		return api.DevRemove202JSONResponse{Status: "accepted", Slot: req.Body.Slot}, nil
	case errors.Is(err, detect.ErrSlotEmpty):
		return api.DevRemove404JSONResponse{Error: err.Error()}, nil
	default:
		return api.DevRemove400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{Error: err.Error()}}, nil
	}
}

// --- store/app -> generated type mapping ---

func toAPIJob(j store.Job) api.Job {
	aj := api.Job{
		Id:        j.ID,
		Slot:      j.Slot,
		Serial:    j.Serial,
		Status:    string(j.Status),
		StartedAt: j.StartedAt,
		Copied:    &j.Copied,
		Deduped:   &j.Deduped,
		Skipped:   &j.Skipped,
		Erased:    &j.Erased,
		Bytes:     &j.Bytes,
	}
	if j.Err != "" {
		aj.Error = &j.Err
	}
	if j.FinishedAt != nil {
		aj.FinishedAt = j.FinishedAt
	}
	return aj
}

func toAPIStats(st store.Stats) api.Stats {
	return api.Stats{
		TotalJobs:    st.TotalJobs,
		CompleteJobs: st.CompleteJobs,
		ErrorJobs:    st.ErrorJobs,
		FilesCopied:  st.FilesCopied,
		Bytes:        st.Bytes,
	}
}

func toAPISlot(s app.SlotState) api.SlotState {
	as := api.SlotState{Slot: s.Slot, State: s.State}
	if s.JobID != 0 {
		id := s.JobID
		as.JobId = &id
	}
	return as
}
