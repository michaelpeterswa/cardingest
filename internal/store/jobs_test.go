package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestJobsLifecycleAndStats(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	// A completed job.
	id, err := s.StartJob(ctx, "A", "card1")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := s.FinishJob(ctx, id, JobComplete, Job{Copied: 3, Skipped: 1, Erased: 3, Bytes: 2048}); err != nil {
		t.Fatalf("finish: %v", err)
	}

	// An errored job.
	id2, _ := s.StartJob(ctx, "B", "card2")
	if err := s.FinishJob(ctx, id2, JobError, Job{Err: "boom"}); err != nil {
		t.Fatalf("finish err: %v", err)
	}

	jobs, err := s.ListJobs(ctx, 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("want 2 jobs, got %d", len(jobs))
	}
	// Newest first: the errored job (id2) leads.
	if jobs[0].ID != id2 || jobs[0].Status != JobError || jobs[0].Err != "boom" {
		t.Fatalf("job[0] = %+v", jobs[0])
	}
	if jobs[1].Status != JobComplete || jobs[1].Copied != 3 || jobs[1].Bytes != 2048 {
		t.Fatalf("job[1] = %+v", jobs[1])
	}
	if jobs[1].FinishedAt == nil {
		t.Fatal("completed job should have finishedAt")
	}

	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.TotalJobs != 2 || st.CompleteJobs != 1 || st.ErrorJobs != 1 || st.FilesCopied != 3 || st.Bytes != 2048 {
		t.Fatalf("stats = %+v", st)
	}
}
