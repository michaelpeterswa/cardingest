package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// JobStatus is the lifecycle state of an ingest job.
type JobStatus string

const (
	JobRunning  JobStatus = "running"
	JobComplete JobStatus = "complete"
	JobError    JobStatus = "error"
)

// Job is one ingest run recorded for history and stats.
type Job struct {
	ID         int64      `json:"id"`
	Slot       string     `json:"slot"`
	Serial     string     `json:"serial"`
	Status     JobStatus  `json:"status"`
	Copied     int        `json:"copied"`
	Deduped    int        `json:"deduped"`
	Skipped    int        `json:"skipped"`
	Erased     int        `json:"erased"`
	Bytes      int64      `json:"bytes"`
	Err        string     `json:"error,omitempty"`
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
}

// Stats is an aggregate summary across all recorded jobs.
type Stats struct {
	TotalJobs    int   `json:"totalJobs"`
	CompleteJobs int   `json:"completeJobs"`
	ErrorJobs    int   `json:"errorJobs"`
	FilesCopied  int   `json:"filesCopied"`
	Bytes        int64 `json:"bytes"`
}

// StartJob records a new running job and returns its id.
func (s *SQLite) StartJob(ctx context.Context, slot, serial string) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO jobs (slot, serial, status) VALUES (?, ?, ?)`,
		slot, serial, JobRunning)
	if err != nil {
		return 0, fmt.Errorf("start job: %w", err)
	}
	return res.LastInsertId()
}

// FinishJob updates a job with its terminal status and counts.
func (s *SQLite) FinishJob(ctx context.Context, id int64, status JobStatus, j Job) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs
		   SET status=?, copied=?, deduped=?, skipped=?, erased=?, bytes=?, err=?,
		       finished_at=strftime('%s','now')
		 WHERE id=?`,
		status, j.Copied, j.Deduped, j.Skipped, j.Erased, j.Bytes, j.Err, id)
	if err != nil {
		return fmt.Errorf("finish job: %w", err)
	}
	return nil
}

// ListJobs returns the most recent jobs, newest first.
func (s *SQLite) ListJobs(ctx context.Context, limit int) ([]Job, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, slot, serial, status, copied, deduped, skipped, erased, bytes,
		       err, started_at, finished_at
		  FROM jobs ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var jobs []Job
	for rows.Next() {
		var (
			j          Job
			startedAt  int64
			finishedAt sql.NullInt64
		)
		if err := rows.Scan(&j.ID, &j.Slot, &j.Serial, &j.Status, &j.Copied, &j.Deduped,
			&j.Skipped, &j.Erased, &j.Bytes, &j.Err, &startedAt, &finishedAt); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		j.StartedAt = time.Unix(startedAt, 0).UTC()
		if finishedAt.Valid {
			t := time.Unix(finishedAt.Int64, 0).UTC()
			j.FinishedAt = &t
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// Stats aggregates counts across all jobs.
func (s *SQLite) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	err := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COALESCE(SUM(status='complete'), 0),
			COALESCE(SUM(status='error'), 0),
			COALESCE(SUM(copied), 0),
			COALESCE(SUM(bytes), 0)
		FROM jobs`).Scan(&st.TotalJobs, &st.CompleteJobs, &st.ErrorJobs, &st.FilesCopied, &st.Bytes)
	if err != nil {
		return st, fmt.Errorf("stats: %w", err)
	}
	return st, nil
}
