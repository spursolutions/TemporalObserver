package models

import "time"

// WorkflowExecution is a summary row derived entirely from Temporal's Visibility API and
// history events - namespace/IDs/type/status/timing only. It never carries workflow input
// or output payloads; see Phase 2 (allowlisted field extraction) for anything payload-derived.
type WorkflowExecution struct {
	ID               int64      `json:"id"`
	Namespace        string     `json:"namespace"`
	WorkflowID       string     `json:"workflow_id"`
	RunID            string     `json:"run_id"`
	WorkflowType     string     `json:"workflow_type"`
	TaskQueue        string     `json:"task_queue"`
	Status           string     `json:"status"`
	StartTime        time.Time  `json:"start_time"`
	CloseTime        *time.Time `json:"close_time,omitempty"`
	DurationMs       *int64     `json:"duration_ms,omitempty"`
	HistoryFetchedAt *time.Time `json:"-"`
}

// ActivityExecution is one activity attempt extracted from a workflow's history.
type ActivityExecution struct {
	ID             int64      `json:"id"`
	Namespace      string     `json:"namespace"`
	WorkflowID     string     `json:"workflow_id"`
	RunID          string     `json:"run_id"`
	ActivityType   string     `json:"activity_type"`
	ActivityID     string     `json:"activity_id"`
	Attempt        int32      `json:"attempt"`
	Status         string     `json:"status"`
	ScheduledTime  *time.Time `json:"scheduled_time,omitempty"`
	StartTime      *time.Time `json:"start_time,omitempty"`
	CloseTime      *time.Time `json:"close_time,omitempty"`
	DurationMs     *int64     `json:"duration_ms,omitempty"`
	FailureType    string     `json:"failure_type,omitempty"`
	FailureMessage string     `json:"failure_message,omitempty"`
}

// DurationBaseline is a rolling p50/p95 duration computed from historical data, scoped to a
// workflow type (ActivityType == "") or to one activity type within it.
type DurationBaseline struct {
	Namespace    string    `json:"namespace"`
	WorkflowType string    `json:"workflow_type"`
	ActivityType string    `json:"activity_type,omitempty"`
	SampleCount  int64     `json:"sample_count"`
	P50Ms        *int64    `json:"p50_ms,omitempty"`
	P95Ms        *int64    `json:"p95_ms,omitempty"`
	ComputedAt   time.Time `json:"computed_at"`
}

// Threshold is an optional operator override. A nil field falls back to the computed baseline.
type Threshold struct {
	Namespace          string   `json:"namespace"`
	WorkflowType       string   `json:"workflow_type"`
	ActivityType       string   `json:"activity_type,omitempty"`
	MaxDurationMs      *int64   `json:"max_duration_ms,omitempty"`
	BaselineMultiplier *float64 `json:"baseline_multiplier,omitempty"`
}

// Alert is a single flagged condition - a stuck/slow workflow or activity, a repeated-retry
// activity, or an elevated failure rate for a workflow type.
type Alert struct {
	ID           int64      `json:"id"`
	Namespace    string     `json:"namespace"`
	WorkflowID   string     `json:"workflow_id"`
	RunID        string     `json:"run_id"`
	WorkflowType string     `json:"workflow_type"`
	ActivityType string     `json:"activity_type,omitempty"`
	ActivityID   string     `json:"activity_id,omitempty"`
	AlertType    string     `json:"alert_type"`
	Severity     string     `json:"severity"`
	Message      string     `json:"message"`
	CreatedAt    time.Time  `json:"created_at"`
	ResolvedAt   *time.Time `json:"resolved_at,omitempty"`
}

// NamespaceOverview aggregates workflow_executions by namespace + workflow type for the
// Overview/Namespace dashboard views.
type NamespaceOverview struct {
	Namespace     string   `json:"namespace"`
	WorkflowType  string   `json:"workflow_type"`
	Total         int64    `json:"total"`
	Running       int64    `json:"running"`
	Completed     int64    `json:"completed"`
	Failed        int64    `json:"failed"`
	Canceled      int64    `json:"canceled"`
	TimedOut      int64    `json:"timed_out"`
	Terminated    int64    `json:"terminated"`
	AvgDurationMs *float64 `json:"avg_duration_ms,omitempty"`
	MaxDurationMs *int64   `json:"max_duration_ms,omitempty"`
}
