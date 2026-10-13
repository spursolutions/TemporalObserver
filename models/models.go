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
