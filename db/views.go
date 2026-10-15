package db

import (
	"context"
	"encoding/json"

	"TemporalObserver/models"
)

// ListSlowestWorkflows returns the longest-duration closed executions, for the Overview /
// Namespace "slowest workflows" panels.
func ListSlowestWorkflows(ctx context.Context, namespace string, limit int) ([]models.WorkflowExecution, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, namespace, workflow_id, run_id, workflow_type, task_queue, status, start_time, close_time, duration_ms
		FROM workflow_executions
		WHERE ($1 = '' OR namespace = $1) AND duration_ms IS NOT NULL
		ORDER BY duration_ms DESC
		LIMIT $2
	`, namespace, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWorkflowExecutions(rows)
}

// ListRecentFailures returns the most recently failed/timed-out/terminated executions.
func ListRecentFailures(ctx context.Context, namespace string, limit int) ([]models.WorkflowExecution, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, namespace, workflow_id, run_id, workflow_type, task_queue, status, start_time, close_time, duration_ms
		FROM workflow_executions
		WHERE ($1 = '' OR namespace = $1) AND status IN ('Failed', 'TimedOut', 'Terminated')
		ORDER BY close_time DESC NULLS LAST
		LIMIT $2
	`, namespace, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWorkflowExecutions(rows)
}

// GetWorkflowExecution returns one execution plus its allowlisted search-index fields, for the
// Workflow Detail drill-down view. The returned map is the ONLY payload-derived data ever shown -
// never the raw input/output.
func GetWorkflowExecution(ctx context.Context, namespace, workflowID, runID string) (*models.WorkflowExecution, map[string]string, error) {
	var w models.WorkflowExecution
	var searchIndexJSON []byte
	err := pool.QueryRow(ctx, `
		SELECT id, namespace, workflow_id, run_id, workflow_type, task_queue, status, start_time, close_time, duration_ms, search_index
		FROM workflow_executions
		WHERE namespace = $1 AND workflow_id = $2 AND run_id = $3
	`, namespace, workflowID, runID).Scan(&w.ID, &w.Namespace, &w.WorkflowID, &w.RunID, &w.WorkflowType,
		&w.TaskQueue, &w.Status, &w.StartTime, &w.CloseTime, &w.DurationMs, &searchIndexJSON)
	if err != nil {
		return nil, nil, err
	}

	fields := map[string]string{}
	if len(searchIndexJSON) > 0 {
		_ = json.Unmarshal(searchIndexJSON, &fields)
	}
	return &w, fields, nil
}
