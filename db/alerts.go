package db

import (
	"context"

	"TemporalObserver/models"
)

const aggregateAlertWorkflowID = "__aggregate__"

// UpsertAlert raises an alert, or re-opens a previously resolved one with the same identity.
// Severity and message always refresh to the latest evaluation, even while the alert stays
// continuously active - only created_at (the "since when" timestamp) is preserved across
// updates and only reset when reopening a previously-resolved alert.
func UpsertAlert(ctx context.Context, a models.Alert) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO alerts (namespace, workflow_id, run_id, workflow_type, activity_type, activity_id, alert_type, severity, message, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
		ON CONFLICT (namespace, workflow_id, run_id, workflow_type, activity_type, activity_id, alert_type) DO UPDATE SET
			resolved_at = NULL,
			severity    = EXCLUDED.severity,
			message     = EXCLUDED.message,
			created_at  = CASE WHEN alerts.resolved_at IS NOT NULL THEN NOW() ELSE alerts.created_at END
	`, a.Namespace, a.WorkflowID, a.RunID, a.WorkflowType, a.ActivityType, a.ActivityID, a.AlertType, a.Severity, a.Message)
	return err
}

// ResolveAlertsForExecution closes the "stuck_workflow" alert for one execution once it leaves
// the Running state - that alert type's precondition (still running) is no longer true. Other
// alert types (slow_workflow, slow_activity, repeated_retry) describe something that already
// happened and are deliberately NOT touched here: this function can be called again for the
// same execution (e.g. from the closed-cursor's small overlap window), and resolving every
// alert type unconditionally would wipe out a slow_workflow/slow_activity alert that the
// independent alert-evaluation loop legitimately raised in between calls.
func ResolveAlertsForExecution(ctx context.Context, namespace, workflowID, runID string) error {
	_, err := pool.Exec(ctx, `
		UPDATE alerts SET resolved_at = NOW()
		WHERE namespace = $1 AND workflow_id = $2 AND run_id = $3 AND alert_type = 'stuck_workflow' AND resolved_at IS NULL
	`, namespace, workflowID, runID)
	return err
}

// ResolveAggregateAlert closes a namespace/workflow-type-level alert (e.g. failure rate) once
// the condition that raised it is no longer true.
func ResolveAggregateAlert(ctx context.Context, namespace, workflowType, alertType string) error {
	_, err := pool.Exec(ctx, `
		UPDATE alerts SET resolved_at = NOW()
		WHERE namespace = $1 AND workflow_type = $2 AND workflow_id = $3 AND alert_type = $4 AND resolved_at IS NULL
	`, namespace, workflowType, aggregateAlertWorkflowID, alertType)
	return err
}

func ListActiveAlerts(ctx context.Context, namespace string) ([]models.Alert, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, namespace, workflow_id, run_id, workflow_type, activity_type, activity_id, alert_type, severity, message, created_at, resolved_at
		FROM alerts
		WHERE resolved_at IS NULL AND ($1 = '' OR namespace = $1)
		ORDER BY created_at DESC
	`, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []models.Alert
	for rows.Next() {
		var a models.Alert
		if err := rows.Scan(&a.ID, &a.Namespace, &a.WorkflowID, &a.RunID, &a.WorkflowType, &a.ActivityType, &a.ActivityID,
			&a.AlertType, &a.Severity, &a.Message, &a.CreatedAt, &a.ResolvedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

// CountActiveAlertsBySeverity powers the Prometheus gauge - one row per (namespace, workflow_type, severity).
func CountActiveAlertsBySeverity(ctx context.Context) (map[[3]string]int, error) {
	rows, err := pool.Query(ctx, `
		SELECT namespace, workflow_type, severity, COUNT(*)
		FROM alerts
		WHERE resolved_at IS NULL
		GROUP BY namespace, workflow_type, severity
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[[3]string]int{}
	for rows.Next() {
		var ns, wt, sev string
		var count int
		if err := rows.Scan(&ns, &wt, &sev, &count); err != nil {
			return nil, err
		}
		out[[3]string{ns, wt, sev}] = count
	}
	return out, nil
}

// GetRecentlyClosedForAlerting returns executions closed within the lookback window, for the
// slow-workflow check (evaluated once per execution thanks to alert dedupe).
func GetRecentlyClosedForAlerting(ctx context.Context, namespace string, lookbackSeconds int) ([]models.WorkflowExecution, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, namespace, workflow_id, run_id, workflow_type, task_queue, status, start_time, close_time, duration_ms
		FROM workflow_executions
		WHERE namespace = $1 AND status <> 'Running'
		  AND close_time >= NOW() - ($2 * INTERVAL '1 second')
	`, namespace, lookbackSeconds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWorkflowExecutions(rows)
}

// ActivityAlertRow is an activity attempt joined with its parent workflow's type, since
// thresholds/baselines are scoped by workflow type + activity type together.
type ActivityAlertRow struct {
	models.ActivityExecution
	WorkflowType string
}

// GetRecentActivitiesForAlerting returns activity attempts created within the lookback window,
// for the slow-activity and repeated-retry checks.
func GetRecentActivitiesForAlerting(ctx context.Context, namespace string, lookbackSeconds int) ([]ActivityAlertRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT a.id, a.namespace, a.workflow_id, a.run_id, a.activity_type, a.activity_id, a.attempt, a.status,
		       a.scheduled_time, a.start_time, a.close_time, a.duration_ms, a.failure_type, a.failure_message,
		       w.workflow_type
		FROM activity_executions a
		JOIN workflow_executions w
		  ON w.namespace = a.namespace AND w.workflow_id = a.workflow_id AND w.run_id = a.run_id
		WHERE a.namespace = $1 AND a.created_at >= NOW() - ($2 * INTERVAL '1 second')
	`, namespace, lookbackSeconds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ActivityAlertRow
	for rows.Next() {
		var a ActivityAlertRow
		if err := rows.Scan(&a.ID, &a.Namespace, &a.WorkflowID, &a.RunID, &a.ActivityType, &a.ActivityID,
			&a.Attempt, &a.Status, &a.ScheduledTime, &a.StartTime, &a.CloseTime, &a.DurationMs,
			&a.FailureType, &a.FailureMessage, &a.WorkflowType); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

// GetFailureRate returns (failed, total) closed executions per workflow type within the window.
func GetFailureRate(ctx context.Context, namespace string, lookbackSeconds int) (map[string][2]int64, error) {
	rows, err := pool.Query(ctx, `
		SELECT workflow_type,
		       COUNT(*) FILTER (WHERE status = 'Failed'),
		       COUNT(*)
		FROM workflow_executions
		WHERE namespace = $1 AND status <> 'Running'
		  AND close_time >= NOW() - ($2 * INTERVAL '1 second')
		GROUP BY workflow_type
	`, namespace, lookbackSeconds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string][2]int64{}
	for rows.Next() {
		var wt string
		var failed, total int64
		if err := rows.Scan(&wt, &failed, &total); err != nil {
			return nil, err
		}
		out[wt] = [2]int64{failed, total}
	}
	return out, nil
}

func AggregateAlertWorkflowID() string {
	return aggregateAlertWorkflowID
}
