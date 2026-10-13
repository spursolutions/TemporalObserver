package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"TemporalObserver/models"
)

var pool *pgxpool.Pool

const schema = `
CREATE TABLE IF NOT EXISTS workflow_executions (
	id                 BIGSERIAL PRIMARY KEY,
	namespace          TEXT NOT NULL,
	workflow_id        TEXT NOT NULL,
	run_id             TEXT NOT NULL,
	workflow_type      TEXT NOT NULL,
	task_queue         TEXT NOT NULL DEFAULT '',
	status             TEXT NOT NULL,
	start_time         TIMESTAMPTZ NOT NULL,
	close_time         TIMESTAMPTZ,
	duration_ms        BIGINT,
	history_fetched_at TIMESTAMPTZ,
	updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	UNIQUE (namespace, workflow_id, run_id)
);
CREATE INDEX IF NOT EXISTS idx_workflow_executions_ns_type ON workflow_executions (namespace, workflow_type);
CREATE INDEX IF NOT EXISTS idx_workflow_executions_status ON workflow_executions (namespace, status);
CREATE INDEX IF NOT EXISTS idx_workflow_executions_start_time ON workflow_executions (start_time);
CREATE INDEX IF NOT EXISTS idx_workflow_executions_pending_history
	ON workflow_executions (namespace)
	WHERE status <> 'Running' AND history_fetched_at IS NULL;

CREATE TABLE IF NOT EXISTS activity_executions (
	id               BIGSERIAL PRIMARY KEY,
	namespace        TEXT NOT NULL,
	workflow_id      TEXT NOT NULL,
	run_id           TEXT NOT NULL,
	activity_type    TEXT NOT NULL,
	activity_id      TEXT NOT NULL,
	attempt          INT NOT NULL DEFAULT 1,
	status           TEXT NOT NULL,
	scheduled_time   TIMESTAMPTZ,
	start_time       TIMESTAMPTZ,
	close_time       TIMESTAMPTZ,
	duration_ms      BIGINT,
	failure_type     TEXT NOT NULL DEFAULT '',
	failure_message  TEXT NOT NULL DEFAULT '',
	created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	UNIQUE (namespace, workflow_id, run_id, activity_id, attempt)
);
CREATE INDEX IF NOT EXISTS idx_activity_executions_wf ON activity_executions (namespace, workflow_id, run_id);
CREATE INDEX IF NOT EXISTS idx_activity_executions_type ON activity_executions (namespace, activity_type);

CREATE TABLE IF NOT EXISTS collector_cursors (
	namespace       TEXT PRIMARY KEY,
	closed_cursor   TIMESTAMPTZ NOT NULL,
	updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

func InitDB(databaseURL string) error {
	var err error
	pool, err = pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("error ensuring schema exists: %w", err)
	}

	log.Println("TemporalObserver database schema ready")
	return nil
}

func GetDB() *pgxpool.Pool {
	if pool == nil {
		log.Fatal("database not initialized. Call InitDB first.")
	}
	return pool
}

func CloseDB() {
	if pool != nil {
		pool.Close()
	}
}

// UpsertWorkflowExecution inserts or refreshes a summary row keyed by (namespace, workflow_id, run_id).
func UpsertWorkflowExecution(ctx context.Context, w models.WorkflowExecution) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO workflow_executions
			(namespace, workflow_id, run_id, workflow_type, task_queue, status, start_time, close_time, duration_ms, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
		ON CONFLICT (namespace, workflow_id, run_id) DO UPDATE SET
			workflow_type = EXCLUDED.workflow_type,
			task_queue    = CASE WHEN EXCLUDED.task_queue <> '' THEN EXCLUDED.task_queue ELSE workflow_executions.task_queue END,
			status        = EXCLUDED.status,
			close_time    = COALESCE(EXCLUDED.close_time, workflow_executions.close_time),
			duration_ms   = COALESCE(EXCLUDED.duration_ms, workflow_executions.duration_ms),
			updated_at    = NOW()
	`, w.Namespace, w.WorkflowID, w.RunID, w.WorkflowType, w.TaskQueue, w.Status, w.StartTime, w.CloseTime, w.DurationMs)
	return err
}

// GetRunningExecutions returns rows we currently believe are still running, for the
// reconciliation pass (DescribeWorkflowExecution re-check).
func GetRunningExecutions(ctx context.Context, namespace string, limit int) ([]models.WorkflowExecution, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, namespace, workflow_id, run_id, workflow_type, task_queue, status, start_time, close_time, duration_ms
		FROM workflow_executions
		WHERE namespace = $1 AND status = 'Running'
		ORDER BY updated_at ASC
		LIMIT $2
	`, namespace, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWorkflowExecutions(rows)
}

// GetPendingHistoryFetch returns closed executions whose activity history we haven't pulled yet.
func GetPendingHistoryFetch(ctx context.Context, namespace string, limit int) ([]models.WorkflowExecution, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, namespace, workflow_id, run_id, workflow_type, task_queue, status, start_time, close_time, duration_ms
		FROM workflow_executions
		WHERE namespace = $1 AND status <> 'Running' AND history_fetched_at IS NULL
		ORDER BY close_time ASC NULLS LAST
		LIMIT $2
	`, namespace, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWorkflowExecutions(rows)
}

func MarkHistoryFetched(ctx context.Context, namespace, workflowID, runID string) error {
	_, err := pool.Exec(ctx, `
		UPDATE workflow_executions SET history_fetched_at = NOW()
		WHERE namespace = $1 AND workflow_id = $2 AND run_id = $3
	`, namespace, workflowID, runID)
	return err
}

func UpsertActivityExecution(ctx context.Context, a models.ActivityExecution) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO activity_executions
			(namespace, workflow_id, run_id, activity_type, activity_id, attempt, status,
			 scheduled_time, start_time, close_time, duration_ms, failure_type, failure_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (namespace, workflow_id, run_id, activity_id, attempt) DO UPDATE SET
			status          = EXCLUDED.status,
			start_time      = COALESCE(EXCLUDED.start_time, activity_executions.start_time),
			close_time      = COALESCE(EXCLUDED.close_time, activity_executions.close_time),
			duration_ms     = COALESCE(EXCLUDED.duration_ms, activity_executions.duration_ms),
			failure_type    = EXCLUDED.failure_type,
			failure_message = EXCLUDED.failure_message
	`, a.Namespace, a.WorkflowID, a.RunID, a.ActivityType, a.ActivityID, a.Attempt, a.Status,
		a.ScheduledTime, a.StartTime, a.CloseTime, a.DurationMs, a.FailureType, a.FailureMessage)
	return err
}

// GetClosedCursor returns the StartTimeFilter watermark used to poll ListClosedWorkflowExecutions,
// defaulting to "now minus lookback" the first time a namespace is seen.
func GetClosedCursor(ctx context.Context, namespace string, fallback time.Time) (time.Time, error) {
	var cursor time.Time
	err := pool.QueryRow(ctx, `SELECT closed_cursor FROM collector_cursors WHERE namespace = $1`, namespace).Scan(&cursor)
	if err != nil {
		return fallback, nil
	}
	return cursor, nil
}

func SetClosedCursor(ctx context.Context, namespace string, cursor time.Time) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO collector_cursors (namespace, closed_cursor, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (namespace) DO UPDATE SET closed_cursor = EXCLUDED.closed_cursor, updated_at = NOW()
	`, namespace, cursor)
	return err
}

func scanWorkflowExecutions(rows interface {
	Next() bool
	Scan(dest ...any) error
}) ([]models.WorkflowExecution, error) {
	var out []models.WorkflowExecution
	for rows.Next() {
		var w models.WorkflowExecution
		if err := rows.Scan(&w.ID, &w.Namespace, &w.WorkflowID, &w.RunID, &w.WorkflowType,
			&w.TaskQueue, &w.Status, &w.StartTime, &w.CloseTime, &w.DurationMs); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, nil
}

// ListWorkflowExecutions supports the basic filters standard Temporal visibility already
// gives us for free: namespace, workflow type, status, and a start-time range.
func ListWorkflowExecutions(ctx context.Context, namespace, workflowType, status string, from, to *time.Time, limit, offset int) ([]models.WorkflowExecution, error) {
	query := `
		SELECT id, namespace, workflow_id, run_id, workflow_type, task_queue, status, start_time, close_time, duration_ms
		FROM workflow_executions
		WHERE ($1 = '' OR namespace = $1)
		  AND ($2 = '' OR workflow_type = $2)
		  AND ($3 = '' OR status = $3)
		  AND ($4::timestamptz IS NULL OR start_time >= $4)
		  AND ($5::timestamptz IS NULL OR start_time <= $5)
		ORDER BY start_time DESC
		LIMIT $6 OFFSET $7
	`
	rows, err := pool.Query(ctx, query, namespace, workflowType, status, from, to, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWorkflowExecutions(rows)
}

func GetNamespaceOverview(ctx context.Context, namespace string) ([]models.NamespaceOverview, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			namespace,
			workflow_type,
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE status = 'Running')      AS running,
			COUNT(*) FILTER (WHERE status = 'Completed')     AS completed,
			COUNT(*) FILTER (WHERE status = 'Failed')        AS failed,
			COUNT(*) FILTER (WHERE status = 'Canceled')      AS canceled,
			COUNT(*) FILTER (WHERE status = 'TimedOut')      AS timed_out,
			COUNT(*) FILTER (WHERE status = 'Terminated')    AS terminated,
			AVG(duration_ms) FILTER (WHERE duration_ms IS NOT NULL) AS avg_duration_ms,
			MAX(duration_ms) AS max_duration_ms
		FROM workflow_executions
		WHERE ($1 = '' OR namespace = $1)
		GROUP BY namespace, workflow_type
		ORDER BY namespace, workflow_type
	`, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []models.NamespaceOverview
	for rows.Next() {
		var o models.NamespaceOverview
		if err := rows.Scan(&o.Namespace, &o.WorkflowType, &o.Total, &o.Running, &o.Completed,
			&o.Failed, &o.Canceled, &o.TimedOut, &o.Terminated, &o.AvgDurationMs, &o.MaxDurationMs); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, nil
}

func ListActivityExecutions(ctx context.Context, namespace, workflowID, runID string) ([]models.ActivityExecution, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, namespace, workflow_id, run_id, activity_type, activity_id, attempt, status,
		       scheduled_time, start_time, close_time, duration_ms, failure_type, failure_message
		FROM activity_executions
		WHERE namespace = $1 AND workflow_id = $2 AND run_id = $3
		ORDER BY scheduled_time ASC NULLS LAST, attempt ASC
	`, namespace, workflowID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []models.ActivityExecution
	for rows.Next() {
		var a models.ActivityExecution
		if err := rows.Scan(&a.ID, &a.Namespace, &a.WorkflowID, &a.RunID, &a.ActivityType, &a.ActivityID,
			&a.Attempt, &a.Status, &a.ScheduledTime, &a.StartTime, &a.CloseTime, &a.DurationMs,
			&a.FailureType, &a.FailureMessage); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}
