package db

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"TemporalObserver/models"
)

// IsNotFound reports whether err is simply "no row matched" (an expected, non-error outcome
// for GetThreshold/GetBaseline - e.g. no override configured) as opposed to a real failure
// (connection/timeout/pool exhaustion) that happens to come back through the same Scan call.
func IsNotFound(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

// RecomputeBaselines derives p50/p95 duration baselines per (namespace, workflow_type) and
// per (namespace, workflow_type, activity_type) from everything collected so far. Workflow-level
// rows use activity_type = ” so both share one table.
func RecomputeBaselines(ctx context.Context) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO duration_baselines (namespace, workflow_type, activity_type, sample_count, p50_ms, p95_ms, computed_at)
		SELECT namespace, workflow_type, '', COUNT(*),
		       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ms)::bigint,
		       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)::bigint,
		       NOW()
		FROM workflow_executions
		WHERE duration_ms IS NOT NULL
		GROUP BY namespace, workflow_type
		ON CONFLICT (namespace, workflow_type, activity_type) DO UPDATE SET
			sample_count = EXCLUDED.sample_count,
			p50_ms       = EXCLUDED.p50_ms,
			p95_ms       = EXCLUDED.p95_ms,
			computed_at  = NOW()
	`)
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO duration_baselines (namespace, workflow_type, activity_type, sample_count, p50_ms, p95_ms, computed_at)
		SELECT a.namespace, w.workflow_type, a.activity_type, COUNT(*),
		       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a.duration_ms)::bigint,
		       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY a.duration_ms)::bigint,
		       NOW()
		FROM activity_executions a
		JOIN workflow_executions w
		  ON w.namespace = a.namespace AND w.workflow_id = a.workflow_id AND w.run_id = a.run_id
		WHERE a.duration_ms IS NOT NULL
		GROUP BY a.namespace, w.workflow_type, a.activity_type
		ON CONFLICT (namespace, workflow_type, activity_type) DO UPDATE SET
			sample_count = EXCLUDED.sample_count,
			p50_ms       = EXCLUDED.p50_ms,
			p95_ms       = EXCLUDED.p95_ms,
			computed_at  = NOW()
	`)
	return err
}

func GetBaseline(ctx context.Context, namespace, workflowType, activityType string) (*models.DurationBaseline, error) {
	var b models.DurationBaseline
	err := pool.QueryRow(ctx, `
		SELECT namespace, workflow_type, activity_type, sample_count, p50_ms, p95_ms, computed_at
		FROM duration_baselines
		WHERE namespace = $1 AND workflow_type = $2 AND activity_type = $3
	`, namespace, workflowType, activityType).Scan(
		&b.Namespace, &b.WorkflowType, &b.ActivityType, &b.SampleCount, &b.P50Ms, &b.P95Ms, &b.ComputedAt)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func GetThreshold(ctx context.Context, namespace, workflowType, activityType string) (*models.Threshold, error) {
	var t models.Threshold
	err := pool.QueryRow(ctx, `
		SELECT namespace, workflow_type, activity_type, max_duration_ms, baseline_multiplier
		FROM thresholds
		WHERE namespace = $1 AND workflow_type = $2 AND activity_type = $3
	`, namespace, workflowType, activityType).Scan(
		&t.Namespace, &t.WorkflowType, &t.ActivityType, &t.MaxDurationMs, &t.BaselineMultiplier)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func ListThresholds(ctx context.Context) ([]models.Threshold, error) {
	rows, err := pool.Query(ctx, `
		SELECT namespace, workflow_type, activity_type, max_duration_ms, baseline_multiplier
		FROM thresholds
		ORDER BY namespace, workflow_type, activity_type
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.Threshold
	for rows.Next() {
		var t models.Threshold
		if err := rows.Scan(&t.Namespace, &t.WorkflowType, &t.ActivityType, &t.MaxDurationMs, &t.BaselineMultiplier); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

func UpsertThreshold(ctx context.Context, t models.Threshold) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO thresholds (namespace, workflow_type, activity_type, max_duration_ms, baseline_multiplier, updated_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (namespace, workflow_type, activity_type) DO UPDATE SET
			max_duration_ms     = EXCLUDED.max_duration_ms,
			baseline_multiplier = EXCLUDED.baseline_multiplier,
			updated_at          = NOW()
	`, t.Namespace, t.WorkflowType, t.ActivityType, t.MaxDurationMs, t.BaselineMultiplier)
	return err
}
