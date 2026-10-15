package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"TemporalObserver/models"
)

// UpsertSearchIndex stores ONLY the allowlisted scalar fields already extracted by the
// collector (see collector/payload.go) - never a raw payload. fields must already be the
// filtered result of an AllowlistSet.Canonical check (see db.GetAllowlistFields).
func UpsertSearchIndex(ctx context.Context, namespace, workflowID, runID string, fields map[string]string) error {
	if len(fields) == 0 {
		return nil
	}
	blob, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
		UPDATE workflow_executions
		SET search_index = search_index || $4::jsonb
		WHERE namespace = $1 AND workflow_id = $2 AND run_id = $3
	`, namespace, workflowID, runID, blob)
	return err
}

// likePattern builds a substring ILIKE pattern, escaping the three characters that are
// special to LIKE/ILIKE (\, %, _) so a value containing them is matched literally rather
// than as a wildcard.
func likePattern(value string) string {
	escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(value)
	return "%" + escaped + "%"
}

// SearchResult bundles a page of matches with the total match count (before LIMIT/OFFSET),
// so callers can paginate without a separate COUNT(*) round trip.
type SearchResult struct {
	Rows  []models.WorkflowExecution
	Total int64
}

// SearchWorkflows filters on the same basic columns as ListWorkflowExecutions (namespace,
// workflow type, status, start-time range - all free via standard Temporal visibility), plus
// a substring match against the allowlisted search_index:
//   - field set:    substring match against that one field's value only.
//   - field empty:  substring match against ANY allowlisted field's value for that execution -
//     still scoped to search_index, which only ever contains values the operator explicitly
//     allowlisted (see db.AddAllowlistField) - never raw payload content.
//
// Results are paginated (limit/offset, capped by the caller) so a value that matches many
// executions returns a bounded page instead of an unbounded result set.
func SearchWorkflows(ctx context.Context, namespace, workflowType, status, field, value string, from, to *time.Time, limit, offset int) (SearchResult, error) {
	args := []interface{}{namespace, workflowType, status, from, to}
	argIdx := 6

	var valueClause string
	switch {
	case value == "":
		valueClause = "TRUE"
	case field != "":
		valueClause = fmt.Sprintf("search_index ->> $%d ILIKE $%d ESCAPE '\\'", argIdx, argIdx+1)
		args = append(args, field, likePattern(value))
		argIdx += 2
	default:
		valueClause = fmt.Sprintf(
			`EXISTS (SELECT 1 FROM jsonb_each_text(search_index) kv WHERE kv.value ILIKE $%d ESCAPE '\')`,
			argIdx)
		args = append(args, likePattern(value))
		argIdx++
	}

	query := fmt.Sprintf(`
		SELECT id, namespace, workflow_id, run_id, workflow_type, task_queue, status, start_time, close_time, duration_ms,
		       COUNT(*) OVER() AS total_count
		FROM workflow_executions
		WHERE ($1 = '' OR namespace = $1)
		  AND ($2 = '' OR workflow_type = $2)
		  AND ($3 = '' OR status = $3)
		  AND ($4::timestamptz IS NULL OR start_time >= $4)
		  AND ($5::timestamptz IS NULL OR start_time <= $5)
		  AND (%s)
		ORDER BY start_time DESC
		LIMIT $%d OFFSET $%d
	`, valueClause, argIdx, argIdx+1)
	args = append(args, limit, offset)

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return SearchResult{}, err
	}
	defer rows.Close()

	var out SearchResult
	for rows.Next() {
		var w models.WorkflowExecution
		if err := rows.Scan(&w.ID, &w.Namespace, &w.WorkflowID, &w.RunID, &w.WorkflowType,
			&w.TaskQueue, &w.Status, &w.StartTime, &w.CloseTime, &w.DurationMs, &out.Total); err != nil {
			return SearchResult{}, err
		}
		out.Rows = append(out.Rows, w)
	}
	return out, nil
}
