package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrSensitiveFieldName is wrapped into the error AddAllowlistField returns when a field name
// matches a common PII pattern and force wasn't set.
var ErrSensitiveFieldName = errors.New("field name matches a sensitive pattern")

// AllowlistEntry is one configured, searchable field for a namespace - either scoped to one
// exact workflow type, or WorkflowType == "*" meaning "every workflow type in this namespace".
type AllowlistEntry struct {
	Namespace    string `json:"namespace"`
	WorkflowType string `json:"workflow_type"`
	Field        string `json:"field"`
}

// AllowlistSet is the resolved set of field names permitted for extraction for one
// (namespace, workflow type) pair. Lookup is case-insensitive (keyed by lowercase), but the
// map value preserves the CANONICAL casing as registered via AddAllowlistField - the casing
// that ListAllowlist/the Search UI shows and that search queries are matched against - so a
// payload key whose casing differs from the registered name (e.g. "appraisalId" vs the
// registered "AppraisalID") still gets stored and searched under one consistent name.
type AllowlistSet map[string]string

// Canonical returns the registered field name for a case-insensitive key match, and whether
// one was found.
func (a AllowlistSet) Canonical(key string) (string, bool) {
	canonical, ok := a[strings.ToLower(key)]
	return canonical, ok
}

const allowlistSchema = `
CREATE TABLE IF NOT EXISTS search_allowlist (
	namespace     TEXT NOT NULL,
	workflow_type TEXT NOT NULL,
	field         TEXT NOT NULL,
	created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	PRIMARY KEY (namespace, workflow_type, field)
);
`

// sensitivePatterns is a defense-in-depth guard, not the primary safety mechanism. The
// allowlist itself (matching by bare key name anywhere in the input tree, see
// collector/payload.go) has no concept of JSON path/position, so a generically-named field
// added here for one legitimate purpose could also match the same key name inside an
// unrelated nested object. These patterns catch the common PII-shaped names so adding one
// requires a deliberate override rather than happening silently.
var sensitivePatterns = []string{
	"ssn", "social", "dob", "birth", "name", "email", "phone", "address",
	"license", "account", "card", "password", "secret", "token", "pin",
}

// MatchesSensitivePattern reports whether a field name looks like it could be (or sit next to)
// restricted personal data, case-insensitively.
func MatchesSensitivePattern(field string) (string, bool) {
	lower := strings.ToLower(field)
	for _, p := range sensitivePatterns {
		if strings.Contains(lower, p) {
			return p, true
		}
	}
	return "", false
}

// AddAllowlistField makes one more field searchable. workflowType == "" is treated as "*"
// (namespace-wide). This is the ONLY way a new field ever becomes extractable - there is no
// code path that stores a field that hasn't been added here first. Unless force is true, a
// field name matching MatchesSensitivePattern is rejected rather than silently accepted.
func AddAllowlistField(ctx context.Context, namespace, workflowType, field string, force bool) error {
	if workflowType == "" {
		workflowType = "*"
	}
	if pattern, matched := MatchesSensitivePattern(field); matched && !force {
		return fmt.Errorf("%w: field %q matches common PII pattern %q - retry with force=true if this is intentional and safe for this workflow type", ErrSensitiveFieldName, field, pattern)
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO search_allowlist (namespace, workflow_type, field)
		VALUES ($1, $2, $3)
		ON CONFLICT (namespace, workflow_type, field) DO NOTHING
	`, namespace, workflowType, field)
	return err
}

func RemoveAllowlistField(ctx context.Context, namespace, workflowType, field string) error {
	if workflowType == "" {
		workflowType = "*"
	}
	_, err := pool.Exec(ctx, `
		DELETE FROM search_allowlist WHERE namespace = $1 AND workflow_type = $2 AND field = $3
	`, namespace, workflowType, field)
	return err
}

// ListAllowlist returns every configured entry, optionally filtered to one namespace, for the
// allowlist management view.
func ListAllowlist(ctx context.Context, namespace string) ([]AllowlistEntry, error) {
	rows, err := pool.Query(ctx, `
		SELECT namespace, workflow_type, field FROM search_allowlist
		WHERE ($1 = '' OR namespace = $1)
		ORDER BY namespace, workflow_type, field
	`, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AllowlistEntry
	for rows.Next() {
		var e AllowlistEntry
		if err := rows.Scan(&e.Namespace, &e.WorkflowType, &e.Field); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// GetAllowlistFields resolves the actual allowed set for one (namespace, workflow type) pair -
// the union of its exact-type entries and the namespace's "*" entries. Called once per
// closed-workflow history fetch, alongside the GetWorkflowHistory call it accompanies.
func GetAllowlistFields(ctx context.Context, namespace, workflowType string) (AllowlistSet, error) {
	rows, err := pool.Query(ctx, `
		SELECT field FROM search_allowlist
		WHERE namespace = $1 AND (workflow_type = $2 OR workflow_type = '*')
	`, namespace, workflowType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	set := AllowlistSet{}
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			return nil, err
		}
		set[strings.ToLower(f)] = f
	}
	return set, nil
}

// DistinctWorkflowTypes lists workflow types actually observed in a namespace, for populating
// the namespace -> workflow type selector in Search.
func DistinctWorkflowTypes(ctx context.Context, namespace string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT workflow_type FROM workflow_executions WHERE namespace = $1 ORDER BY workflow_type
	`, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var wt string
		if err := rows.Scan(&wt); err != nil {
			return nil, err
		}
		out = append(out, wt)
	}
	return out, nil
}

// DistinctNamespaces lists namespaces actually observed, for the namespace selector.
func DistinctNamespaces(ctx context.Context) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT DISTINCT namespace FROM workflow_executions ORDER BY namespace`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var ns string
		if err := rows.Scan(&ns); err != nil {
			return nil, err
		}
		out = append(out, ns)
	}
	return out, nil
}
