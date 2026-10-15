package collector

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/getsentry/sentry-go"

	observerclient "TemporalObserver/client"
	"TemporalObserver/config"
	"TemporalObserver/db"
	"TemporalObserver/models"
)

const (
	maxAlertScanRows = 2000

	alertLookback = 2 * time.Hour // re-scanned every cycle; UpsertAlert dedupes so this is safe

	defaultWorkflowStuckMs  = int64(2 * time.Hour / time.Millisecond)
	defaultWorkflowSlowMs   = int64(2 * time.Hour / time.Millisecond)
	defaultActivityMs       = int64(10 * time.Minute / time.Millisecond)
	defaultBaselineMultiple = 2.0

	highRetryAttempts = 5

	minSamplesForFailureRate = 5
	warningFailureRate       = 0.20
	criticalFailureRate      = 0.50

	severityWarning  = "warning"
	severityCritical = "critical"
)

// RunAlertLoop periodically evaluates stuck/slow workflows and activities, repeated retries,
// and elevated failure rates, writing results to the alerts table. The registry's current
// namespace set is re-read every tick, same as the collector's own poll loop.
func RunAlertLoop(ctx context.Context, cfg config.Config, registry *observerclient.Registry) {
	ticker := time.NewTicker(cfg.AlertInterval)
	defer ticker.Stop()

	evaluateAll(registry)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			evaluateAll(registry)
		}
	}
}

func evaluateAll(registry *observerclient.Registry) {
	ctx := context.Background()
	for _, ns := range registry.Namespaces() {
		if err := evaluateNamespace(ctx, ns); err != nil {
			log.Printf("[alerts] namespace %q evaluation failed: %v", ns, err)
			sentry.CaptureException(err)
		}
	}
}

func evaluateNamespace(ctx context.Context, ns string) error {
	if err := evaluateStuckWorkflows(ctx, ns); err != nil {
		return err
	}
	if err := evaluateSlowWorkflows(ctx, ns); err != nil {
		return err
	}
	if err := evaluateActivities(ctx, ns); err != nil {
		return err
	}
	return evaluateFailureRates(ctx, ns)
}

func evaluateStuckWorkflows(ctx context.Context, ns string) error {
	running, err := db.GetRunningExecutions(ctx, ns, maxAlertScanRows)
	if err != nil {
		return err
	}
	for _, w := range running {
		threshold, err := resolveThreshold(ctx, ns, w.WorkflowType, "", defaultWorkflowStuckMs)
		if err != nil {
			return err
		}
		elapsedMs := time.Since(w.StartTime).Milliseconds()
		if elapsedMs <= threshold {
			continue
		}
		severity := severityWarning
		if elapsedMs > threshold*2 {
			severity = severityCritical
		}
		err = db.UpsertAlert(ctx, models.Alert{
			Namespace: ns, WorkflowID: w.WorkflowID, RunID: w.RunID, WorkflowType: w.WorkflowType,
			AlertType: "stuck_workflow", Severity: severity,
			Message: fmt.Sprintf("running for %s, exceeds threshold of %s", time.Duration(elapsedMs)*time.Millisecond, time.Duration(threshold)*time.Millisecond),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func evaluateSlowWorkflows(ctx context.Context, ns string) error {
	closed, err := db.GetRecentlyClosedForAlerting(ctx, ns, int(alertLookback.Seconds()))
	if err != nil {
		return err
	}
	for _, w := range closed {
		if w.DurationMs == nil {
			continue
		}
		threshold, err := resolveThreshold(ctx, ns, w.WorkflowType, "", defaultWorkflowSlowMs)
		if err != nil {
			return err
		}
		if *w.DurationMs <= threshold {
			continue
		}
		severity := severityWarning
		if *w.DurationMs > threshold*2 {
			severity = severityCritical
		}
		err = db.UpsertAlert(ctx, models.Alert{
			Namespace: ns, WorkflowID: w.WorkflowID, RunID: w.RunID, WorkflowType: w.WorkflowType,
			AlertType: "slow_workflow", Severity: severity,
			Message: fmt.Sprintf("took %s, exceeds threshold of %s", time.Duration(*w.DurationMs)*time.Millisecond, time.Duration(threshold)*time.Millisecond),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func evaluateActivities(ctx context.Context, ns string) error {
	activities, err := db.GetRecentActivitiesForAlerting(ctx, ns, int(alertLookback.Seconds()))
	if err != nil {
		return err
	}
	for _, a := range activities {
		if a.Attempt >= highRetryAttempts {
			err = db.UpsertAlert(ctx, models.Alert{
				Namespace: ns, WorkflowID: a.WorkflowID, RunID: a.RunID, WorkflowType: a.WorkflowType,
				ActivityType: a.ActivityType, ActivityID: a.ActivityID, AlertType: "repeated_retry", Severity: severityWarning,
				Message: fmt.Sprintf("attempt %d (>= %d)", a.Attempt, highRetryAttempts),
			})
			if err != nil {
				return err
			}
		}

		if a.DurationMs == nil {
			continue
		}
		threshold, err := resolveThreshold(ctx, ns, a.WorkflowType, a.ActivityType, defaultActivityMs)
		if err != nil {
			return err
		}
		if *a.DurationMs <= threshold {
			continue
		}
		severity := severityWarning
		if *a.DurationMs > threshold*2 {
			severity = severityCritical
		}
		err = db.UpsertAlert(ctx, models.Alert{
			Namespace: ns, WorkflowID: a.WorkflowID, RunID: a.RunID, WorkflowType: a.WorkflowType,
			ActivityType: a.ActivityType, ActivityID: a.ActivityID, AlertType: "slow_activity", Severity: severity,
			Message: fmt.Sprintf("took %s, exceeds threshold of %s", time.Duration(*a.DurationMs)*time.Millisecond, time.Duration(threshold)*time.Millisecond),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func evaluateFailureRates(ctx context.Context, ns string) error {
	rates, err := db.GetFailureRate(ctx, ns, int(alertLookback.Seconds()))
	if err != nil {
		return err
	}
	for workflowType, counts := range rates {
		failed, total := counts[0], counts[1]
		if total < minSamplesForFailureRate {
			continue
		}
		rate := float64(failed) / float64(total)
		if rate < warningFailureRate {
			if err := db.ResolveAggregateAlert(ctx, ns, workflowType, "failure_rate"); err != nil {
				return err
			}
			continue
		}
		severity := severityWarning
		if rate >= criticalFailureRate {
			severity = severityCritical
		}
		err = db.UpsertAlert(ctx, models.Alert{
			Namespace: ns, WorkflowID: db.AggregateAlertWorkflowID(), RunID: "", WorkflowType: workflowType,
			AlertType: "failure_rate", Severity: severity,
			Message: fmt.Sprintf("%.0f%% failed (%d/%d) in the last %s", rate*100, failed, total, alertLookback),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// resolveThreshold applies the precedence described in the architecture plan: an explicit
// operator override first, then the computed baseline (p95 * a multiplier), then a flat
// fallback for workflow types with no history yet.
func resolveThreshold(ctx context.Context, namespace, workflowType, activityType string, fallbackMs int64) (int64, error) {
	if t, err := db.GetThreshold(ctx, namespace, workflowType, activityType); err == nil {
		if t.MaxDurationMs != nil {
			return *t.MaxDurationMs, nil
		}
		if t.BaselineMultiplier != nil {
			if b, err := db.GetBaseline(ctx, namespace, workflowType, activityType); err == nil && b.P95Ms != nil {
				return int64(float64(*b.P95Ms) * *t.BaselineMultiplier), nil
			} else {
				reportLookupError("GetBaseline", namespace, workflowType, activityType, err)
			}
		}
	} else {
		reportLookupError("GetThreshold", namespace, workflowType, activityType, err)
	}
	if b, err := db.GetBaseline(ctx, namespace, workflowType, activityType); err == nil && b.P95Ms != nil {
		return int64(float64(*b.P95Ms) * defaultBaselineMultiple), nil
	} else {
		reportLookupError("GetBaseline", namespace, workflowType, activityType, err)
	}
	return fallbackMs, nil
}

// reportLookupError surfaces a real DB failure (connection/timeout/pool exhaustion) instead of
// letting it look identical to "no override/baseline configured", which is the expected,
// silent case (db.IsNotFound) and not logged.
func reportLookupError(op, namespace, workflowType, activityType string, err error) {
	if err == nil || db.IsNotFound(err) {
		return
	}
	log.Printf("[alerts] %s(%s, %s, %s) failed: %v", op, namespace, workflowType, activityType, err)
	sentry.CaptureException(err)
}
