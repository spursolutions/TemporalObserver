package collector

import (
	"context"
	"log"
	"time"

	"github.com/getsentry/sentry-go"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/timestamppb"

	"TemporalObserver/config"
	"TemporalObserver/db"
	"TemporalObserver/models"
)

const pageSize = int32(100)

// Run drives the polling loop for every configured namespace. It only ever calls Temporal's
// read-only visibility/history APIs - nothing here can start, signal, or cancel a workflow.
func Run(ctx context.Context, cfg config.Config, clients map[string]client.Client) {
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	// Run one pass immediately on startup instead of waiting for the first tick.
	pollAll(ctx, cfg, clients)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pollAll(ctx, cfg, clients)
		}
	}
}

func pollAll(ctx context.Context, cfg config.Config, clients map[string]client.Client) {
	for ns, c := range clients {
		if err := pollNamespace(ctx, cfg, ns, c); err != nil {
			log.Printf("[collector] namespace %q poll failed: %v", ns, err)
			sentry.CaptureException(err)
		}
	}
}

func pollNamespace(ctx context.Context, cfg config.Config, ns string, c client.Client) error {
	now := time.Now().UTC()

	if err := syncOpenExecutions(ctx, c, ns, now.Add(-cfg.OpenLookbackWindow), now); err != nil {
		return err
	}
	if err := syncRecentlyClosed(ctx, cfg, c, ns, now); err != nil {
		return err
	}
	if err := reconcileRunning(ctx, cfg, c, ns); err != nil {
		return err
	}
	if err := fetchPendingHistories(ctx, cfg, c, ns); err != nil {
		return err
	}
	return nil
}

// syncOpenExecutions discovers currently-running workflows so we know about them even before
// they close. Standard (non-Advanced) visibility only supports one filter at a time, so this
// uses StartTimeFilter scoped to the configured lookback window.
func syncOpenExecutions(ctx context.Context, c client.Client, ns string, earliest, latest time.Time) error {
	var nextPageToken []byte
	for {
		resp, err := c.ListOpenWorkflow(ctx, &workflowservice.ListOpenWorkflowExecutionsRequest{
			Namespace:       ns,
			MaximumPageSize: pageSize,
			NextPageToken:   nextPageToken,
			StartTimeFilter: &filterpb.StartTimeFilter{
				EarliestTime: timestamppb.New(earliest),
				LatestTime:   timestamppb.New(latest),
			},
		})
		if err != nil {
			return err
		}
		for _, exec := range resp.GetExecutions() {
			w := models.WorkflowExecution{
				Namespace:    ns,
				WorkflowID:   exec.GetExecution().GetWorkflowId(),
				RunID:        exec.GetExecution().GetRunId(),
				WorkflowType: exec.GetType().GetName(),
				TaskQueue:    exec.GetTaskQueue(),
				Status:       statusName(exec.GetStatus()),
				StartTime:    exec.GetStartTime().AsTime(),
			}
			if err := db.UpsertWorkflowExecution(ctx, w); err != nil {
				return err
			}
		}
		nextPageToken = resp.GetNextPageToken()
		if len(nextPageToken) == 0 {
			break
		}
	}
	return nil
}

// syncRecentlyClosed catches workflows that both started and closed since the last poll,
// including ones too short-lived for syncOpenExecutions to have observed in time.
func syncRecentlyClosed(ctx context.Context, cfg config.Config, c client.Client, ns string, now time.Time) error {
	cursor, err := db.GetClosedCursor(ctx, ns, now.Add(-cfg.OpenLookbackWindow))
	if err != nil {
		return err
	}

	var nextPageToken []byte
	for {
		resp, err := c.ListClosedWorkflow(ctx, &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       ns,
			MaximumPageSize: pageSize,
			NextPageToken:   nextPageToken,
			StartTimeFilter: &filterpb.StartTimeFilter{
				EarliestTime: timestamppb.New(cursor),
				LatestTime:   timestamppb.New(now),
			},
		})
		if err != nil {
			return err
		}
		for _, exec := range resp.GetExecutions() {
			if err := upsertClosedExecution(ctx, ns, exec); err != nil {
				return err
			}
		}
		nextPageToken = resp.GetNextPageToken()
		if len(nextPageToken) == 0 {
			break
		}
	}

	// Small overlap so a workflow that closes right at the boundary isn't missed next poll.
	return db.SetClosedCursor(ctx, ns, now.Add(-10*time.Second))
}

func upsertClosedExecution(ctx context.Context, ns string, exec *workflowpb.WorkflowExecutionInfo) error {
	w := models.WorkflowExecution{
		Namespace:    ns,
		WorkflowID:   exec.GetExecution().GetWorkflowId(),
		RunID:        exec.GetExecution().GetRunId(),
		WorkflowType: exec.GetType().GetName(),
		TaskQueue:    exec.GetTaskQueue(),
		Status:       statusName(exec.GetStatus()),
		StartTime:    exec.GetStartTime().AsTime(),
	}
	if ct := exec.GetCloseTime(); ct != nil {
		closeTime := ct.AsTime()
		w.CloseTime = &closeTime
		durationMs := closeTime.Sub(w.StartTime).Milliseconds()
		w.DurationMs = &durationMs
	}
	return db.UpsertWorkflowExecution(ctx, w)
}

// reconcileRunning re-checks rows we still believe are "Running" directly via
// DescribeWorkflowExecution, so long-running or out-of-window workflows still get marked
// closed even if the listing APIs above miss the transition.
func reconcileRunning(ctx context.Context, cfg config.Config, c client.Client, ns string) error {
	running, err := db.GetRunningExecutions(ctx, ns, cfg.ReconcileBatch)
	if err != nil {
		return err
	}
	for _, w := range running {
		desc, err := c.DescribeWorkflowExecution(ctx, w.WorkflowID, w.RunID)
		if err != nil {
			// Execution may have been pruned from Temporal's retention; leave it as-is.
			log.Printf("[collector] describe %s/%s failed: %v", w.WorkflowID, w.RunID, err)
			continue
		}
		info := desc.GetWorkflowExecutionInfo()
		if info == nil {
			continue
		}
		updated := models.WorkflowExecution{
			Namespace:    ns,
			WorkflowID:   w.WorkflowID,
			RunID:        w.RunID,
			WorkflowType: info.GetType().GetName(),
			TaskQueue:    info.GetTaskQueue(),
			Status:       statusName(info.GetStatus()),
			StartTime:    w.StartTime,
		}
		if ct := info.GetCloseTime(); ct != nil {
			closeTime := ct.AsTime()
			updated.CloseTime = &closeTime
			durationMs := closeTime.Sub(w.StartTime).Milliseconds()
			updated.DurationMs = &durationMs
		}
		if err := db.UpsertWorkflowExecution(ctx, updated); err != nil {
			return err
		}
	}
	return nil
}

// fetchPendingHistories pulls full history for closed workflows exactly once, extracting
// activity-level attempts, retries, and failure reasons.
func fetchPendingHistories(ctx context.Context, cfg config.Config, c client.Client, ns string) error {
	pending, err := db.GetPendingHistoryFetch(ctx, ns, cfg.HistoryFetchBatch)
	if err != nil {
		return err
	}
	for _, w := range pending {
		if err := extractActivities(ctx, c, ns, w.WorkflowID, w.RunID); err != nil {
			log.Printf("[collector] history fetch %s/%s failed: %v", w.WorkflowID, w.RunID, err)
			sentry.CaptureException(err)
			continue
		}
		if err := db.MarkHistoryFetched(ctx, ns, w.WorkflowID, w.RunID); err != nil {
			return err
		}
	}
	return nil
}

type activityAttempt struct {
	activityType   string
	activityID     string
	attempt        int32
	scheduledTime  *time.Time
	startTime      *time.Time
	closeTime      *time.Time
	status         string
	failureType    string
	failureMessage string
}

func extractActivities(ctx context.Context, c client.Client, ns, workflowID, runID string) error {
	byScheduledID := map[int64]*activityAttempt{}

	iter := c.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return err
		}
		applyHistoryEvent(byScheduledID, event)
	}

	for _, a := range byScheduledID {
		rec := models.ActivityExecution{
			Namespace:      ns,
			WorkflowID:     workflowID,
			RunID:          runID,
			ActivityType:   a.activityType,
			ActivityID:     a.activityID,
			Attempt:        a.attempt,
			Status:         a.status,
			ScheduledTime:  a.scheduledTime,
			StartTime:      a.startTime,
			CloseTime:      a.closeTime,
			FailureType:    a.failureType,
			FailureMessage: a.failureMessage,
		}
		if a.startTime != nil && a.closeTime != nil {
			durationMs := a.closeTime.Sub(*a.startTime).Milliseconds()
			rec.DurationMs = &durationMs
		}
		if err := db.UpsertActivityExecution(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

func applyHistoryEvent(byScheduledID map[int64]*activityAttempt, event *historypb.HistoryEvent) {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		attrs := event.GetActivityTaskScheduledEventAttributes()
		t := event.GetEventTime().AsTime()
		byScheduledID[event.GetEventId()] = &activityAttempt{
			activityType:  attrs.GetActivityType().GetName(),
			activityID:    attrs.GetActivityId(),
			attempt:       1,
			scheduledTime: &t,
			status:        "Scheduled",
		}
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
		attrs := event.GetActivityTaskStartedEventAttributes()
		a := byScheduledID[attrs.GetScheduledEventId()]
		if a == nil {
			return
		}
		t := event.GetEventTime().AsTime()
		a.startTime = &t
		a.attempt = attrs.GetAttempt()
		a.status = "Started"
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		attrs := event.GetActivityTaskCompletedEventAttributes()
		a := byScheduledID[attrs.GetScheduledEventId()]
		if a == nil {
			return
		}
		t := event.GetEventTime().AsTime()
		a.closeTime = &t
		a.status = "Completed"
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		attrs := event.GetActivityTaskFailedEventAttributes()
		a := byScheduledID[attrs.GetScheduledEventId()]
		if a == nil {
			return
		}
		t := event.GetEventTime().AsTime()
		a.closeTime = &t
		a.status = "Failed"
		if f := attrs.GetFailure(); f != nil {
			a.failureMessage = f.GetMessage()
			if ai := f.GetApplicationFailureInfo(); ai != nil {
				a.failureType = ai.GetType()
			}
		}
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		attrs := event.GetActivityTaskTimedOutEventAttributes()
		a := byScheduledID[attrs.GetScheduledEventId()]
		if a == nil {
			return
		}
		t := event.GetEventTime().AsTime()
		a.closeTime = &t
		a.status = "TimedOut"
		a.failureType = "Timeout"
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		attrs := event.GetActivityTaskCanceledEventAttributes()
		a := byScheduledID[attrs.GetScheduledEventId()]
		if a == nil {
			return
		}
		t := event.GetEventTime().AsTime()
		a.closeTime = &t
		a.status = "Canceled"
	}
}

func statusName(status enumspb.WorkflowExecutionStatus) string {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return "Running"
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return "Completed"
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return "Failed"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return "Canceled"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return "Terminated"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return "ContinuedAsNew"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return "TimedOut"
	default:
		return "Unknown"
	}
}
