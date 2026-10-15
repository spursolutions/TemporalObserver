package ui

import (
	"bytes"
	"context"
	"encoding/json"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/client"
)

// RawHistory is the live-fetched (never stored, never indexed) input/result for one workflow
// execution's Workflow Detail page - the same data "open in temporal-ui" already shows one
// click away, just rendered inline. Unlike collector/payload.go's extraction, this is NOT
// filtered by the allowlist and is NEVER written to TemporalObserver's own database: it exists
// only for the duration of rendering this one page view, gated by the same auth as everything
// else here (OBSERVER_API_KEY). It is fetched fresh from Temporal on every page load.
type RawHistory struct {
	Input  []string // one entry per input payload/arg, pretty-printed JSON (or raw text if not JSON)
	Result []string // one entry per result/failure payload, same rendering
}

// FetchRawHistory pulls just the WorkflowExecutionStarted input and the terminal event's
// result/failure/cancellation/termination details - not the full event history, and nothing
// from it is kept after the HTTP response is written.
func FetchRawHistory(ctx context.Context, c client.Client, workflowID, runID string) (RawHistory, error) {
	var out RawHistory
	iter := c.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return out, err
		}
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
			attrs := event.GetWorkflowExecutionStartedEventAttributes()
			out.Input = renderPayloads(attrs.GetInput())
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
			attrs := event.GetWorkflowExecutionCompletedEventAttributes()
			out.Result = renderPayloads(attrs.GetResult())
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
			attrs := event.GetWorkflowExecutionFailedEventAttributes()
			out.Result = renderFailure(attrs.GetFailure())
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
			out.Result = []string{`"workflow timed out"`}
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
			attrs := event.GetWorkflowExecutionTerminatedEventAttributes()
			out.Result = []string{renderJSON(map[string]interface{}{
				"reason": attrs.GetReason(),
			})}
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
			attrs := event.GetWorkflowExecutionCanceledEventAttributes()
			out.Result = renderPayloads(attrs.GetDetails())
		}
	}
	return out, nil
}

func renderPayloads(payloads *commonpb.Payloads) []string {
	var out []string
	for _, p := range payloads.GetPayloads() {
		if p == nil {
			continue
		}
		encoding := string(p.GetMetadata()["encoding"])
		if encoding != "json/plain" {
			out = append(out, string(p.GetData()))
			continue
		}
		var decoded interface{}
		dec := json.NewDecoder(bytes.NewReader(p.GetData()))
		dec.UseNumber()
		if err := dec.Decode(&decoded); err != nil {
			out = append(out, string(p.GetData()))
			continue
		}
		out = append(out, renderJSON(decoded))
	}
	return out
}

func renderFailure(f *failurepb.Failure) []string {
	if f == nil {
		return nil
	}
	m := map[string]interface{}{
		"message": f.GetMessage(),
		"source":  f.GetSource(),
	}
	if info := f.GetApplicationFailureInfo(); info != nil {
		m["type"] = info.GetType()
	}
	if cause := f.GetCause(); cause != nil {
		m["cause"] = cause.GetMessage()
	}
	return []string{renderJSON(m)}
}

func renderJSON(v interface{}) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return ""
	}
	return string(b)
}
