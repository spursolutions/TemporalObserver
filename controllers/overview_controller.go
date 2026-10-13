package controllers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"TemporalObserver/db"
)

// GetOverview returns per-namespace, per-workflow-type volume/success/failure/duration
// aggregates - Temporal Observability Dashboard spec section 1 & 5 (Overview/Namespace views).
func GetOverview(ctx *gin.Context) {
	namespace := ctx.Query("namespace")
	rows, err := db.GetNamespaceOverview(ctx.Request.Context(), namespace)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"results": rows})
}

// ListWorkflows supports the filters standard Temporal visibility already gives for free:
// namespace, workflow type, status, and a start-time range.
func ListWorkflows(ctx *gin.Context) {
	namespace := ctx.Query("namespace")
	workflowType := ctx.Query("workflow_type")
	status := ctx.Query("status")

	var from, to *time.Time
	if v := ctx.Query("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			from = &t
		}
	}
	if v := ctx.Query("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			to = &t
		}
	}

	limit := 50
	if v, err := strconv.Atoi(ctx.Query("limit")); err == nil && v > 0 && v <= 500 {
		limit = v
	}
	offset := 0
	if v, err := strconv.Atoi(ctx.Query("offset")); err == nil && v >= 0 {
		offset = v
	}

	rows, err := db.ListWorkflowExecutions(ctx.Request.Context(), namespace, workflowType, status, from, to, limit, offset)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"results": rows})
}

// GetWorkflowActivities returns the extracted activity-execution history for one workflow run -
// durations, retries, failure reason - for the Workflow Detail drill-down view.
func GetWorkflowActivities(ctx *gin.Context) {
	namespace := ctx.Param("namespace")
	workflowID := ctx.Param("workflowId")
	runID := ctx.Param("runId")

	rows, err := db.ListActivityExecutions(ctx.Request.Context(), namespace, workflowID, runID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"results": rows})
}
