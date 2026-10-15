package controllers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"TemporalObserver/db"
)

// SearchWorkflows exposes the allowlisted-field search described in the architecture plan:
// namespace/type/status/time-range filters (standard visibility, free) plus a substring match
// against the allowlisted search_index - either one specific field, or (field left blank) any
// allowlisted field's value for that execution. Never the raw payload: search_index only ever
// contains values an operator explicitly allowlisted via POST /api/allowlist. Always paginated
// (limit capped, offset-based) so a value matching many executions returns a bounded page.
func SearchWorkflows(ctx *gin.Context) {
	namespace := ctx.Query("namespace")
	workflowType := ctx.Query("workflow_type")
	status := ctx.Query("status")
	field := ctx.Query("field")
	value := ctx.Query("value")

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

	limit := 25
	if v, err := strconv.Atoi(ctx.Query("limit")); err == nil && v > 0 && v <= 200 {
		limit = v
	}
	offset := 0
	if v, err := strconv.Atoi(ctx.Query("offset")); err == nil && v >= 0 {
		offset = v
	}

	result, err := db.SearchWorkflows(ctx.Request.Context(), namespace, workflowType, status, field, value, from, to, limit, offset)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"results": result.Rows,
		"total":   result.Total,
		"limit":   limit,
		"offset":  offset,
	})
}
