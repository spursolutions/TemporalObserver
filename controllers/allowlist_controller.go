package controllers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"TemporalObserver/db"
)

// ListAllowlist returns the configured searchable fields, optionally filtered to one namespace -
// used to populate the Search page's "which keys can I search by" selector.
func ListAllowlist(ctx *gin.Context) {
	rows, err := db.ListAllowlist(ctx.Request.Context(), ctx.Query("namespace"))
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"results": rows})
}

type allowlistRequest struct {
	Namespace    string `json:"namespace" binding:"required"`
	WorkflowType string `json:"workflow_type"` // "" or "*" = every workflow type in this namespace
	Field        string `json:"field" binding:"required"`
	Force        bool   `json:"force"` // required to add a field matching a common PII pattern
}

// AddAllowlistField is the ONLY way a new field ever becomes searchable. This is the
// compliance-sensitive control point: nothing is extracted or stored until an operator
// explicitly adds its key name here, scoped to a namespace (and optionally one workflow type).
// A field name matching a common PII pattern (see db.MatchesSensitivePattern) is rejected
// unless force is set, since matching is by bare key name anywhere in the input tree - not
// scoped to a JSON path - so a generic name could also pull a value out of an unrelated
// nested object.
func AddAllowlistField(ctx *gin.Context) {
	var req allowlistRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := db.AddAllowlistField(ctx.Request.Context(), req.Namespace, req.WorkflowType, req.Field, req.Force); err != nil {
		if errors.Is(err, db.ErrSensitiveFieldName) {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"note":   "applies to newly-closed workflows going forward; already-closed executions are not backfilled",
	})
}

func RemoveAllowlistField(ctx *gin.Context) {
	var req allowlistRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := db.RemoveAllowlistField(ctx.Request.Context(), req.Namespace, req.WorkflowType, req.Field); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// ListNamespaces and ListWorkflowTypes power the Search page's cascading selectors
// (namespace -> workflow type) from data actually observed, not a static list.
func ListNamespaces(ctx *gin.Context) {
	rows, err := db.DistinctNamespaces(ctx.Request.Context())
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"results": rows})
}

func ListWorkflowTypes(ctx *gin.Context) {
	rows, err := db.DistinctWorkflowTypes(ctx.Request.Context(), ctx.Query("namespace"))
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"results": rows})
}
