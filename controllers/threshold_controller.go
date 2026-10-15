package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"TemporalObserver/db"
	"TemporalObserver/models"
)

func ListThresholds(ctx *gin.Context) {
	rows, err := db.ListThresholds(ctx.Request.Context())
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"results": rows})
}

// UpsertThreshold lets an operator override the computed baseline for one workflow type (or
// one activity type within it) - e.g. "alert if OrderCompletionWorkflow runs over 6 hours."
func UpsertThreshold(ctx *gin.Context) {
	var t models.Threshold
	if err := ctx.ShouldBindJSON(&t); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if t.Namespace == "" || t.WorkflowType == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "namespace and workflow_type are required"})
		return
	}
	if err := db.UpsertThreshold(ctx.Request.Context(), t); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"status": "ok"})
}
