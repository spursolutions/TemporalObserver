package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"TemporalObserver/db"
)

func ListAlerts(ctx *gin.Context) {
	namespace := ctx.Query("namespace")
	rows, err := db.ListActiveAlerts(ctx.Request.Context(), namespace)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"results": rows})
}
