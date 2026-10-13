package routes

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"TemporalObserver/controllers"
)

func SetupRouter() *gin.Engine {
	router := gin.Default()

	router.GET("/health", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	api := router.Group("/api")
	{
		api.GET("/overview", controllers.GetOverview)
		api.GET("/workflows", controllers.ListWorkflows)
		api.GET("/workflows/:namespace/:workflowId/:runId/activities", controllers.GetWorkflowActivities)
	}

	return router
}
