package routes

import (
	"net/http"

	"github.com/gin-gonic/gin"

	observerclient "TemporalObserver/client"
	"TemporalObserver/config"
	"TemporalObserver/controllers"
	"TemporalObserver/metrics"
	"TemporalObserver/middleware"
	"TemporalObserver/ui"
)

func SetupRouter(cfg config.Config, registry *observerclient.Registry) *gin.Engine {
	router := gin.Default()
	router.SetHTMLTemplate(ui.Templates())
	ui.Init(cfg, registry)

	router.GET("/health", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/metrics", gin.WrapH(metrics.PromHandler()))

	api := router.Group("/api", middleware.APIKeyAuth(cfg.APIKey))
	{
		api.GET("/overview", controllers.GetOverview)
		api.GET("/workflows", controllers.ListWorkflows)
		api.GET("/workflows/:namespace/:workflowId/:runId/activities", controllers.GetWorkflowActivities)
		api.GET("/search", controllers.SearchWorkflows)
		api.GET("/alerts", controllers.ListAlerts)
		api.GET("/thresholds", controllers.ListThresholds)
		api.PUT("/thresholds", controllers.UpsertThreshold)
		api.GET("/namespaces", controllers.ListNamespaces)
		api.GET("/workflow-types", controllers.ListWorkflowTypes)
		api.GET("/allowlist", controllers.ListAllowlist)
		api.POST("/allowlist", controllers.AddAllowlistField)
		api.DELETE("/allowlist", controllers.RemoveAllowlistField)
	}

	uiGroup := router.Group("/ui", middleware.APIKeyAuth(cfg.APIKey))
	{
		uiGroup.GET("/overview", ui.Overview)
		uiGroup.GET("/namespace/:namespace", ui.NamespaceView)
		uiGroup.GET("/workflow/:namespace/:workflowId/:runId", ui.WorkflowDetail)
		uiGroup.GET("/search", ui.SearchPage)
		uiGroup.GET("/alerts", ui.AlertsPage)
	}
	router.GET("/", func(ctx *gin.Context) { ctx.Redirect(http.StatusFound, "/ui/overview") })

	return router
}
