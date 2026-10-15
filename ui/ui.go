package ui

import (
	"embed"
	"html/template"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	observerclient "TemporalObserver/client"
	"TemporalObserver/config"
	"TemporalObserver/db"
)

//go:embed templates/*.html
var templatesFS embed.FS

var temporalUIBaseURL string
var clientRegistry *observerclient.Registry

var funcMap = template.FuncMap{
	// fmtMs renders a *int64/*float64 millisecond duration (nil-safe) as a compact human
	// duration, e.g. "1.5s", "2m3s", "1h4m".
	"fmtMs": func(v interface{}) string {
		var ms int64
		switch t := v.(type) {
		case *int64:
			if t == nil {
				return "-"
			}
			ms = *t
		case *float64:
			if t == nil {
				return "-"
			}
			ms = int64(*t)
		case int64:
			ms = t
		case float64:
			ms = int64(t)
		default:
			return "-"
		}
		return time.Duration(ms * int64(time.Millisecond)).Round(100 * time.Millisecond).String()
	},
	"incr": func(n int) int { return n + 1 },
	"decr": func(n int) int { return n - 1 },
}

// Templates parses every embedded page once at startup. Each file becomes a template named
// after its own filename (e.g. "overview.html") since none of them use {{define}}, so gin can
// render them directly by name with no shared-layout name collisions.
func Templates() *template.Template {
	return template.Must(template.New("").Funcs(funcMap).ParseFS(templatesFS, "templates/*.html"))
}

func Init(cfg config.Config, registry *observerclient.Registry) {
	temporalUIBaseURL = cfg.TemporalUIBaseURL
	clientRegistry = registry
}

func workflowLink(namespace, workflowID, runID string) string {
	return temporalUIBaseURL + "/namespaces/" + namespace + "/workflows/" + workflowID + "/" + runID + "/history"
}

func Overview(ctx *gin.Context) {
	namespace := ctx.Query("namespace")
	rctx := ctx.Request.Context()

	overview, err := db.GetNamespaceOverview(rctx, namespace)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "failed to load overview: %v", err)
		return
	}
	alerts, err := db.ListActiveAlerts(rctx, namespace)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "failed to load alerts: %v", err)
		return
	}
	slowest, err := db.ListSlowestWorkflows(rctx, namespace, 10)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "failed to load slowest workflows: %v", err)
		return
	}
	failures, err := db.ListRecentFailures(rctx, namespace, 10)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "failed to load recent failures: %v", err)
		return
	}

	ctx.HTML(http.StatusOK, "overview.html", gin.H{
		"Namespace": namespace,
		"Overview":  overview,
		"Alerts":    alerts,
		"Slowest":   slowest,
		"Failures":  failures,
	})
}

func NamespaceView(ctx *gin.Context) {
	namespace := ctx.Param("namespace")
	rctx := ctx.Request.Context()

	overview, err := db.GetNamespaceOverview(rctx, namespace)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "failed to load overview: %v", err)
		return
	}
	alerts, err := db.ListActiveAlerts(rctx, namespace)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "failed to load alerts: %v", err)
		return
	}
	slowest, err := db.ListSlowestWorkflows(rctx, namespace, 10)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "failed to load slowest workflows: %v", err)
		return
	}
	failures, err := db.ListRecentFailures(rctx, namespace, 10)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "failed to load recent failures: %v", err)
		return
	}

	ctx.HTML(http.StatusOK, "namespace.html", gin.H{
		"Namespace": namespace,
		"Overview":  overview,
		"Alerts":    alerts,
		"Slowest":   slowest,
		"Failures":  failures,
	})
}

func WorkflowDetail(ctx *gin.Context) {
	namespace := ctx.Param("namespace")
	workflowID := ctx.Param("workflowId")
	runID := ctx.Param("runId")
	rctx := ctx.Request.Context()

	workflow, searchFields, err := db.GetWorkflowExecution(rctx, namespace, workflowID, runID)
	if err != nil {
		ctx.String(http.StatusNotFound, "workflow not found: %v", err)
		return
	}
	activities, err := db.ListActivityExecutions(rctx, namespace, workflowID, runID)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "failed to load activities: %v", err)
		return
	}

	// Live from Temporal, never stored - see rawhistory.go. Best-effort: if Temporal is
	// unreachable or the history has been pruned, show the rest of the page anyway.
	var raw RawHistory
	if c, ok := clientRegistry.Snapshot()[namespace]; ok {
		if fetched, err := FetchRawHistory(rctx, c, workflowID, runID); err == nil {
			raw = fetched
		}
	}

	ctx.HTML(http.StatusOK, "workflow_detail.html", gin.H{
		"Workflow":     workflow,
		"SearchFields": searchFields,
		"RawInput":     raw.Input,
		"RawResult":    raw.Result,
		"Activities":   activities,
		"TemporalLink": workflowLink(namespace, workflowID, runID),
	})
}

const searchPageSize = 25

func SearchPage(ctx *gin.Context) {
	namespace := ctx.Query("namespace")
	workflowType := ctx.Query("workflow_type")
	status := ctx.Query("status")
	field := ctx.Query("field")
	value := ctx.Query("value")
	hasQuery := value != "" // field is optional - blank means "any allowlisted field"

	page := 1
	if v, err := strconv.Atoi(ctx.Query("page")); err == nil && v > 0 {
		page = v
	}
	offset := (page - 1) * searchPageSize

	data := gin.H{
		"Namespace":    namespace,
		"WorkflowType": workflowType,
		"Status":       status,
		"Field":        field,
		"Value":        value,
		"HasQuery":     hasQuery,
		"Page":         page,
	}

	if hasQuery {
		result, err := db.SearchWorkflows(ctx.Request.Context(), namespace, workflowType, status, field, value, nil, nil, searchPageSize, offset)
		if err != nil {
			ctx.String(http.StatusInternalServerError, "search failed: %v", err)
			return
		}
		data["Results"] = result.Rows
		data["Total"] = result.Total
		data["HasNextPage"] = int64(offset+searchPageSize) < result.Total
		data["HasPrevPage"] = page > 1
		data["PageStart"] = 0
		if result.Total > 0 {
			data["PageStart"] = offset + 1
		}
		data["PageEnd"] = offset + len(result.Rows)
	}

	ctx.HTML(http.StatusOK, "search.html", data)
}

func AlertsPage(ctx *gin.Context) {
	namespace := ctx.Query("namespace")
	alerts, err := db.ListActiveAlerts(ctx.Request.Context(), namespace)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "failed to load alerts: %v", err)
		return
	}
	ctx.HTML(http.StatusOK, "alerts.html", gin.H{
		"Namespace": namespace,
		"Alerts":    alerts,
	})
}
