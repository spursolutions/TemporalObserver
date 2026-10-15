package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/gin-gonic/gin"
)

// APIKeyAuth gates every route behind a shared key when OBSERVER_API_KEY is configured. If it's
// left unset (e.g. local dev), auth is skipped entirely - this mirrors TemporalBridge's own
// opt-in auth pattern rather than inventing a new one.
func APIKeyAuth(apiKey string) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		if apiKey == "" {
			ctx.Next()
			return
		}
		provided := ctx.GetHeader("X-API-Key")
		if provided == "" {
			provided = ctx.Query("api_key")
		}
		if subtle.ConstantTimeCompare([]byte(provided), []byte(apiKey)) != 1 {
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or missing API key"})
			return
		}
		ctx.Next()
	}
}
