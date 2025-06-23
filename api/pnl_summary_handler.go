package api

import (
	"net/http"
	"time"

	"gohustle/logger"

	"github.com/gorilla/mux"
)

// HandleGetLatestPnLSummary returns the latest P&L summary for both real and paper trading
func HandleGetLatestPnLSummary(w http.ResponseWriter, r *http.Request) {
	log := logger.L()
	log.Debug("Returning dummy P&L summary")

	// Return a dummy response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "P&L summary endpoint is temporarily returning dummy data",
		Data: map[string]interface{}{
			"realized_pnl":   0.0,
			"unrealized_pnl": 0.0,
			"total_pnl":      0.0,
			"last_updated":   time.Now().Format(time.RFC3339),
		},
	})
}

// RegisterPnLRoutes registers all P&L related routes
func RegisterPnLRoutes(router *mux.Router) {
	// Add the new P&L summary endpoint
	router.HandleFunc("/api/pnl/summary", HandleGetLatestPnLSummary).Methods("GET", "OPTIONS")
}
