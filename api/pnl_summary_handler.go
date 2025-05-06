package api

import (
	"net/http"

	"gohustle/db"
	"gohustle/logger"

	"github.com/gorilla/mux"
)

// HandleGetLatestPnLSummary returns the latest P&L summary for both real and paper trading
func HandleGetLatestPnLSummary(w http.ResponseWriter, r *http.Request) {
	log := logger.L()
	ctx := r.Context()

	// Get database connection
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		log.Error("Failed to get database connection", nil)
		sendErrorResponse(w, "Database connection failed", http.StatusInternalServerError)
		return
	}

	// Get the latest P&L summary
	summary, err := timescaleDB.GetLatestStrategyPnLSummary(ctx)
	if err != nil {
		log.Error("Failed to get latest P&L summary", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Failed to get P&L summary", http.StatusInternalServerError)
		return
	}

	sendJSONResponse(w, Response{
		Success: true,
		Message: "Latest P&L summary retrieved successfully",
		Data:    summary,
	})
}

// RegisterPnLRoutes registers all P&L related routes
func RegisterPnLRoutes(router *mux.Router) {
	// Add the new P&L summary endpoint
	router.HandleFunc("/api/pnl/summary", HandleGetLatestPnLSummary).Methods("GET", "OPTIONS")
}
