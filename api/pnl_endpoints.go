package api

import (
	"encoding/json"
	"net/http"

	"gohustle/db"
	"gohustle/logger"
)

// GetLatestPnLSummaryHandler returns the latest P&L summary for both real and paper trading
func GetLatestPnLSummaryHandler(w http.ResponseWriter, r *http.Request) {
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

	// Set content type header
	w.Header().Set("Content-Type", "application/json")

	// Send response
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"data":    summary,
	})
}
