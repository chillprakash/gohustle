package api

import (
	"net/http"
)

// SetupRoutes initializes all API routes
func (s *APIServer) setupRoutes() {
	// Remove the global CORS middleware since we're applying it at the server level
	s.router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Log all incoming requests for debugging
			s.log.Debug("Request received", map[string]interface{}{
				"method": r.Method,
				"path":   r.URL.Path,
			})
			next.ServeHTTP(w, r)
		})
	})

	// Create public router (no auth required)
	publicRouter := s.router.PathPrefix("/api").Subrouter()
	publicRouter.HandleFunc("/auth/login", s.handleLogin).Methods("POST")

	// Create authenticated router
	authenticatedRouter := s.router.PathPrefix("/api").Subrouter()
	// Apply Auth middleware to the authenticated router
	// (CORS is already applied at the router level)
	authenticatedRouter.Use(s.AuthMiddleware)
	// Auth routes (no authentication required)
	authenticatedRouter.HandleFunc("/auth/logout", s.handleLogout).Methods("POST")
	authenticatedRouter.HandleFunc("/auth/check", s.handleAuthCheck).Methods("GET")
	// Health check endpoint
	authenticatedRouter.HandleFunc("/health", s.handleHealthCheck).Methods("GET")

	authenticatedRouter.HandleFunc("/orders/place", s.handlePlaceOrder).Methods("POST")

	// Expiries endpoint
	authenticatedRouter.HandleFunc("/expiries", s.handleGetExpiries).Methods("GET", "OPTIONS")

	// Option chain endpoint
	authenticatedRouter.HandleFunc("/option-chain", s.handleGetOptionChain).Methods("GET", "OPTIONS")

	// Position routes
	positionRouter := authenticatedRouter.PathPrefix("/positions").Subrouter()
	positionRouter.HandleFunc("", s.handleGetPositions).Methods("GET", "OPTIONS")
	positionRouter.HandleFunc("/exit", s.handleExitAllPositions).Methods("POST", "OPTIONS")

	// P&L endpoints
	authenticatedRouter.HandleFunc("/pnl", s.handleGetPnL).Methods("GET", "OPTIONS")
	authenticatedRouter.HandleFunc("/pnl/params", s.handleGetPnLParams).Methods("GET", "OPTIONS")
	authenticatedRouter.HandleFunc("/pnl/params", s.handleUpdatePnLParams).Methods("POST", "OPTIONS")
	authenticatedRouter.HandleFunc("/pnl/summary", HandleGetLatestPnLSummary).Methods("GET", "OPTIONS")

	// Time series metrics endpoint
	authenticatedRouter.HandleFunc("/metrics", s.handleGetTimeSeriesMetrics).Methods("GET", "OPTIONS")

	authenticatedRouter.HandleFunc("/indices", s.handleListIndices).Methods("GET", "OPTIONS")
	authenticatedRouter.HandleFunc("/instruments", s.handleListInstruments).Methods("GET", "OPTIONS")
	authenticatedRouter.HandleFunc("/status", s.handleGetMarketStatus).Methods("GET", "OPTIONS")

	// Data export endpoints
	authenticatedRouter.HandleFunc("/export/wal-to-parquet", s.handleWalToParquet).Methods("POST", "OPTIONS")

	// Tick data export endpoints
	authenticatedRouter.HandleFunc("/ticks/dates", handleGetAvailableTickDates).Methods("GET", "OPTIONS")
	authenticatedRouter.HandleFunc("/ticks/export", handleExportTickData).Methods("POST", "OPTIONS")
	authenticatedRouter.HandleFunc("/ticks/delete", handleDeleteTickData).Methods("POST", "OPTIONS")
	authenticatedRouter.HandleFunc("/ticks/files", handleListExportedFiles).Methods("GET", "OPTIONS")
	authenticatedRouter.HandleFunc("/ticks/samples", handleGetTickSamples).Methods("POST", "OPTIONS")

	// Archive management endpoints
	authenticatedRouter.HandleFunc("/archive/jobs", handleGetArchiveJobs).Methods("GET", "OPTIONS")
	authenticatedRouter.HandleFunc("/archive/retry", handleRetryArchiveJob).Methods("POST", "OPTIONS")
	authenticatedRouter.HandleFunc("/archive/run", handleRunArchiveJob).Methods("POST", "OPTIONS")
	authenticatedRouter.HandleFunc("/archive/consolidate", handleRunConsolidationJob).Methods("POST", "OPTIONS")
	authenticatedRouter.HandleFunc("/archive/files", handleListArchiveFiles).Methods("GET", "OPTIONS")
	authenticatedRouter.HandleFunc("/tick-data/dashboard", handleGetTickDataDashboard).Methods("GET", "OPTIONS")

	// General endpoint
	authenticatedRouter.HandleFunc("/general", s.handleGeneral).Methods("GET", "OPTIONS")
}
