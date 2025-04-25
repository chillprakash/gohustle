// In api/api.go
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"gohustle/cache"
	"gohustle/db"
	"gohustle/filestore"
	"gohustle/logger"
	"gohustle/optionchain"
	"gohustle/types"
	"gohustle/zerodha"

	"github.com/gorilla/mux"
)

// Server represents the HTTP API server
type Server struct {
	router       *mux.Router
	server       *http.Server
	port         string
	ctx          context.Context
	cancel       context.CancelFunc
	log          *logger.Logger
	tickStore    *filestore.TickStore
	parquetStore *filestore.ParquetStore
}

// Response is a standard API response structure
type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// GeneralResponse represents the response structure for general market information
type GeneralResponse struct {
	LotSizes map[string]int `json:"lot_sizes"`
}

// LotSizes contains the standard lot sizes for each index
var LotSizes = map[string]int{
	"NIFTY":     75,
	"BANKNIFTY": 30,
	"SENSEX":    20,
}

var (
	instance *Server
	once     sync.Once
	mu       sync.RWMutex
)

// GetAPIServer returns the singleton instance of the API server
func GetAPIServer() *Server {
	mu.RLock()
	if instance != nil {
		mu.RUnlock()
		return instance
	}
	mu.RUnlock()

	mu.Lock()
	defer mu.Unlock()

	once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		instance = &Server{
			router:       mux.NewRouter(),
			port:         "8080", // Default port, can be changed
			ctx:          ctx,
			cancel:       cancel,
			log:          logger.L(),
			tickStore:    filestore.GetTickStore(),
			parquetStore: filestore.GetParquetStore(),
		}
		// Initialize routes
		instance.setupRoutes()
	})

	return instance
}

// SetPort allows changing the default port
func (s *Server) SetPort(port string) {
	s.port = port
}

// Start starts the HTTP server
func (s *Server) Start(ctx context.Context) error {
	// Create HTTP server
	s.server = &http.Server{
		Addr:         ":" + s.port,
		Handler:      s.router,
		ReadTimeout:  2400 * time.Second, // Reduced from 300s
		WriteTimeout: 2400 * time.Second, // Reduced from 300s
		IdleTimeout:  2400 * time.Second, // Reduced from 600s
	}

	// Start the server in a goroutine
	go func() {
		s.log.Info("Starting API server", map[string]interface{}{
			"port": s.port,
			"timeouts": map[string]string{
				"read":  "30s",
				"write": "30s",
				"idle":  "120s",
			},
		})

		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("API server error", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}()

	// Wait for context cancellation
	go func() {
		<-ctx.Done()
		s.Shutdown()
	}()

	return nil
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown() error {
	s.log.Info("Shutting down API server", nil)

	// Create a timeout context for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if s.server != nil {
		if err := s.server.Shutdown(ctx); err != nil {
			return fmt.Errorf("server shutdown failed: %w", err)
		}
	}

	if s.cancel != nil {
		s.cancel()
	}

	return nil
}

// WalToParquetRequest holds parameters for WAL to Parquet conversion
type WalToParquetRequest struct {
	IndexName    string `json:"index_name"`
	Date         string `json:"date"` // Format: YYYY-MM-DD
	ParquetPath  string `json:"parquet_path"`
	Compression  string `json:"compression,omitempty"`    // Optional
	RowGroupSize int    `json:"row_group_size,omitempty"` // Optional
}

// OptionChainResponse represents the structure for each option in the chain
type OptionData struct {
	InstrumentToken string  `json:"instrument_token"`
	LTP             float64 `json:"ltp"`
	OI              int64   `json:"oi"`
	Volume          int64   `json:"volume"`
	VWAP            float64 `json:"vwap"`
	Change          float64 `json:"change"`
}

type OptionChainItem struct {
	Strike    float64     `json:"strike"`
	CE        *OptionData `json:"CE"`
	PE        *OptionData `json:"PE"`
	CEPETotal float64     `json:"ce_pe_total"`
	IsATM     bool        `json:"is_atm"`
}

// TimeSeriesMetricsRequest represents the request parameters for fetching time series metrics
type TimeSeriesMetricsRequest struct {
	Index              string `json:"index"`                          // Required: Index name (e.g., "BANKNIFTY")
	Interval           string `json:"interval,omitempty"`             // Required for historical mode: 5s, 10s, 20s, 30s
	Count              int    `json:"count,omitempty"`                // Required for historical mode: Number of points to fetch
	Mode               string `json:"mode,omitempty"`                 // Optional: "historical" or "realtime", defaults to "historical"
	SinceTimestamp     string `json:"since_timestamp,omitempty"`      // Optional: Get data after this timestamp
	UntilTimestamp     string `json:"until_timestamp,omitempty"`      // Optional: Get data before this timestamp
	LastKnownTimestamp string `json:"last_known_timestamp,omitempty"` // Optional: Only get points newer than this (for realtime mode)
}

// ValidIntervals contains the list of valid interval values and their durations
var ValidIntervals = map[string]time.Duration{
	"5s":  5 * time.Second,
	"10s": 10 * time.Second,
	"20s": 20 * time.Second,
	"30s": 30 * time.Second,
}

// ValidDurations contains the list of valid duration values and their time.Duration equivalents
var ValidDurations = map[string]time.Duration{
	"5m":  5 * time.Minute,
	"10m": 10 * time.Minute,
	"15m": 15 * time.Minute,
	"30m": 30 * time.Minute,
}

// TimeSeriesMetricsResponse represents a single data point in the time series
type TimeSeriesMetricsResponse struct {
	Timestamp       int64   `json:"timestamp"`
	UnderlyingPrice float64 `json:"underlying_price"`
	SyntheticFuture float64 `json:"synthetic_future"`
	LowestStraddle  float64 `json:"straddle"`
	ATMStrike       float64 `json:"atm_strike"`
}

// CORSMiddleware adds CORS headers to responses
func (s *Server) CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*") // Allow all origins
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Max-Age", "3600")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// setupRoutes initializes all API routes
func (s *Server) setupRoutes() {
	// Add CORS middleware to the router
	s.router.Use(s.CORSMiddleware)

	// Auth routes (no authentication required)
	s.router.HandleFunc("/api/auth/login", s.handleLogin).Methods("POST", "OPTIONS")
	s.router.HandleFunc("/api/auth/logout", s.handleLogout).Methods("POST", "OPTIONS")
	s.router.HandleFunc("/api/auth/check", s.handleAuthCheck).Methods("GET", "OPTIONS") // Debug endpoint

	// Create authenticated router
	authenticatedRouter := s.router.PathPrefix("/api").Subrouter()
	authenticatedRouter.Use(s.AuthMiddleware)

	// Health check endpoint
	authenticatedRouter.HandleFunc("/health", s.handleHealthCheck).Methods("GET", "OPTIONS")

	// Expiries endpoint
	authenticatedRouter.HandleFunc("/expiries", s.handleGetExpiries).Methods("GET", "OPTIONS")

	// Option chain endpoint
	authenticatedRouter.HandleFunc("/option-chain", s.handleGetOptionChain).Methods("GET", "OPTIONS")

	// Positions endpoint
	authenticatedRouter.HandleFunc("/positions", s.handleGetPositions).Methods("GET", "OPTIONS")

	// Time series metrics endpoint
	authenticatedRouter.HandleFunc("/metrics", s.handleGetTimeSeriesMetrics).Methods("GET", "OPTIONS")

	// API version 1 routes
	v1 := authenticatedRouter.PathPrefix("/v1").Subrouter()

	// Market data endpoints
	market := v1.PathPrefix("/market").Subrouter()
	market.HandleFunc("/indices", s.handleListIndices).Methods("GET", "OPTIONS")
	market.HandleFunc("/instruments", s.handleListInstruments).Methods("GET", "OPTIONS")
	market.HandleFunc("/status", s.handleGetMarketStatus).Methods("GET", "OPTIONS")

	// Data export endpoints
	export := v1.PathPrefix("/export").Subrouter()
	export.HandleFunc("/wal-to-parquet", s.handleWalToParquet).Methods("POST", "OPTIONS")

	// General endpoint
	authenticatedRouter.HandleFunc("/general", s.handleGeneral).Methods("GET", "OPTIONS")
}

// Health check handler
func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	resp := Response{
		Success: true,
		Message: "API server is running",
		Data: map[string]interface{}{
			"status": "ok",
			"time":   time.Now().Format(time.RFC3339),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleListIndices(w http.ResponseWriter, r *http.Request) {
	// Implementation to be added
	// This would list available indices from your application
	resp := Response{
		Success: true,
		Message: "List of available indices",
		Data:    []string{}, // Placeholder for actual indices
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleListInstruments(w http.ResponseWriter, r *http.Request) {
	// Implementation to be added
	// This would list available instruments from your application
	resp := Response{
		Success: true,
		Message: "List of available instruments",
		Data:    []string{}, // Placeholder for actual instruments
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleGetMarketStatus(w http.ResponseWriter, r *http.Request) {
	// Implementation to be added
	// This would return the current market status
	resp := Response{
		Success: true,
		Message: "Market status",
		Data:    "Market is open", // Placeholder for actual market status
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// Helper functions for HTTP responses
func sendErrorResponse(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": message,
	})
}

func sendJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// Add after imports
func (s *Server) readRawParquetSamples(filePath string, numSamples int) error {
	s.log.Info("Reading parquet file samples", map[string]interface{}{
		"file":        filePath,
		"num_samples": numSamples,
	})

	// Use the existing parquet store to read samples
	store := filestore.GetParquetStore()
	samples, err := store.ReadSamples(filePath, numSamples)
	if err != nil {
		return fmt.Errorf("failed to read parquet samples: %w", err)
	}

	// Log each sample
	for i, sample := range samples {
		s.log.Info("Sample record", map[string]interface{}{
			"index":         i + 1,
			"timestamp":     sample.ExchangeTimestamp.Time.Unix(),
			"instrument":    sample.InstrumentToken,
			"last_price":    sample.LastPrice,
			"volume":        sample.VolumeTraded,
			"received_time": sample.ExchangeTimestamp.Time.Unix(),
		})
	}

	return nil
}

// Update handleWalToParquet to use the method
func (s *Server) handleWalToParquet(w http.ResponseWriter, r *http.Request) {
	// Parse request
	var req WalToParquetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.IndexName == "" {
		sendErrorResponse(w, "index_name is required", http.StatusBadRequest)
		return
	}
	if req.Date == "" {
		sendErrorResponse(w, "date is required", http.StatusBadRequest)
		return
	}

	// Create output directory if not specified
	if req.ParquetPath == "" {
		req.ParquetPath = filepath.Join(filestore.DefaultParquetDir, fmt.Sprintf("%s_%s.parquet", req.IndexName, req.Date))
	}

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(req.ParquetPath), 0755); err != nil {
		sendErrorResponse(w, fmt.Sprintf("failed to create output directory: %v", err), http.StatusInternalServerError)
		return
	}

	// Create Arrow converter
	converter := filestore.NewArrowConverter()

	// Start conversion with timeout context
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	// Create error channel for monitoring conversion
	errChan := make(chan error, 1)
	go func() {
		errChan <- converter.ConvertWALToParquet(req.IndexName, req.Date, req.ParquetPath)
	}()

	// Wait for completion or timeout
	select {
	case err := <-errChan:
		if err != nil {
			s.log.Error("Failed to convert WAL to Parquet", map[string]interface{}{
				"error": err.Error(),
				"index": req.IndexName,
				"date":  req.Date,
			})
			sendErrorResponse(w, fmt.Sprintf("conversion failed: %v", err), http.StatusInternalServerError)
			return
		}
	case <-ctx.Done():
		sendErrorResponse(w, "operation timed out after 10 minutes", http.StatusGatewayTimeout)
		return
	}

	// After successful conversion, read and print raw samples
	if err := s.readRawParquetSamples(req.ParquetPath, 5); err != nil {
		s.log.Error("Failed to read raw samples", map[string]interface{}{
			"error": err.Error(),
			"path":  req.ParquetPath,
		})
	}

	// Send success response
	resp := Response{
		Success: true,
		Message: "Successfully converted WAL to Parquet using Arrow",
		Data: map[string]interface{}{
			"output_file": req.ParquetPath,
		},
	}

	s.log.Info("Successfully converted WAL to Parquet", map[string]interface{}{
		"index":       req.IndexName,
		"date":        req.Date,
		"output_file": req.ParquetPath,
	})

	sendJSONResponse(w, resp)
}

// handleGetExpiries returns the expiry dates for all indices from in-memory cache
func (s *Server) handleGetExpiries(w http.ResponseWriter, r *http.Request) {
	cache := cache.GetInMemoryCacheInstance()

	// Get list of instruments from cache
	instrumentsKey := "instrument:expiries:list"
	instrumentsValue, exists := cache.Get(instrumentsKey)
	if !exists {
		sendErrorResponse(w, "No instruments found in cache", http.StatusNotFound)
		return
	}

	instruments, ok := instrumentsValue.([]string)
	if !ok {
		sendErrorResponse(w, "Invalid data type for instruments in cache", http.StatusInternalServerError)
		return
	}

	// Create response map
	expiriesMap := make(map[string][]string)

	// Get expiries for each instrument
	for _, instrument := range instruments {
		key := fmt.Sprintf("instrument:expiries:%s", instrument)
		value, exists := cache.Get(key)
		if !exists {
			s.log.Debug("No expiries found for instrument", map[string]interface{}{
				"instrument": instrument,
			})
			continue
		}

		dates, ok := value.([]string)
		if !ok {
			s.log.Error("Invalid data type for expiries in cache", map[string]interface{}{
				"instrument": instrument,
			})
			continue
		}

		// Add to response map
		expiriesMap[instrument] = dates
	}

	if len(expiriesMap) == 0 {
		sendErrorResponse(w, "No expiry dates found", http.StatusNotFound)
		return
	}

	resp := Response{
		Success: true,
		Message: "Expiry dates retrieved successfully",
		Data:    expiriesMap,
	}

	sendJSONResponse(w, resp)
}

// handleGetOptionChain returns the option chain for a specific index and expiry
func (s *Server) handleGetOptionChain(w http.ResponseWriter, r *http.Request) {
	// Get query parameters
	index := r.URL.Query().Get("index")
	expiry := r.URL.Query().Get("expiry")
	strikes_count := r.URL.Query().Get("strikes")
	force_calculate := r.URL.Query().Get("force") == "true"

	// Validate required parameters
	if index == "" {
		sendErrorResponse(w, "Missing required parameter: index", http.StatusBadRequest)
		return
	}
	if expiry == "" {
		sendErrorResponse(w, "Missing required parameter: expiry", http.StatusBadRequest)
		return
	}

	// Parse strikes_count (default to 5)
	numStrikes := 5
	if strikes_count != "" {
		var err error
		numStrikes, err = strconv.Atoi(strikes_count)
		if err != nil {
			sendErrorResponse(w, "Invalid value for strikes parameter. Must be an integer.", http.StatusBadRequest)
			return
		}
	}

	// Validate expiry date format
	if _, err := time.Parse("2006-01-02", expiry); err != nil {
		sendErrorResponse(w, "Invalid expiry date format. Use YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	optionChainMgr := optionchain.GetOptionChainManager()

	var response *optionchain.OptionChainResponse
	var err error

	// Try to get from in-memory first unless force calculate is true
	if !force_calculate {
		response = optionChainMgr.GetLatestChain(index, expiry)
		if response != nil {
			// Check if data is fresh (less than 2 seconds old)
			if time.Since(time.Unix(0, response.Timestamp)) < 2*time.Second {
				resp := Response{
					Success: true,
					Message: "Option chain retrieved from memory",
					Data:    response,
				}
				sendJSONResponse(w, resp)
				return
			}
		}
	}

	// If we don't have fresh data, calculate new
	response, err = optionChainMgr.CalculateOptionChain(r.Context(), index, expiry, numStrikes)
	if err != nil {
		sendErrorResponse(w, "Failed to calculate option chain", http.StatusInternalServerError)
		return
	}

	resp := Response{
		Success: true,
		Message: "Option chain calculated successfully",
		Data:    response,
	}

	sendJSONResponse(w, resp)
}

// handleGetPositions returns detailed analysis of all positions
func (s *Server) handleGetPositions(w http.ResponseWriter, r *http.Request) {
	pm := zerodha.GetPositionManager()
	if pm == nil {
		sendErrorResponse(w, "Position manager not initialized", http.StatusInternalServerError)
		return
	}

	analysis, err := pm.GetPositionAnalysis(r.Context())
	if err != nil {
		sendErrorResponse(w, "Failed to get position analysis", http.StatusInternalServerError)
		return
	}

	resp := Response{
		Success: true,
		Data:    analysis,
	}

	sendJSONResponse(w, resp)
}

// Helper function to convert Redis Z member to float64
func convertRedisZMemberToFloat64(member interface{}) float64 {
	switch v := member.(type) {
	case float64:
		return v
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	case int64:
		return float64(v)
	case int:
		return float64(v)
	}
	return 0
}

// handleGetTimeSeriesMetrics handles requests for time series metrics
func (s *Server) handleGetTimeSeriesMetrics(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters into request struct
	req := TimeSeriesMetricsRequest{
		Index:              r.URL.Query().Get("index"),
		Interval:           r.URL.Query().Get("interval"),
		Mode:               r.URL.Query().Get("mode"),
		SinceTimestamp:     r.URL.Query().Get("since_timestamp"),
		UntilTimestamp:     r.URL.Query().Get("until_timestamp"),
		LastKnownTimestamp: r.URL.Query().Get("last_known_timestamp"),
	}

	// Set default mode if not specified
	if req.Mode == "" {
		req.Mode = "historical"
	}

	// Validate index (required for all modes)
	if req.Index == "" {
		sendErrorResponse(w, "Missing required parameter: index", http.StatusBadRequest)
		return
	}

	// Get metrics store instance
	metricsStore, err := db.GetMetricsStore()
	if err != nil {
		s.log.Error("Failed to get metrics store", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Failed to get metrics store", http.StatusInternalServerError)
		return
	}

	var metrics []*types.MetricsData

	switch req.Mode {
	case "realtime":
		// Handle real-time mode
		if req.LastKnownTimestamp != "" {
			// Parse last known timestamp
			lastKnown, err := strconv.ParseInt(req.LastKnownTimestamp, 10, 64)
			if err != nil {
				sendErrorResponse(w, "Invalid last_known_timestamp format", http.StatusBadRequest)
				return
			}
			// Get only newer points
			metrics, err = metricsStore.GetMetricsAfterTimestamp(req.Index, lastKnown)
		} else {
			// Get latest point only
			metrics, err = metricsStore.GetMetrics(req.Index, "5s", 1) // Use shortest interval for real-time
		}

	case "historical":
		// Validate historical mode parameters
		if req.Interval == "" {
			sendErrorResponse(w, "Missing required parameter for historical mode: interval", http.StatusBadRequest)
			return
		}

		// Validate interval
		if _, ok := ValidIntervals[req.Interval]; !ok {
			sendErrorResponse(w, "Invalid interval. Valid values are: 5s, 10s, 20s, 30s", http.StatusBadRequest)
			return
		}

		// Parse count if provided
		if countStr := r.URL.Query().Get("count"); countStr != "" {
			count, err := strconv.Atoi(countStr)
			if err != nil {
				sendErrorResponse(w, "Invalid count parameter. Must be an integer.", http.StatusBadRequest)
				return
			}
			if count <= 0 {
				sendErrorResponse(w, "Count must be greater than 0", http.StatusBadRequest)
				return
			}
			if count > 1000 {
				sendErrorResponse(w, "Count cannot exceed 1000", http.StatusBadRequest)
				return
			}
			req.Count = count
		}

		// Handle time range if provided
		var startTime, endTime int64
		if req.SinceTimestamp != "" {
			startTime, err = strconv.ParseInt(req.SinceTimestamp, 10, 64)
			if err != nil {
				sendErrorResponse(w, "Invalid since_timestamp format", http.StatusBadRequest)
				return
			}
		}
		if req.UntilTimestamp != "" {
			endTime, err = strconv.ParseInt(req.UntilTimestamp, 10, 64)
			if err != nil {
				sendErrorResponse(w, "Invalid until_timestamp format", http.StatusBadRequest)
				return
			}
		}

		if startTime > 0 && endTime > 0 {
			metrics, err = metricsStore.GetMetricsInTimeRange(req.Index, req.Interval, startTime, endTime)
		} else if req.Count > 0 {
			metrics, err = metricsStore.GetMetrics(req.Index, req.Interval, req.Count)
		} else {
			// Default to last 50 points if no time range or count specified
			metrics, err = metricsStore.GetMetrics(req.Index, req.Interval, 50)
		}

	default:
		sendErrorResponse(w, "Invalid mode. Must be 'historical' or 'realtime'", http.StatusBadRequest)
		return
	}

	if err != nil {
		s.log.Error("Failed to fetch metrics", map[string]interface{}{
			"error": err.Error(),
			"mode":  req.Mode,
			"index": req.Index,
		})
		sendErrorResponse(w, "Failed to fetch metrics", http.StatusInternalServerError)
		return
	}

	// Convert to response format
	validMetrics := make([]*TimeSeriesMetricsResponse, 0, len(metrics))
	for _, metric := range metrics {
		validMetrics = append(validMetrics, &TimeSeriesMetricsResponse{
			Timestamp:       metric.Timestamp,
			UnderlyingPrice: metric.UnderlyingPrice,
			SyntheticFuture: metric.SyntheticFuture,
			LowestStraddle:  metric.LowestStraddle,
			ATMStrike:       metric.ATMStrike,
		})
	}

	resp := Response{
		Success: true,
		Message: "Time series metrics retrieved successfully",
		Data: map[string]interface{}{
			"mode":            req.Mode,
			"index":           req.Index,
			"interval":        req.Interval,
			"requested_count": req.Count,
			"returned_count":  len(validMetrics),
			"metrics":         validMetrics,
		},
	}

	sendJSONResponse(w, resp)
}

// handleGeneral handles the /general endpoint that returns general market information
func (s *Server) handleGeneral(w http.ResponseWriter, r *http.Request) {
	response := Response{
		Success: true,
		Data: GeneralResponse{
			LotSizes: LotSizes,
		},
	}

	sendJSONResponse(w, response)
}
