// In api/api.go
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gohustle/appparameters"
	"gohustle/cache"
	"gohustle/core"
	"gohustle/filestore"
	"gohustle/logger"
	"gohustle/optionchain"
	"gohustle/utils"
	"gohustle/zerodha"

	"github.com/gorilla/mux"
	"github.com/redis/go-redis/v9"
)

// APIServer represents the HTTP API server
type APIServer struct {
	router          *mux.Router
	server          *http.Server
	port            string
	ctx             context.Context
	cancel          context.CancelFunc
	log             *logger.Logger
	orderManager    *zerodha.OrderManager
	positionManager *zerodha.PositionManager
}

// GetAPIServer returns the singleton instance of the API server
func GetAPIServer() *APIServer {
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
		instance = &APIServer{
			router:          mux.NewRouter(),
			port:            "8080", // Default port, can be changed
			ctx:             ctx,
			cancel:          cancel,
			log:             logger.L(),
			orderManager:    zerodha.GetOrderManager(),
			positionManager: zerodha.GetPositionManager(),
		}
		// Initialize routes
		instance.setupRoutes()
	})
	return instance
}

// mapToPlaceOrderRequest converts an HTTP request to a PlaceOrderAPIRequest
func (s *APIServer) mapToPlaceOrderRequest(request *http.Request) (*zerodha.PlaceOrderRequest, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading request body: %w", err)
	}

	var orderReq zerodha.PlaceOrderRequest
	if err := json.Unmarshal(body, &orderReq); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	return &orderReq, nil
}

func (s *APIServer) handlePlaceOrder(w http.ResponseWriter, request *http.Request) {
	orderReq, err := s.mapToPlaceOrderRequest(request)
	if err != nil {
		s.log.Error("Failed to parse order request", map[string]interface{}{"error": err.Error()})
		sendErrorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate modify away/closer orders have either quantity or percentage
	if orderReq.OrderType == zerodha.OrderTypeModifyAway || orderReq.OrderType == zerodha.OrderTypeModifyCloser {
		if orderReq.Quantity == 0 && orderReq.Percentage == 0 {
			errMsg := "modify order requires either quantity or percentage to be specified"
			s.log.Error("Invalid modify order request", map[string]interface{}{"error": errMsg})
			sendErrorResponse(w, errMsg, http.StatusBadRequest)
			return
		}
	}
	s.log.Info("Order placement request", map[string]interface{}{"order": orderReq})
	resp, err := zerodha.PlaceOrder(*orderReq)
	if err != nil {
		s.log.Error("Order placement failed", map[string]interface{}{"error": err.Error()})
		sendErrorResponse(w, "Order placement failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	logger.L().Info("Order placed successfully", map[string]interface{}{"order": resp})
	// Return the response to the client
	sendJSONResponse(w, resp)
}

// PnLParams represents P&L parameters
type PnLParams struct {
	ExitPnL   float64 `json:"exit_pnl"`
	TargetPnL float64 `json:"target_pnl"`
}

// PnLParamsResponse represents the response for P&L parameters
type PnLParamsResponse struct {
	Success bool      `json:"success"`
	Data    PnLParams `json:"data"`
	Error   string    `json:"error,omitempty"`
}

// UpdatePnLParamsRequest represents the request to update P&L parameters
type UpdatePnLParamsRequest struct {
	ExitPnL   *float64 `json:"exit_pnl,omitempty"`
	TargetPnL *float64 `json:"target_pnl,omitempty"`
}

// GeneralResponse represents the response structure for general market information
type GeneralResponse struct {
	LotSizes  map[string]int             `json:"lot_sizes"`
	IndexInfo map[string]IndexInfoStruct `json:"index_info,omitempty"`
}

// IndexInfoStruct contains metadata about an index
type IndexInfoStruct struct {
	IndexNumber int    `json:"index_number"`
	DisplayName string `json:"display_name"`
	Enabled     bool   `json:"enabled"`
}

// LotSizes contains the standard lot sizes for each index
var LotSizes = map[string]int{
	"NIFTY":     75,
	"BANKNIFTY": 30,
	"SENSEX":    20,
}

var (
	instance *APIServer
	once     sync.Once
	mu       sync.RWMutex
)

// SetPort allows changing the default port
func (s *APIServer) SetPort(port string) {
	s.port = port
}

// Start starts the HTTP server
func (s *APIServer) Start(ctx context.Context) error {
	// Setup all routes
	s.setupRoutes()

	// Create HTTP server with CORS middleware
	s.server = &http.Server{
		Addr:         ":" + s.port,
		Handler:      s.CORSMiddleware(s.router),
		ReadTimeout:  30 * time.Second,  // Standard timeout for reading request
		WriteTimeout: 30 * time.Second,  // Standard timeout for writing response
		IdleTimeout:  120 * time.Second, // Longer timeout for idle connections
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
func (s *APIServer) Shutdown() error {
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

// SettingsResponse contains settings data returned by the API
type SettingsResponse struct {
	LimitOrder string `json:"limit_order"`
}

// UpdateSettingsRequest contains settings data to be updated
type UpdateSettingsRequest struct {
	LimitOrder string `json:"limit_order"`
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
func (s *APIServer) CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Log the incoming request for debugging
		s.log.Debug("CORS Middleware", map[string]interface{}{
			"method":   r.Method,
			"path":     r.URL.Path,
			"origin":   r.Header.Get("Origin"),
			"endpoint": r.URL.String(),
		})

		// Allow requests from any origin
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// Allow specific HTTP methods
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")

		// Allow specific headers
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")

		// Allow credentials
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Call the next handler
		next.ServeHTTP(w, r)
	})
}

// Health check handler
func (s *APIServer) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	resp := Response{
		Success: true,
		Message: "API server is running",
		Data: map[string]interface{}{
			"status": "ok",
			"time":   utils.NowIST().Format(time.RFC3339),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (s *APIServer) handleListIndices(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleListInstruments(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleGetMarketStatus(w http.ResponseWriter, r *http.Request) {
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
func (s *APIServer) readRawParquetSamples(filePath string, numSamples int) error {
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

// handleGetExpiries returns the expiry dates for all indices from Redis cache
func (s *APIServer) handleGetExpiries(w http.ResponseWriter, r *http.Request) {
	// Get the Redis cache instance
	cacheMeta, err := cache.GetCacheMetaInstance()
	if err != nil {
		s.log.Error("Failed to get cache instance", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Failed to get cache instance", http.StatusInternalServerError)
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer cancel()

	// Get the expiry map from Redis
	expiryMap, err := cacheMeta.GetExpiryMap(ctx)
	if err != nil {
		s.log.Error("Failed to get expiry map from Redis", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Failed to get expiry data", http.StatusInternalServerError)
		return
	}

	// Convert the time.Time values to formatted strings for the response
	responseMap := make(map[string][]string)
	for index, dates := range expiryMap {
		formattedDates := make([]string, 0, len(dates))
		for _, date := range dates {
			formattedDates = append(formattedDates, utils.FormatKiteDate(date))
		}
		responseMap[index] = formattedDates
	}

	// Check if we have any data
	if len(responseMap) == 0 {
		// Try to get data for all configured indices if the map is empty
		allIndices := core.GetIndices().GetAllIndices()
		for _, index := range allIndices {
			expiry, err := cacheMeta.GetNearestExpiry(ctx, index.NameInOptions)
			if err == nil && expiry != "" {
				responseMap[index.NameInOptions] = []string{expiry}
			}
		}

		// If still empty, return error
		if len(responseMap) == 0 {
			sendErrorResponse(w, "No expiry dates found", http.StatusNotFound)
			return
		}
	}

	resp := Response{
		Success: true,
		Message: "Expiry dates retrieved successfully",
		Data:    responseMap,
	}

	sendJSONResponse(w, resp)
}

// handleGetOptionChain returns the option chain for a specific index and expiry
func (s *APIServer) handleGetOptionChain(w http.ResponseWriter, r *http.Request) {
	// Get query parameters
	index := r.URL.Query().Get("index")
	expiry := r.URL.Query().Get("expiry")
	strikes_count := r.URL.Query().Get("strikes")

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
	// This represents the number of strikes to include on EACH SIDE of the ATM strike
	// So the total number of strikes returned will be approximately (2*numStrikes + 1)
	numStrikes := 5
	if strikes_count != "" {
		var err error
		numStrikes, err = strconv.Atoi(strikes_count)
		if err != nil {
			sendErrorResponse(w, "Invalid value for strikes parameter. Must be an integer.", http.StatusBadRequest)
			return
		}
	}

	// Validate expiry date format (DD-MM-YYYY as defined in utils.KiteDateFormat)
	if _, err := utils.ParseKiteDate(expiry); err != nil {
		sendErrorResponse(w, fmt.Sprintf("Invalid expiry date format. Use DD-MM-YYYY (e.g., %s)", utils.GetCurrentKiteDate()), http.StatusBadRequest)
		return
	}

	optionChainMgr := optionchain.GetOptionChainManager()

	var response *optionchain.OptionChainResponse
	var err error

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

// ExitAllPositionsRequest represents the request to exit all positions
type ExitAllPositionsRequest struct {
	// PositionType must be either "paper" or "real"
	PositionType string `json:"positionType"`
}

// handleGetPositions returns detailed analysis of positions
// Query parameters:
// - filter: "all" (default), "paper", or "real"
func (s *APIServer) handleGetPositions(w http.ResponseWriter, r *http.Request) {
	pm := zerodha.GetPositionManager()
	if pm == nil {
		sendErrorResponse(w, "Position manager not initialized", http.StatusInternalServerError)
		return
	}

	analysis, err := pm.GetPositionAnalysis(r.Context())
	if err != nil {
		sendErrorResponse(w, "Failed to get position analysis: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp := Response{
		Success: true,
		Data:    analysis,
	}

	sendJSONResponse(w, resp)
}

// handleExitAllPositions exits all positions of the specified type (paper/real)
func (s *APIServer) handleExitAllPositions(w http.ResponseWriter, r *http.Request) {
	s.log.Info("Handling exit all positions request")

	// Set CORS headers for all responses
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	// Handle CORS preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		sendErrorResponse(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		sendErrorResponse(w, "Error reading request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Log the raw request body for debugging
	s.log.Debug("Exit positions request body", map[string]interface{}{
		"body": string(body),
	})

	// Validate JSON
	if len(body) == 0 {
		sendErrorResponse(w, "Request body cannot be empty", http.StatusBadRequest)
		return
	}

	var req ExitAllPositionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		sendErrorResponse(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.log.Info("Exiting all positions", map[string]interface{}{
		"positionType": req.PositionType,
	})

	// Set content type and send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Successfully processed exit positions request",
	})
	s.log.Info("Successfully processed exit positions request")
}

// handleGetPnLParams returns the current P&L parameters
func (s *APIServer) handleGetPnLParams(w http.ResponseWriter, r *http.Request) {
	// Get parameters with proper error handling
	exitPnL, err := appparameters.GetAppParameterManager().GetParameter(r.Context(), appparameters.AppParamExitPNL)
	if err != nil {
		s.log.Error("Failed to get exit_pnl parameter", map[string]interface{}{"error": err.Error()})
		sendErrorResponse(w, "Failed to retrieve exit P&L parameter", http.StatusInternalServerError)
		return
	}

	targetPnL, err := appparameters.GetAppParameterManager().GetParameter(r.Context(), appparameters.AppParamTargetPNL)
	if err != nil {
		s.log.Error("Failed to get target_pnl parameter", map[string]interface{}{"error": err.Error()})
		sendErrorResponse(w, "Failed to retrieve target P&L parameter", http.StatusInternalServerError)
		return
	}

	// Check if parameters exist
	if exitPnL == nil || targetPnL == nil {
		sendErrorResponse(w, "P&L parameters not found", http.StatusNotFound)
		return
	}

	sendJSONResponse(w, PnLParamsResponse{
		Success: true,
		Data: PnLParams{
			ExitPnL:   utils.StringToFloat64(exitPnL.Value),
			TargetPnL: utils.StringToFloat64(targetPnL.Value),
		},
	})
}

// handleUpdatePnLParams updates the P&L parameters
func (s *APIServer) handleUpdatePnLParams(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req UpdatePnLParamsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Validate at least one parameter is provided
	if req.ExitPnL == nil && req.TargetPnL == nil {
		sendErrorResponse(w, "No parameters provided for update", http.StatusBadRequest)
		return
	}

	// Update exit_pnl if provided
	if req.ExitPnL != nil {
		appparameters.GetAppParameterManager().SetParameter(r.Context(), appparameters.AppParamExitPNL, fmt.Sprintf("%f", *req.ExitPnL))
	}

	// Update target_pnl if provided
	if req.TargetPnL != nil {
		appparameters.GetAppParameterManager().SetParameter(r.Context(), appparameters.AppParamTargetPNL, fmt.Sprintf("%f", *req.TargetPnL))
	}

	// Get updated parameters
	exitPnL, err := appparameters.GetAppParameterManager().GetParameter(r.Context(), appparameters.AppParamExitPNL)
	if err != nil {
		sendErrorResponse(w, fmt.Sprintf("Failed to get exit P&L: %v", err), http.StatusInternalServerError)
		return
	}
	targetPnL, err := appparameters.GetAppParameterManager().GetParameter(r.Context(), appparameters.AppParamTargetPNL)
	if err != nil {
		sendErrorResponse(w, fmt.Sprintf("Failed to get target P&L: %v", err), http.StatusInternalServerError)
		return
	}

	sendJSONResponse(w, PnLParamsResponse{
		Success: true,
		Data: PnLParams{
			ExitPnL:   utils.StringToFloat64(exitPnL.Value),
			TargetPnL: utils.StringToFloat64(targetPnL.Value),
		},
	})
}

func (s *APIServer) handleGetPnL(w http.ResponseWriter, r *http.Request) {
	// pnlManager := zerodha.GetPnLManager()
	// if pnlManager == nil {
	// 	sendErrorResponse(w, "PnL manager not initialized", http.StatusInternalServerError)
	// 	return
	// }
	// pnlSummary, err := pnlManager.CalculatePnL(r.Context())
	// if err != nil {
	// 	s.log.Error("Failed to calculate P&L", map[string]interface{}{
	// 		"error": err.Error(),
	// 	})
	// 	sendErrorResponse(w, "Failed to calculate P&L", http.StatusInternalServreErþror)
	// 	return
	// }
	resp := Response{
		Success: true,
		Data:    nil,
	}
	sendJSONResponse(w, resp)
}

// handleGetTimeSeriesMetrics handles requests for time series metrics
func (s *APIServer) handleGetTimeSeriesMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodOptions {
		sendErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Handle CORS preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Parse query parameters
	metricType := r.URL.Query().Get("metric_type")
	if metricType == "" {
		sendErrorResponse(w, "metric_type parameter is required", http.StatusBadRequest)
		return
	}

	// Get time range parameters
	startTimeStr := r.URL.Query().Get("start_time")
	endTimeStr := r.URL.Query().Get("end_time")

	// Validate time parameters
	var startTime, endTime time.Time
	var err error

	if startTimeStr != "" {
		startTime, err = time.Parse(time.RFC3339, startTimeStr)
		if err != nil {
			sendErrorResponse(w, "Invalid start_time format. Use RFC3339 format.", http.StatusBadRequest)
			return
		}
	} else {
		// Default to 24 hours ago if not provided
		startTime = time.Now().Add(-24 * time.Hour)
	}

	if endTimeStr != "" {
		endTime, err = time.Parse(time.RFC3339, endTimeStr)
		if err != nil {
			sendErrorResponse(w, "Invalid end_time format. Use RFC3339 format.", http.StatusBadRequest)
			return
		}
	} else {
		// Default to now if not provided
		endTime = time.Now()
	}

	// Get Redis client
	redisClient, err := cache.GetRedisCache()
	if err != nil {
		logger.L().Error("Failed to get Redis client", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Define Redis key pattern based on metric type
	var keyPattern string
	switch metricType {
	case "cpu_usage":
		keyPattern = "metrics:cpu:*"
	case "memory_usage":
		keyPattern = "metrics:memory:*"
	case "network_io":
		keyPattern = "metrics:network:*"
	case "disk_io":
		keyPattern = "metrics:disk:*"
	default:
		sendErrorResponse(w, "Unsupported metric type", http.StatusBadRequest)
		return
	}

	// Get all keys matching the pattern
	ctx := r.Context()
	keys, err := redisClient.GetCacheDB1().Keys(ctx, keyPattern).Result()
	if err != nil {
		logger.L().Error("Failed to get metric keys from Redis", map[string]interface{}{
			"error":       err.Error(),
			"key_pattern": keyPattern,
		})
		sendErrorResponse(w, "Failed to retrieve metrics", http.StatusInternalServerError)
		return
	}

	// If no metrics found
	if len(keys) == 0 {
		sendJSONResponse(w, Response{
			Success: true,
			Message: "No metrics found for the specified type and time range",
			Data: map[string]interface{}{
				"metric_type": metricType,
				"start_time":  startTime.Format(time.RFC3339),
				"end_time":    endTime.Format(time.RFC3339),
				"data_points": []interface{}{},
			},
		})
		return
	}

	// Get values for all keys in a pipeline
	pipe := redisClient.GetCacheDB1().Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))

	for i, key := range keys {
		cmds[i] = pipe.Get(ctx, key)
	}

	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		logger.L().Error("Failed to get metric values from Redis", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Failed to retrieve metric values", http.StatusInternalServerError)
		return
	}

	// Process results
	type MetricPoint struct {
		Timestamp time.Time `json:"timestamp"`
		Value     float64   `json:"value"`
	}

	var dataPoints []MetricPoint

	for i, cmd := range cmds {
		val, err := cmd.Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			logger.L().Error("Failed to get value for key", map[string]interface{}{
				"error": err.Error(),
				"key":   keys[i],
			})
			continue
		}

		// Extract timestamp from key
		// Expected format: metrics:type:timestamp
		parts := strings.Split(keys[i], ":")
		if len(parts) < 3 {
			continue
		}

		// Parse timestamp
		tsStr := parts[2]
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			continue
		}

		timestamp := time.Unix(ts, 0)

		// Filter by time range
		if timestamp.Before(startTime) || timestamp.After(endTime) {
			continue
		}

		// Parse value
		value, err := strconv.ParseFloat(val, 64)
		if err != nil {
			continue
		}

		dataPoints = append(dataPoints, MetricPoint{
			Timestamp: timestamp,
			Value:     value,
		})
	}

	// Sort data points by timestamp
	sort.Slice(dataPoints, func(i, j int) bool {
		return dataPoints[i].Timestamp.Before(dataPoints[j].Timestamp)
	})

	// Return response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "Time series metrics retrieved successfully",
		Data: map[string]interface{}{
			"metric_type": metricType,
			"start_time":  startTime.Format(time.RFC3339),
			"end_time":    endTime.Format(time.RFC3339),
			"data_points": dataPoints,
		},
	})
}

func (s *APIServer) handleGenerateDummyPositions(w http.ResponseWriter, r *http.Request) {
	// Parse index and expiry from query params (same as option-chain)
	index := r.URL.Query().Get("index")
	expiry := r.URL.Query().Get("expiry")

	// Validate required parameters
	if index == "" || expiry == "" {
		s.log.Error("Missing required parameters", map[string]interface{}{
			"error": "index and expiry are required parameters",
		})
		sendErrorResponse(w, "index and expiry are required parameters", http.StatusBadRequest)
		return
	}

	// Get Redis client for positions DB
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		s.log.Error("Failed to get Redis cache", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "failed to get Redis cache", http.StatusInternalServerError)
		return
	}
	positionsDB := redisCache.GetPositionsDB2()
	if positionsDB == nil {
		s.log.Error("Redis positions DB not initialized", nil)
		sendErrorResponse(w, "Redis positions DB not initialized", http.StatusInternalServerError)
		return
	}

	// Get option chain manager
	optionChainManager := optionchain.GetOptionChainManager()
	if optionChainManager == nil {
		s.log.Error("Option chain manager not initialized", nil)
		sendErrorResponse(w, "option chain manager not initialized", http.StatusInternalServerError)
		return
	}

	// Default to 1 strike (just ATM) if not specified
	strikesCount := 1

	// Calculate option chain to get ATM options
	optionChain, err := optionChainManager.CalculateOptionChain(r.Context(), index, expiry, strikesCount)
	if err != nil {
		s.log.Error("Failed to calculate option chain", map[string]interface{}{
			"error":  err.Error(),
			"index":  index,
			"expiry": expiry,
		})
		sendErrorResponse(w, fmt.Sprintf("failed to calculate option chain: %v", err), http.StatusInternalServerError)
		return
	}

	// Get lot size for the index from core package
	indices := core.GetIndices()
	indexObj := indices.GetIndexByName(index)
	if indexObj == nil {
		s.log.Error("Invalid index", map[string]interface{}{
			"index": index,
		})
		sendErrorResponse(w, "invalid index", http.StatusBadRequest)
		return
	}

	// Use one lot as the position quantity
	quantity := indexObj.GetMaxLotsPerOrder() * 5
	s.log.Debug("Using lot size for index", map[string]interface{}{
		"index":    index,
		"lot_size": quantity,
	})

	// Prepare variables for position data
	positions := []string{} // Will hold "tokenID_quantity" strings for all_positions
	positionTimestamp := time.Now().Format(time.RFC3339)

	// Find the ATM strike data (prefer one marked as ATM, otherwise use first)
	atmStrikeData := optionChain.Chain[0]
	for _, strike := range optionChain.Chain {
		if strike.IsATM {
			atmStrikeData = strike
			break
		}
	}

	// Generate CE position (buy)
	if atmStrikeData.CE != nil {
		ceOption := atmStrikeData.CE
		// Get the instrument token from the CE option
		ceToken, err := strconv.ParseUint(ceOption.InstrumentToken, 10, 32)
		if err != nil {
			s.log.Error("Failed to parse instrument token", map[string]interface{}{
				"error": err.Error(),
				"token": ceOption.InstrumentToken,
			})
			// Continue anyway, we'll skip this position
		} else {
			// Generate tradingsymbol based on token (simplified version for dummy data)
			tradingSymbol := fmt.Sprintf("%s%d%s", index, int(atmStrikeData.Strike), "CE")

			// Format: position:{tradingsymbol}:{token}
			cePositionKey := fmt.Sprintf("position:%s:%s", tradingSymbol, ceOption.InstrumentToken)
			cePosition := map[string]interface{}{
				"instrument_token": ceToken,
				"tradingsymbol":    tradingSymbol,
				"quantity":         quantity, // Using index lot size
				"average_price":    ceOption.LTP,
				"last_price":       ceOption.LTP,
				"pnl":              0.0, // Initial P&L is 0
				"product":          "NRML",
				"exchange":         "NFO",
			}
			// Convert to JSON
			cePositionJSON, _ := json.Marshal(cePosition)

			// Store directly in Redis (using hash structure)
			err = positionsDB.HSet(r.Context(), "positions", cePositionKey, string(cePositionJSON)).Err()
			if err != nil {
				s.log.Error("Failed to store CE position in Redis", map[string]interface{}{
					"error": err.Error(),
					"key":   cePositionKey,
				})
			}

			// Set TTL (30 days)
			err = positionsDB.Expire(r.Context(), cePositionKey, 30*24*time.Hour).Err()
			if err != nil {
				s.log.Error("Failed to set TTL for CE position", map[string]interface{}{
					"error": err.Error(),
					"key":   cePositionKey,
				})
			}

			// Add to positions list
			positions = append(positions, fmt.Sprintf("%d_%d", ceToken, quantity))
		}
	}

	// Generate PE position (buy)
	if atmStrikeData.PE != nil {
		peOption := atmStrikeData.PE
		// Get the instrument token from the PE option
		peToken, err := strconv.ParseUint(peOption.InstrumentToken, 10, 32)
		if err != nil {
			s.log.Error("Failed to parse instrument token", map[string]interface{}{
				"error": err.Error(),
				"token": peOption.InstrumentToken,
			})
			// Continue anyway, we'll skip this position
		} else {
			// Generate tradingsymbol based on token (simplified version for dummy data)
			tradingSymbol := fmt.Sprintf("%s%d%s", index, int(atmStrikeData.Strike), "PE")

			// Format: position:{tradingsymbol}:{token}
			pePositionKey := fmt.Sprintf("position:%s:%s", tradingSymbol, peOption.InstrumentToken)
			pePosition := map[string]interface{}{
				"instrument_token": peToken,
				"tradingsymbol":    tradingSymbol,
				"quantity":         quantity, // Using index lot size
				"average_price":    peOption.LTP,
				"last_price":       peOption.LTP,
				"pnl":              0.0, // Initial P&L is 0
				"product":          "NRML",
				"exchange":         "NFO",
			}
			// Convert to JSON
			pePositionJSON, _ := json.Marshal(pePosition)

			// Store directly in Redis (using hash structure)
			err = positionsDB.HSet(r.Context(), "positions", pePositionKey, string(pePositionJSON)).Err()
			if err != nil {
				s.log.Error("Failed to store PE position in Redis", map[string]interface{}{
					"error": err.Error(),
					"key":   pePositionKey,
				})
			}

			// Set TTL (30 days)
			err = positionsDB.Expire(r.Context(), pePositionKey, 30*24*time.Hour).Err()
			if err != nil {
				s.log.Error("Failed to set TTL for PE position", map[string]interface{}{
					"error": err.Error(),
					"key":   pePositionKey,
				})
			}

			// Add to positions list
			positions = append(positions, fmt.Sprintf("%d_%d", peToken, quantity))
		}
	}

	// Sort positions to ensure consistent output
	sort.Strings(positions)

	// Set the consolidated list of positions
	allPositionsValue := strings.Join(positions, ",")
	err = positionsDB.Set(r.Context(), "position:all_positions", allPositionsValue, 0).Err()
	if err != nil {
		s.log.Error("Failed to set consolidated positions list", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Set timestamp for position updates
	err = positionsDB.Set(r.Context(), "position:all_positions:timestamp", positionTimestamp, 0).Err()
	if err != nil {
		s.log.Error("Failed to set positions timestamp", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Create success response
	response := Response{
		Success: true,
		Message: "Dummy positions created successfully",
		Data: map[string]interface{}{
			"index":                index,
			"expiry":               expiry,
			"atm_strike":           atmStrikeData.Strike,
			"ce_position_quantity": quantity,
			"pe_position_quantity": quantity,
			"positions_created":    len(positions),
			"lot_size":             quantity,
		},
	}

	sendJSONResponse(w, response)
}

// handleGeneral handles the /general endpoint that returns general market information
func (s *APIServer) handleGeneral(w http.ResponseWriter, r *http.Request) {
	// Get all indices to include their metadata
	allIndices := core.GetIndices()

	// Create index info map
	indexInfo := make(map[string]IndexInfoStruct)

	// Add NIFTY
	indexInfo["NIFTY"] = IndexInfoStruct{
		IndexNumber: allIndices.NIFTY.IndexNumber,
		DisplayName: allIndices.NIFTY.NameInIndices,
		Enabled:     allIndices.NIFTY.Enabled,
	}

	// Add SENSEX
	indexInfo["SENSEX"] = IndexInfoStruct{
		IndexNumber: allIndices.SENSEX.IndexNumber,
		DisplayName: allIndices.SENSEX.NameInIndices,
		Enabled:     allIndices.SENSEX.Enabled,
	}

	// Add BANKNIFTY
	indexInfo["BANKNIFTY"] = IndexInfoStruct{
		IndexNumber: allIndices.BANKNIFTY.IndexNumber,
		DisplayName: allIndices.BANKNIFTY.NameInIndices,
		Enabled:     allIndices.BANKNIFTY.Enabled,
	}

	response := Response{
		Success: true,
		Data: GeneralResponse{
			LotSizes:  LotSizes,
			IndexInfo: indexInfo,
		},
	}

	sendJSONResponse(w, response)
}

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

// handleGetSettings handles GET /settings endpoint to retrieve application settings
func (s *APIServer) handleGetSettings(w http.ResponseWriter, r *http.Request) {
	// Get Redis cache instance
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		s.log.Error("Failed to get Redis cache", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Failed to connect to Redis", http.StatusInternalServerError)
		return
	}

	// Get settings DB
	settingsDB := redisCache.GetSettingsDB6()

	// Get limit_order setting from Redis, default to "disabled" if not set
	limitOrder, err := settingsDB.Get(r.Context(), "limit_order").Result()
	if err == redis.Nil {
		// If key doesn't exist, return default value
		limitOrder = "disabled"
	} else if err != nil {
		s.log.Error("Failed to get settings from Redis", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Failed to retrieve settings", http.StatusInternalServerError)
		return
	}

	response := Response{
		Success: true,
		Data: SettingsResponse{
			LimitOrder: limitOrder,
		},
	}

	sendJSONResponse(w, response)
}

// handleUpdateSettings handles POST /settings endpoint to update application settings
func (s *APIServer) handleUpdateSettings(w http.ResponseWriter, r *http.Request) {
	// Get Redis cache instance
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		s.log.Error("Failed to get Redis cache", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Failed to connect to Redis", http.StatusInternalServerError)
		return
	}

	// Get settings DB
	settingsDB := redisCache.GetSettingsDB6()

	// Parse request body
	var req UpdateSettingsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.log.Error("Failed to decode settings update request", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Validate limit_order value
	if req.LimitOrder != "enabled" && req.LimitOrder != "disabled" {
		sendErrorResponse(w, "Invalid limit_order value, must be 'enabled' or 'disabled'", http.StatusBadRequest)
		return
	}

	// Store the setting in Redis with a long TTL (30 days)
	err = settingsDB.Set(r.Context(), "limit_order", req.LimitOrder, 30*24*time.Hour).Err()
	if err != nil {
		s.log.Error("Failed to store settings in Redis", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Failed to update settings", http.StatusInternalServerError)
		return
	}

	// Convert request to response
	settingsResponse := SettingsResponse{
		LimitOrder: req.LimitOrder,
	}

	// Return the updated settings
	response := Response{
		Success: true,
		Data:    settingsResponse,
	}

	sendJSONResponse(w, response)
}
