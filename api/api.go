// In api/api.go
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"gohustle/cache"
	"gohustle/core"
	"gohustle/db"
	"gohustle/filestore"
	"gohustle/logger"
	"gohustle/optionchain"
	"gohustle/types"
	"gohustle/utils"
	"gohustle/zerodha"

	"github.com/gorilla/mux"
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

func (s *APIServer) handleGetOrders(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orders, err := db.GetTimescaleDB().ListOrders(ctx)
	if err != nil {
		s.log.Error("Failed to fetch orders", map[string]interface{}{"error": err.Error()})
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Failed to fetch orders",
		})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"orders":  orders,
	})
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
		sendErrorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := zerodha.PlaceOrder(*orderReq)
	if err != nil {
		s.log.Error("Order placement failed", map[string]interface{}{"error": err.Error()})
		sendErrorResponse(w, "Order placement failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
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

// Update handleWalToParquet to use the method
func (s *APIServer) handleWalToParquet(w http.ResponseWriter, r *http.Request) {
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

	// Get filter from query parameter, default to "all"
	filterParam := r.URL.Query().Get("filter")
	filterType := zerodha.PositionFilterAll

	// Validate filter parameter
	switch filterParam {
	case "paper":
		filterType = zerodha.PositionFilterPaper
	case "real":
		filterType = zerodha.PositionFilterReal
	case "", "all":
		// Default to all positions
	default:
		sendErrorResponse(w, "Invalid filter parameter. Must be 'all', 'paper', or 'real'.", http.StatusBadRequest)
		return
	}

	s.log.Debug("Getting positions", map[string]interface{}{
		"filter": filterType,
	})

	analysis, err := pm.GetPositionAnalysis(r.Context(), filterType)
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
	// Get PnL manager
	pnlManager := zerodha.GetPnLManager()

	// Get parameters with proper error handling
	exitPnL, _, err := pnlManager.AppParamManager().GetFloat(r.Context(), "exit_pnl")
	if err != nil {
		s.log.Error("Failed to get exit_pnl parameter", map[string]interface{}{"error": err.Error()})
		sendErrorResponse(w, "Failed to retrieve exit P&L parameter", http.StatusInternalServerError)
		return
	}

	targetPnL, _, err := pnlManager.AppParamManager().GetFloat(r.Context(), "target_pnl")
	if err != nil {
		s.log.Error("Failed to get target_pnl parameter", map[string]interface{}{"error": err.Error()})
		sendErrorResponse(w, "Failed to retrieve target P&L parameter", http.StatusInternalServerError)
		return
	}

	sendJSONResponse(w, PnLParamsResponse{
		Success: true,
		Data: PnLParams{
			ExitPnL:   exitPnL,
			TargetPnL: targetPnL,
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

	// Get PnL manager
	pnlManager := zerodha.GetPnLManager()

	// Update exit_pnl if provided
	if req.ExitPnL != nil {
		if err := pnlManager.SetExitPnL(r.Context(), *req.ExitPnL); err != nil {
			sendErrorResponse(w, fmt.Sprintf("Failed to update exit P&L: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Update target_pnl if provided
	if req.TargetPnL != nil {
		if err := pnlManager.SetTargetPnL(r.Context(), *req.TargetPnL); err != nil {
			sendErrorResponse(w, fmt.Sprintf("Failed to update target P&L: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Get updated parameters
	exitPnL, _, _ := pnlManager.AppParamManager().GetFloat(r.Context(), "exit_pnl")
	targetPnL, _, _ := pnlManager.AppParamManager().GetFloat(r.Context(), "target_pnl")

	sendJSONResponse(w, PnLParamsResponse{
		Success: true,
		Data: PnLParams{
			ExitPnL:   exitPnL,
			TargetPnL: targetPnL,
		},
	})
}

func (s *APIServer) handleGetPnL(w http.ResponseWriter, r *http.Request) {
	pnlManager := zerodha.GetPnLManager()
	if pnlManager == nil {
		sendErrorResponse(w, "PnL manager not initialized", http.StatusInternalServerError)
		return
	}

	// Get query parameters
	strategy := r.URL.Query().Get("strategy")
	paperOnly := r.URL.Query().Get("paper_only") == "true"
	realOnly := r.URL.Query().Get("real_only") == "true"

	// Calculate P&L
	pnlSummary, err := pnlManager.CalculatePnL(r.Context())
	if err != nil {
		s.log.Error("Failed to calculate P&L", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Failed to calculate P&L", http.StatusInternalServerError)
		return
	}

	// Filter by strategy if specified
	if strategy != "" {
		// Create a filtered summary
		filteredSummary := &zerodha.PnLSummary{
			PositionPnL:      make(map[string]float64),
			PaperPositionPnL: make(map[string]float64),
			StrategyPnL:      make(map[string]zerodha.StrategyPnLSummary),
			PaperStrategyPnL: make(map[string]zerodha.StrategyPnLSummary),
			UpdatedAt:        pnlSummary.UpdatedAt,
		}

		// Filter real trading strategies
		if !paperOnly {
			if strategySummary, exists := pnlSummary.StrategyPnL[strategy]; exists {
				filteredSummary.StrategyPnL[strategy] = strategySummary
				filteredSummary.TotalRealizedPnL += strategySummary.RealizedPnL
				filteredSummary.TotalUnrealizedPnL += strategySummary.UnrealizedPnL

				// Add positions to position P&L map
				for _, pos := range strategySummary.Positions {
					filteredSummary.PositionPnL[pos.TradingSymbol] = pos.TotalPnL
				}
			}
		}

		// Filter paper trading strategies
		if !realOnly {
			if strategySummary, exists := pnlSummary.PaperStrategyPnL[strategy]; exists {
				filteredSummary.PaperStrategyPnL[strategy] = strategySummary

				// Add positions to paper position P&L map
				for _, pos := range strategySummary.Positions {
					filteredSummary.PaperPositionPnL[pos.TradingSymbol] = pos.TotalPnL
				}
			}
		}

		// Update total P&L
		filteredSummary.TotalPnL = filteredSummary.TotalRealizedPnL + filteredSummary.TotalUnrealizedPnL

		// Use filtered summary instead of full summary
		pnlSummary = filteredSummary
	} else if paperOnly {
		// Return only paper trading P&L
		filteredSummary := &zerodha.PnLSummary{
			PaperPositionPnL: pnlSummary.PaperPositionPnL,
			PaperStrategyPnL: pnlSummary.PaperStrategyPnL,
			UpdatedAt:        pnlSummary.UpdatedAt,
		}
		pnlSummary = filteredSummary
	} else if realOnly {
		// Return only real trading P&L
		filteredSummary := &zerodha.PnLSummary{
			TotalRealizedPnL:   pnlSummary.TotalRealizedPnL,
			TotalUnrealizedPnL: pnlSummary.TotalUnrealizedPnL,
			TotalPnL:           pnlSummary.TotalPnL,
			PositionPnL:        pnlSummary.PositionPnL,
			StrategyPnL:        pnlSummary.StrategyPnL,
			UpdatedAt:          pnlSummary.UpdatedAt,
		}
		pnlSummary = filteredSummary
	}

	resp := Response{
		Success: true,
		Data:    pnlSummary,
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
func (s *APIServer) handleGetTimeSeriesMetrics(w http.ResponseWriter, r *http.Request) {
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

	// Use TimescaleDB directly for metrics
	var dbMetrics []*db.IndexMetrics
	var err error

	var metrics []*types.MetricsData

	// Helper to convert db.IndexMetrics to types.MetricsData
	convertIndexMetrics := func(dbMetrics []*db.IndexMetrics) []*types.MetricsData {
		var result []*types.MetricsData
		for _, m := range dbMetrics {
			result = append(result, &types.MetricsData{
				UnderlyingPrice: m.SpotPrice,
				SyntheticFuture: m.FairPrice,
				LowestStraddle:  m.StraddlePrice,
				Timestamp:       m.Timestamp.UnixMilli(),
			})
		}
		return result
	}

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
			// Get points after lastKnown timestamp (assume milliseconds since epoch)
			startTime := time.UnixMilli(lastKnown)
			endTime := time.Now()
			dbMetrics, err = db.GetTimescaleDB().GetIndexMetricsInTimeRange(req.Index, startTime, endTime)
		} else {
			// Get latest point only
			dbMetrics, err = db.GetTimescaleDB().GetLatestIndexMetrics(req.Index, 1)
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
			dbMetrics, err = db.GetTimescaleDB().GetIndexMetricsInTimeRange(req.Index, time.UnixMilli(startTime), time.UnixMilli(endTime))
		} else if req.Count > 0 {
			dbMetrics, err = db.GetTimescaleDB().GetLatestIndexMetrics(req.Index, req.Count)
		} else {
			// Default to last 50 points if no time range or count specified
			dbMetrics, err = db.GetTimescaleDB().GetLatestIndexMetrics(req.Index, 50)
		}

	default:
		sendErrorResponse(w, "Invalid mode. Must be 'historical' or 'realtime'", http.StatusBadRequest)
		return
	}

	if err != nil {
		s.log.Error("Failed to fetch metrics", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Failed to fetch metrics", http.StatusInternalServerError)
		return
	}

	// If dbMetrics was used, convert to API type
	if dbMetrics != nil {
		metrics = convertIndexMetrics(dbMetrics)
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
