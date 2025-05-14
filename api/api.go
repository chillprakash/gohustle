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
	"strings"
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

// PlaceOrderAPIRequest is the API payload for placing a regular order
// (mirrors zerodha.PlaceOrderRequest, but with camelCase for JSON)
// MoveType represents the type of option position movement
type MoveType string

const (
	MoveAway   MoveType = "move_away"
	MoveCloser MoveType = "move_closer"
	Exit       MoveType = "exit"
)

// QuantityFraction represents the fraction of the position to process
type QuantityFraction string

const (
	FullPosition    QuantityFraction = "1"
	HalfPosition    QuantityFraction = "0.5"
	QuarterPosition QuantityFraction = "0.25"
)

type PlaceOrderAPIRequest struct {
	// Either provide InstrumentToken OR both TradingSymbol and Exchange
	InstrumentToken string  `json:"instrumentToken,omitempty"`
	TradingSymbol   string  `json:"tradingSymbol,omitempty"`
	Exchange        string  `json:"exchange,omitempty"`
	OrderType       string  `json:"orderType"`
	Side            string  `json:"side"`
	Quantity        int     `json:"quantity"`
	Price           float64 `json:"price,omitempty"`
	TriggerPrice    float64 `json:"triggerPrice,omitempty"`
	Product         string  `json:"product"`
	Validity        string  `json:"validity,omitempty"`
	DisclosedQty    int     `json:"disclosedQty,omitempty"`
	Tag             string  `json:"tag,omitempty"`
	PaperTrading    bool    `json:"paperTrading,omitempty"`

	// New fields for option movement functionality
	// These are populated from query parameters
	MoveType     MoveType         `json:"-"` // move_away, move_closer, or exit
	Steps        int              `json:"-"` // Number of strike steps to move (1 or 2)
	QuantityFrac QuantityFraction `json:"-"` // Fraction of position to process (1, 0.5, 0.25)
}

// PlaceGTTAPIRequest is the API payload for placing a GTT order
type PlaceGTTAPIRequest struct {
	TriggerType string `json:"triggerType"`
	// Either provide InstrumentToken OR both TradingSymbol and Exchange
	InstrumentToken string                   `json:"instrumentToken,omitempty"`
	TradingSymbol   string                   `json:"tradingSymbol,omitempty"`
	Exchange        string                   `json:"exchange,omitempty"`
	TriggerValues   []float64                `json:"triggerValues"`
	LastPrice       float64                  `json:"lastPrice"`
	Orders          []map[string]interface{} `json:"orders"`
}

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

// --- Order Handlers ---
func (s *Server) handleGetOrders(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) handlePlaceOrder(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for option movement functionality
	queryParams := r.URL.Query()

	// Parse request body
	var req PlaceOrderAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Check if this is a move operation by looking for the 'type' query parameter
	moveTypeStr := queryParams.Get("type")
	if moveTypeStr != "" {
		// This is a move operation, parse the parameters
		req.MoveType = MoveType(moveTypeStr)

		// Validate move type
		if req.MoveType != MoveAway && req.MoveType != MoveCloser && req.MoveType != Exit {
			sendErrorResponse(w, "Invalid move type. Must be one of: move_away, move_closer, exit", http.StatusBadRequest)
			return
		}

		// Parse steps parameter
		stepsStr := queryParams.Get("steps")
		if stepsStr != "" {
			steps, err := strconv.Atoi(stepsStr)
			if err != nil || (steps != 1 && steps != 2) {
				sendErrorResponse(w, "Invalid steps value. Must be 1 or 2", http.StatusBadRequest)
				return
			}
			req.Steps = steps
		} else if req.MoveType != Exit {
			// Steps is required for move_away and move_closer
			sendErrorResponse(w, "Steps parameter is required for move operations", http.StatusBadRequest)
			return
		}

		// Parse quantity fraction parameter
		quantityStr := queryParams.Get("quantity")
		if quantityStr != "" {
			req.QuantityFrac = QuantityFraction(quantityStr)
			if req.QuantityFrac != FullPosition && req.QuantityFrac != HalfPosition && req.QuantityFrac != QuarterPosition {
				sendErrorResponse(w, "Invalid quantity value. Must be 1, 0.5, or 0.25", http.StatusBadRequest)
				return
			}
		} else {
			// Default to full position if not specified
			req.QuantityFrac = FullPosition
		}

		// If this is a move operation, we need to handle it differently
		moveOp := GetMoveOperationInstance()
		moveOp.HandleMoveOperation(w, r, &req)
		return
	}

	// If instrument token is provided, fetch trading symbol and exchange
	if req.InstrumentToken != "" && (req.TradingSymbol == "" || req.Exchange == "") {
		// Get the initialized KiteConnect instance
		kc := zerodha.GetKiteConnect()

		// Lookup the instrument details
		tradingSymbol, exchange, err := kc.GetInstrumentDetailsByToken(r.Context(), req.InstrumentToken)
		if err != nil {
			s.log.Error("Failed to get instrument details", map[string]interface{}{
				"error": err.Error(),
				"token": req.InstrumentToken,
			})
			sendErrorResponse(w, fmt.Sprintf("Failed to get instrument details: %v", err), http.StatusBadRequest)
			return
		}

		// Set the trading symbol and exchange
		req.TradingSymbol = tradingSymbol
		req.Exchange = exchange

		s.log.Info("Resolved instrument token", map[string]interface{}{
			"token":          req.InstrumentToken,
			"trading_symbol": tradingSymbol,
			"exchange":       exchange,
		})
	}

	// Validate that we have the required fields
	if req.TradingSymbol == "" || req.Exchange == "" {
		sendErrorResponse(w, "Trading symbol and exchange are required", http.StatusBadRequest)
		return
	}

	orderReq := zerodha.PlaceOrderRequest{
		TradingSymbol: req.TradingSymbol,
		Exchange:      req.Exchange,
		OrderType:     zerodha.OrderType(req.OrderType),
		Side:          zerodha.OrderSide(req.Side),
		Quantity:      req.Quantity,
		Price:         req.Price,
		TriggerPrice:  req.TriggerPrice,
		Product:       zerodha.ProductType(req.Product),
		Validity:      req.Validity,
		DisclosedQty:  req.DisclosedQty,
		Tag:           req.Tag,
	}

	var resp *zerodha.OrderResponse
	var kiteResp interface{}

	// Check if this is a paper trading order
	if req.PaperTrading {
		// Generate a simulated response for paper trading
		s.log.Info("Processing paper trading order", map[string]interface{}{
			"symbol": req.TradingSymbol,
			"side":   req.Side,
			"qty":    req.Quantity,
		})

		// Try to get the latest price for the instrument from Redis
		var executionPrice float64 = req.Price // Default to the requested price

		if req.InstrumentToken != "" {
			// Get Redis cache for LTP data
			redisCache, err := cache.GetRedisCache()
			if err == nil {
				ltpDB := redisCache.GetLTPDB3()
				if ltpDB != nil {
					// Create a context with timeout for Redis operations
					ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
					defer cancel()

					// Format the key as expected in Redis
					ltpKey := fmt.Sprintf("%s_ltp", req.InstrumentToken)

					// Try to get the LTP from Redis
					ltpStr, err := ltpDB.Get(ctx, ltpKey).Result()
					if err == nil {
						ltp, err := strconv.ParseFloat(ltpStr, 64)
						if err == nil && ltp > 0 {
							executionPrice = ltp
							s.log.Info("Using Redis LTP for paper trading", map[string]interface{}{
								"instrument_token": req.InstrumentToken,
								"ltp":              ltp,
							})
						}
					}
				}
			}
		}

		// Generate a unique order ID for paper trading
		paperOrderID := fmt.Sprintf("paper-%s-%d", strings.ToLower(req.TradingSymbol), time.Now().UnixNano())

		// Create a simulated response
		resp = &zerodha.OrderResponse{
			OrderID: paperOrderID,
			Status:  "PAPER",
			Message: "Paper trading order simulated successfully",
		}

		// For paper trades, KiteResponse should be nil as there's no actual Zerodha interaction
		kiteResp = nil

		// Store the execution price in the request for tracking purposes
		orderReq.Price = executionPrice
	} else {
		// Place the actual order with Zerodha using the token from KiteConnect
		resp, err := zerodha.PlaceOrder(orderReq)
		if err != nil {
			s.log.Error("Order placement failed", map[string]interface{}{"error": err.Error()})
			sendErrorResponse(w, err.Error(), http.StatusBadGateway)
			return
		}

		// Store the Kite response for persistence
		kiteResp = resp
	}

	// Persist the order to the database (both real and paper) - using "system" as userID
	zerodha.SaveOrderAsync(orderReq, resp, "system", kiteResp)

	// Return the response to the client
	sendJSONResponse(w, map[string]interface{}{
		"order_id":      resp.OrderID,
		"status":        resp.Status,
		"message":       resp.Message,
		"paper_trading": req.PaperTrading,
	})
}

func (s *Server) handlePlaceGTTOrder(w http.ResponseWriter, r *http.Request) {
	var req PlaceGTTAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Default to regular trading mode
	paperTrading := false

	// If instrument token is provided, fetch trading symbol and exchange
	if req.InstrumentToken != "" && (req.TradingSymbol == "" || req.Exchange == "") {
		// Get the initialized KiteConnect instance
		kc := zerodha.GetKiteConnect()

		// Lookup the instrument details
		tradingSymbol, exchange, err := kc.GetInstrumentDetailsByToken(r.Context(), req.InstrumentToken)
		if err != nil {
			s.log.Error("Failed to get instrument details for GTT order", map[string]interface{}{
				"error": err.Error(),
				"token": req.InstrumentToken,
			})
			sendErrorResponse(w, fmt.Sprintf("Failed to get instrument details: %v", err), http.StatusBadRequest)
			return
		}

		// Set the trading symbol and exchange
		req.TradingSymbol = tradingSymbol
		req.Exchange = exchange

		s.log.Info("Resolved instrument token for GTT order", map[string]interface{}{
			"token":          req.InstrumentToken,
			"trading_symbol": tradingSymbol,
			"exchange":       exchange,
		})
	}

	// Validate that we have the required fields
	if req.TradingSymbol == "" || req.Exchange == "" {
		sendErrorResponse(w, "Trading symbol and exchange are required", http.StatusBadRequest)
		return
	}

	gttReq := zerodha.GTTOrderRequest{
		TriggerType:   req.TriggerType,
		TradingSymbol: req.TradingSymbol,
		Exchange:      req.Exchange,
		TriggerValues: req.TriggerValues,
		LastPrice:     req.LastPrice,
		Orders:        req.Orders,
	}

	var resp *zerodha.OrderResponse
	var err error

	if paperTrading {
		// Generate a simulated response for paper trading GTT order
		s.log.Info("Processing paper trading GTT order", map[string]interface{}{
			"symbol":         req.TradingSymbol,
			"trigger_type":   req.TriggerType,
			"trigger_values": req.TriggerValues,
		})

		// Try to get the latest price for the instrument from Redis
		var currentPrice float64 = req.LastPrice // Default to the requested last price

		if req.InstrumentToken != "" {
			// Get Redis cache for LTP data
			redisCache, err := cache.GetRedisCache()
			if err == nil {
				ltpDB := redisCache.GetLTPDB3()
				if ltpDB != nil {
					// Create a context with timeout for Redis operations
					ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
					defer cancel()

					// Format the key as expected in Redis
					ltpKey := fmt.Sprintf("%s_ltp", req.InstrumentToken)

					// Try to get the LTP from Redis
					ltpStr, err := ltpDB.Get(ctx, ltpKey).Result()
					if err == nil {
						ltp, err := strconv.ParseFloat(ltpStr, 64)
						if err == nil && ltp > 0 {
							currentPrice = ltp
							s.log.Info("Using Redis LTP for paper trading GTT", map[string]interface{}{
								"instrument_token": req.InstrumentToken,
								"ltp":              ltp,
							})
						}
					}
				}
			}
		}

		// Generate a unique order ID for paper trading GTT
		paperOrderID := fmt.Sprintf("paper-gtt-%s-%d", strings.ToLower(req.TradingSymbol), time.Now().UnixNano())

		// Create a simulated response
		resp = &zerodha.OrderResponse{
			OrderID: paperOrderID,
			Status:  "PAPER",
			Message: "Paper trading GTT order simulated successfully",
		}

		// Update the last price in the request with the current market price
		gttReq.LastPrice = currentPrice
	} else {
		// Place the actual GTT order with Zerodha
		resp, err = zerodha.PlaceGTTOrder(gttReq)
		if err != nil {
			s.log.Error("GTT order placement failed", map[string]interface{}{"error": err.Error()})
			sendErrorResponse(w, err.Error(), http.StatusBadGateway)
			return
		}
	}

	// Create a variable to hold the Kite response
	var kiteResp interface{}

	// For paper trades, kiteResp should be nil as there's no actual Zerodha interaction
	if paperTrading {
		kiteResp = nil
	} else {
		// For real trades, use the response from Zerodha
		kiteResp = resp
	}

	// Persist the GTT order to the database (both real and paper) with system user ID
	zerodha.SaveOrderAsync(gttReq, resp, "system", kiteResp)

	sendJSONResponse(w, map[string]interface{}{
		"order_id":      resp.OrderID,
		"status":        resp.Status,
		"message":       resp.Message,
		"paper_trading": paperTrading,
	})
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
	s.router.HandleFunc("/api/orders", func(w http.ResponseWriter, r *http.Request) {
		s.CORSMiddleware(http.HandlerFunc(s.handleGetOrders)).ServeHTTP(w, r)
	}).Methods("GET", "OPTIONS")

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
		// Always set CORS headers for all responses
		w.Header().Set("Access-Control-Allow-Origin", "*") // Allow all origins
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Kite-Token, X-User-ID")
		w.Header().Set("Access-Control-Max-Age", "3600")

		// Add these headers to ensure CORS works properly
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Vary", "Origin")

		// Handle preflight OPTIONS requests
		if r.Method == "OPTIONS" {
			// Return 200 OK with no content for OPTIONS requests
			w.WriteHeader(http.StatusOK)
			return
		}

		// Process the actual request
		next.ServeHTTP(w, r)
	})
}

// setupRoutes initializes all API routes
func (s *Server) setupRoutes() {
	// Create a global CORS middleware handler
	corsMiddleware := s.CORSMiddleware

	// Apply CORS middleware to the main router FIRST, before any routes are defined
	s.router.Use(corsMiddleware)

	// --- Order APIs ---
	// v1 API routes (with version in path)
	v1 := s.router.PathPrefix("/api/v1").Subrouter()
	v1.HandleFunc("/orders/place", s.handlePlaceOrder).Methods("POST", "OPTIONS")
	v1.HandleFunc("/orders/gtt", s.handlePlaceGTTOrder).Methods("POST", "OPTIONS")

	// Direct API routes (without version in path, for React app compatibility)
	directAPI := s.router.PathPrefix("/api").Subrouter()
	directAPI.HandleFunc("/orders/place", s.handlePlaceOrder).Methods("POST", "OPTIONS")
	directAPI.HandleFunc("/orders/gtt", s.handlePlaceGTTOrder).Methods("POST", "OPTIONS")

	// Auth routes (no authentication required)
	s.router.HandleFunc("/api/auth/login", s.handleLogin).Methods("POST", "OPTIONS")
	s.router.HandleFunc("/api/auth/logout", s.handleLogout).Methods("POST", "OPTIONS")
	s.router.HandleFunc("/api/auth/check", s.handleAuthCheck).Methods("GET", "OPTIONS") // Debug endpoint

	// Create authenticated router
	authenticatedRouter := s.router.PathPrefix("/api").Subrouter()
	// Apply Auth middleware to the authenticated router
	// (CORS is already applied at the router level)
	authenticatedRouter.Use(s.AuthMiddleware)

	// Health check endpoint
	authenticatedRouter.HandleFunc("/health", s.handleHealthCheck).Methods("GET", "OPTIONS")

	// Expiries endpoint
	authenticatedRouter.HandleFunc("/expiries", s.handleGetExpiries).Methods("GET", "OPTIONS")

	// Option chain endpoint
	authenticatedRouter.HandleFunc("/option-chain", s.handleGetOptionChain).Methods("GET", "OPTIONS")

	// Positions endpoint
	authenticatedRouter.HandleFunc("/positions", s.handleGetPositions).Methods("GET", "OPTIONS")

	// P&L endpoints
	authenticatedRouter.HandleFunc("/pnl", s.handleGetPnL).Methods("GET", "OPTIONS")
	authenticatedRouter.HandleFunc("/pnl/summary", HandleGetLatestPnLSummary).Methods("GET", "OPTIONS")

	// Time series metrics endpoint
	authenticatedRouter.HandleFunc("/metrics", s.handleGetTimeSeriesMetrics).Methods("GET", "OPTIONS")

	// API version 1 routes
	v1 = authenticatedRouter.PathPrefix("/v1").Subrouter()

	// Market data endpoints
	market := v1.PathPrefix("/market").Subrouter()
	market.HandleFunc("/indices", s.handleListIndices).Methods("GET", "OPTIONS")
	market.HandleFunc("/instruments", s.handleListInstruments).Methods("GET", "OPTIONS")
	market.HandleFunc("/status", s.handleGetMarketStatus).Methods("GET", "OPTIONS")

	// Data export endpoints
	export := v1.PathPrefix("/export").Subrouter()
	export.HandleFunc("/wal-to-parquet", s.handleWalToParquet).Methods("POST", "OPTIONS")

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

// Health check handler
func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
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

// handleGetExpiries returns the expiry dates for all indices from Redis cache
func (s *Server) handleGetExpiries(w http.ResponseWriter, r *http.Request) {
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

	// Try to get from in-memory first unless force calculate is true
	if !force_calculate {
		// Include strikes count in cache key for proper caching
		response = optionChainMgr.GetLatestChain(index, expiry, numStrikes)
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

// handleGetPnL returns P&L calculations for all positions
func (s *Server) handleGetPnL(w http.ResponseWriter, r *http.Request) {
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
func (s *Server) handleGeneral(w http.ResponseWriter, r *http.Request) {
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
