// In api/api.go
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"gohustle/cache"
	"gohustle/filestore"
	"gohustle/logger"
	"gohustle/optionchain"
	"gohustle/zerodha"

	pb "gohustle/proto"

	"github.com/gorilla/mux"
)

// Server represents the HTTP API server
type Server struct {
	router *mux.Router
	server *http.Server
	port   string
	ctx    context.Context
	cancel context.CancelFunc
	log    *logger.Logger
}

// Response is a standard API response structure
type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
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
			router: mux.NewRouter(),
			port:   "8080", // Default port, can be changed
			ctx:    ctx,
			cancel: cancel,
			log:    logger.L(),
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
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start the server in a goroutine
	go func() {
		s.log.Info("Starting API server", map[string]interface{}{
			"port": s.port,
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

	// Health check endpoint
	s.router.HandleFunc("/api/health", s.handleHealthCheck).Methods("GET", "OPTIONS")

	// Expiries endpoint
	s.router.HandleFunc("/api/expiries", s.handleGetExpiries).Methods("GET", "OPTIONS")

	// Option chain endpoint
	s.router.HandleFunc("/api/option-chain", s.handleGetOptionChain).Methods("GET", "OPTIONS")

	// Positions endpoint
	s.router.HandleFunc("/api/positions", s.handleGetPositions).Methods("GET", "OPTIONS")

	// API version 1 routes
	v1 := s.router.PathPrefix("/api/v1").Subrouter()

	// Market data endpoints
	market := v1.PathPrefix("/market").Subrouter()
	market.HandleFunc("/indices", s.handleListIndices).Methods("GET", "OPTIONS")
	market.HandleFunc("/instruments", s.handleListInstruments).Methods("GET", "OPTIONS")
	market.HandleFunc("/status", s.handleGetMarketStatus).Methods("GET", "OPTIONS")

	// Data export endpoints
	export := v1.PathPrefix("/export").Subrouter()
	export.HandleFunc("/wal-to-parquet", s.handleWalToParquet).Methods("POST", "OPTIONS")

	// Add more routes as needed
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

// handleWalToParquet handles the conversion of WAL data to Parquet format
func (s *Server) handleWalToParquet(w http.ResponseWriter, r *http.Request) {
	var req WalToParquetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		SendErrorResponse(w, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	// Validate request
	if err := validateWalToParquetRequest(&req); err != nil {
		SendErrorResponse(w, http.StatusBadRequest, "Invalid request parameters", err)
		return
	}

	// Get tick store instance
	tickStore := filestore.GetTickStore()

	// Start conversion
	startTime := time.Now()

	// Read ticks from WAL
	ticks, err := tickStore.ReadTicks(req.IndexName, req.Date)
	if err != nil {
		SendErrorResponse(w, http.StatusInternalServerError, "Failed to read ticks from WAL", err)
		return
	}

	if len(ticks) == 0 {
		SendErrorResponse(w, http.StatusNotFound, fmt.Sprintf("No ticks found for index %s on date %s", req.IndexName, req.Date), nil)
		return
	}

	// Convert protobuf ticks to parquet records
	records := make([]filestore.TickRecord, len(ticks))
	for i, tick := range ticks {
		records[i] = convertProtoTickToParquetRecord(tick)
	}

	// Get parquet store instance
	parquetStore := filestore.GetParquetStore()

	// Write to parquet file
	writer, err := parquetStore.GetOrCreateWriter(req.ParquetPath)
	if err != nil {
		SendErrorResponse(w, http.StatusInternalServerError, "Failed to create Parquet writer", err)
		return
	}

	// Write records in batches
	const batchSize = 100000
	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		if err := parquetStore.WriteBatch(req.IndexName, records[i:end]); err != nil {
			SendErrorResponse(w, http.StatusInternalServerError, "Failed to write batch to Parquet file", err)
			return
		}
	}

	// Get file info for response
	stats, err := writer.GetStats()
	if err != nil {
		s.log.Error("Failed to get Parquet file stats", map[string]interface{}{
			"error": err.Error(),
			"path":  req.ParquetPath,
		})
	}

	s.log.Info("Parquet file stats", map[string]interface{}{
		"path":  req.ParquetPath,
		"stats": stats,
	})

	resp := Response{
		Success: true,
		Message: "WAL to Parquet conversion successful",
		Data: map[string]interface{}{
			"records_count":     len(records),
			"uncompressed_size": stats.UncompressedSize,
			"compressed_size":   stats.CompressedSize,
			"compression_ratio": stats.CompressionRatio,
			"conversion_time":   time.Since(startTime).String(),
			"parquet_path":      req.ParquetPath,
		},
	}

	SendJSONResponse(w, http.StatusOK, resp)
}

// validateWalToParquetRequest validates the request parameters
func validateWalToParquetRequest(req *WalToParquetRequest) error {
	if req.IndexName == "" {
		return fmt.Errorf("index_name is required")
	}

	if req.Date == "" {
		return fmt.Errorf("date is required")
	}

	// Validate date format (YYYY-MM-DD)
	if _, err := time.Parse("2006-01-02", req.Date); err != nil {
		return fmt.Errorf("invalid date format, expected YYYY-MM-DD")
	}

	if req.ParquetPath == "" {
		return fmt.Errorf("parquet_path is required")
	}

	// Set default values if not provided
	if req.Compression == "" {
		req.Compression = "SNAPPY" // Default compression
	}

	if req.RowGroupSize == 0 {
		req.RowGroupSize = 100000 // Default to 100k rows per group
	}

	return nil
}

// convertProtoTickToParquetRecord converts a protobuf tick to a parquet record
func convertProtoTickToParquetRecord(tick *pb.TickData) filestore.TickRecord {
	record := filestore.TickRecord{
		InstrumentToken:    int64(tick.InstrumentToken),
		IsTradable:         tick.IsTradable,
		IsIndex:            tick.IsIndex,
		Mode:               tick.Mode,
		LastPrice:          tick.LastPrice,
		LastTradedQuantity: int32(tick.LastTradedQuantity),
		AverageTradePrice:  tick.AverageTradePrice,
		VolumeTraded:       int32(tick.VolumeTraded),
		TotalBuyQuantity:   int32(tick.TotalBuyQuantity),
		TotalSellQuantity:  int32(tick.TotalSellQuantity),
		TotalBuy:           int32(tick.TotalBuy),
		TotalSell:          int32(tick.TotalSell),
		NetChange:          tick.NetChange,
	}

	// Convert timestamps
	record.Timestamp.Time = time.Unix(0, tick.Timestamp)
	record.Timestamp.Valid = true

	if tick.LastTradeTime > 0 {
		record.LastTradeTime.Time = time.Unix(0, tick.LastTradeTime)
		record.LastTradeTime.Valid = true
	}

	if tick.TickRecievedTime > 0 {
		record.TickReceivedTime.Time = time.Unix(0, tick.TickRecievedTime)
		record.TickReceivedTime.Valid = true
	}

	if tick.TickStoredInDbTime > 0 {
		record.TickStoredInDbTime.Time = time.Unix(0, tick.TickStoredInDbTime)
		record.TickStoredInDbTime.Valid = true
	}

	// Convert OHLC data
	if tick.Ohlc != nil {
		record.OhlcOpen = tick.Ohlc.Open
		record.OhlcHigh = tick.Ohlc.High
		record.OhlcLow = tick.Ohlc.Low
		record.OhlcClose = tick.Ohlc.Close
	}

	// Convert market depth data
	if tick.Depth != nil {
		// Buy side
		if len(tick.Depth.Buy) >= 1 {
			record.DepthBuyPrice1 = tick.Depth.Buy[0].Price
			record.DepthBuyQuantity1 = int32(tick.Depth.Buy[0].Quantity)
			record.DepthBuyOrders1 = int32(tick.Depth.Buy[0].Orders)
		}
		if len(tick.Depth.Buy) >= 2 {
			record.DepthBuyPrice2 = tick.Depth.Buy[1].Price
			record.DepthBuyQuantity2 = int32(tick.Depth.Buy[1].Quantity)
			record.DepthBuyOrders2 = int32(tick.Depth.Buy[1].Orders)
		}
		if len(tick.Depth.Buy) >= 3 {
			record.DepthBuyPrice3 = tick.Depth.Buy[2].Price
			record.DepthBuyQuantity3 = int32(tick.Depth.Buy[2].Quantity)
			record.DepthBuyOrders3 = int32(tick.Depth.Buy[2].Orders)
		}
		if len(tick.Depth.Buy) >= 4 {
			record.DepthBuyPrice4 = tick.Depth.Buy[3].Price
			record.DepthBuyQuantity4 = int32(tick.Depth.Buy[3].Quantity)
			record.DepthBuyOrders4 = int32(tick.Depth.Buy[3].Orders)
		}
		if len(tick.Depth.Buy) >= 5 {
			record.DepthBuyPrice5 = tick.Depth.Buy[4].Price
			record.DepthBuyQuantity5 = int32(tick.Depth.Buy[4].Quantity)
			record.DepthBuyOrders5 = int32(tick.Depth.Buy[4].Orders)
		}

		// Sell side
		if len(tick.Depth.Sell) >= 1 {
			record.DepthSellPrice1 = tick.Depth.Sell[0].Price
			record.DepthSellQuantity1 = int32(tick.Depth.Sell[0].Quantity)
			record.DepthSellOrders1 = int32(tick.Depth.Sell[0].Orders)
		}
		if len(tick.Depth.Sell) >= 2 {
			record.DepthSellPrice2 = tick.Depth.Sell[1].Price
			record.DepthSellQuantity2 = int32(tick.Depth.Sell[1].Quantity)
			record.DepthSellOrders2 = int32(tick.Depth.Sell[1].Orders)
		}
		if len(tick.Depth.Sell) >= 3 {
			record.DepthSellPrice3 = tick.Depth.Sell[2].Price
			record.DepthSellQuantity3 = int32(tick.Depth.Sell[2].Quantity)
			record.DepthSellOrders3 = int32(tick.Depth.Sell[2].Orders)
		}
		if len(tick.Depth.Sell) >= 4 {
			record.DepthSellPrice4 = tick.Depth.Sell[3].Price
			record.DepthSellQuantity4 = int32(tick.Depth.Sell[3].Quantity)
			record.DepthSellOrders4 = int32(tick.Depth.Sell[3].Orders)
		}
		if len(tick.Depth.Sell) >= 5 {
			record.DepthSellPrice5 = tick.Depth.Sell[4].Price
			record.DepthSellQuantity5 = int32(tick.Depth.Sell[4].Quantity)
			record.DepthSellOrders5 = int32(tick.Depth.Sell[4].Orders)
		}
	}

	return record
}

// SendJSONResponse is a helper function to send a JSON response
func SendJSONResponse(w http.ResponseWriter, status int, resp interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}

// SendErrorResponse is a helper function to send an error response
func SendErrorResponse(w http.ResponseWriter, status int, message string, err error) {
	resp := Response{
		Success: false,
		Message: message,
	}

	if err != nil {
		resp.Error = err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(resp)
}

// handleGetExpiries returns the expiry dates for all indices from in-memory cache
func (s *Server) handleGetExpiries(w http.ResponseWriter, r *http.Request) {
	cache := cache.GetInMemoryCacheInstance()

	// Get list of instruments from cache
	instrumentsKey := "instrument:expiries:list"
	instrumentsValue, exists := cache.Get(instrumentsKey)
	if !exists {
		SendErrorResponse(w, http.StatusNotFound, "No instruments found in cache", nil)
		return
	}

	instruments, ok := instrumentsValue.([]string)
	if !ok {
		SendErrorResponse(w, http.StatusInternalServerError, "Invalid data type for instruments in cache", nil)
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
		SendErrorResponse(w, http.StatusNotFound, "No expiry dates found", nil)
		return
	}

	resp := Response{
		Success: true,
		Message: "Expiry dates retrieved successfully",
		Data:    expiriesMap,
	}

	SendJSONResponse(w, http.StatusOK, resp)
}

// handleGetOptionChain returns the option chain for a specific index and expiry
func (s *Server) handleGetOptionChain(w http.ResponseWriter, r *http.Request) {
	// Get query parameters
	index := r.URL.Query().Get("index")
	expiry := r.URL.Query().Get("expiry")
	strikes_count := r.URL.Query().Get("strikes")

	// Validate required parameters
	if index == "" {
		SendErrorResponse(w, http.StatusBadRequest, "Missing required parameter: index", nil)
		return
	}
	if expiry == "" {
		SendErrorResponse(w, http.StatusBadRequest, "Missing required parameter: expiry", nil)
		return
	}

	// Parse strikes_count (default to 5)
	numStrikes := 5
	if strikes_count != "" {
		var err error
		numStrikes, err = strconv.Atoi(strikes_count)
		if err != nil {
			SendErrorResponse(w, http.StatusBadRequest, "Invalid value for strikes parameter. Must be an integer.", nil)
			return
		}
	}

	// Validate expiry date format
	if _, err := time.Parse("2006-01-02", expiry); err != nil {
		SendErrorResponse(w, http.StatusBadRequest, "Invalid expiry date format. Use YYYY-MM-DD", nil)
		return
	}

	// Get option chain manager and calculate chain
	optionChainMgr := optionchain.GetOptionChainManager()
	response, err := optionChainMgr.CalculateOptionChain(r.Context(), index, expiry, numStrikes)
	if err != nil {
		SendErrorResponse(w, http.StatusInternalServerError, "Failed to calculate option chain", err)
		return
	}

	resp := Response{
		Success: true,
		Message: "Option chain retrieved successfully",
		Data:    response,
	}

	SendJSONResponse(w, http.StatusOK, resp)
}

// handleGetPositions returns detailed analysis of all positions
func (s *Server) handleGetPositions(w http.ResponseWriter, r *http.Request) {
	pm := zerodha.GetPositionManager()
	if pm == nil {
		SendErrorResponse(w, http.StatusInternalServerError, "Position manager not initialized", nil)
		return
	}

	analysis, err := pm.GetPositionAnalysis(r.Context())
	if err != nil {
		SendErrorResponse(w, http.StatusInternalServerError, "Failed to get position analysis", err)
		return
	}

	resp := Response{
		Success: true,
		Data:    analysis,
	}

	SendJSONResponse(w, http.StatusOK, resp)
}
