package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"gohustle/db"
	"gohustle/filestore"
	"gohustle/logger"
)

// handleGetAvailableTickDates, handleExportTickData, handleDeleteTickData, handleListExportedFiles, and handleGetTickSamples
// are registered in the Server.setupRoutes method

// handleGetAvailableTickDates returns a list of dates with available tick data
// Supports optional index_name query parameter to filter by index
func handleGetAvailableTickDates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodOptions {
		sendErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Handle CORS preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Get index_name from query parameters if provided
	indexName := r.URL.Query().Get("index_name")

	// Get available dates from database
	dates, err := db.GetTimescaleDB().GetAvailableTickDates(r.Context(), indexName)
	if err != nil {
		logger.L().Error("Failed to get available tick dates", map[string]interface{}{
			"error": err.Error(),
			"index": indexName,
		})
		sendErrorResponse(w, "Failed to get available tick dates", http.StatusInternalServerError)
		return
	}

	// Return as JSON response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "Available tick dates retrieved successfully",
		Data:    dates,
	})
}

// ExportTickDataRequest represents a request to export tick data
type ExportTickDataRequest struct {
	IndexName string `json:"index_name"`
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
}

// ExportTickDataResponse represents the response from exporting tick data
type ExportTickDataResponse struct {
	IndexName     string `json:"index_name"`
	StartDate     string `json:"start_date"`
	EndDate       string `json:"end_date"`
	TicksExported int64  `json:"ticks_exported"`
	FilePath      string `json:"file_path"`
	FileSize      int64  `json:"file_size_bytes"`
}

// handleExportTickData exports tick data for a specific date range
func handleExportTickData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodOptions {
		sendErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	// Handle CORS preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Parse request
	var req ExportTickDataRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Validate request
	if req.IndexName == "" {
		http.Error(w, "Index name is required", http.StatusBadRequest)
		return
	}
	if req.IndexName != "NIFTY" && req.IndexName != "SENSEX" {
		http.Error(w, "Invalid index name. Must be 'NIFTY' or 'SENSEX'", http.StatusBadRequest)
		return
	}
	if req.StartDate == "" {
		http.Error(w, "Start date is required", http.StatusBadRequest)
		return
	}
	if req.EndDate == "" {
		http.Error(w, "End date is required", http.StatusBadRequest)
		return
	}

	// Parse dates
	startDate, err := time.Parse("2006-01-02", req.StartDate)
	if err != nil {
		http.Error(w, "Invalid start date format. Use YYYY-MM-DD", http.StatusBadRequest)
		return
	}
	endDate, err := time.Parse("2006-01-02", req.EndDate)
	if err != nil {
		http.Error(w, "Invalid end date format. Use YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	// Validate date range
	if endDate.Before(startDate) {
		http.Error(w, "End date must be after start date", http.StatusBadRequest)
		return
	}

	// Create export file path
	fileName := fmt.Sprintf("%s_%s_to_%s.parquet", 
		req.IndexName, 
		req.StartDate, 
		req.EndDate,
	)
	filePath := filepath.Join(filestore.DefaultParquetDir, fileName)

	// Get or create parquet writer
	parquetStore := filestore.GetParquetStore()
	writer, err := parquetStore.GetOrCreateWriter(filePath)
	if err != nil {
		logger.L().Error("Failed to create parquet writer", map[string]interface{}{
			"error":      err.Error(),
			"index":      req.IndexName,
			"start_date": req.StartDate,
			"end_date":   req.EndDate,
			"file_path":  filePath,
		})
		sendErrorResponse(w, "Failed to create export file", http.StatusInternalServerError)
		return
	}

	// Get tick data from database
	rows, err := db.GetTimescaleDB().GetTickDataForExport(r.Context(), req.IndexName, startDate, endDate)
	if err != nil {
		logger.L().Error("Failed to get tick data", map[string]interface{}{
			"error":      err.Error(),
			"index":      req.IndexName,
			"start_date": req.StartDate,
			"end_date":   req.EndDate,
		})
		sendErrorResponse(w, "Failed to get tick data", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Stream data from database to Parquet file
	var tickCount int64
	batchSize := 10000
	batch := make([]filestore.TickRecord, 0, batchSize)

	for rows.Next() {
		var tick db.TickRecord
		err := rows.Scan(
			&tick.ID,
			&tick.InstrumentToken,
			&tick.ExchangeUnixTimestamp,
			&tick.LastPrice,
			&tick.OpenInterest,
			&tick.VolumeTraded,
			&tick.AverageTradePrice,
			&tick.TickReceivedTime,
			&tick.TickStoredInDbTime,
		)
		if err != nil {
			logger.L().Error("Failed to scan tick data", map[string]interface{}{
				"error": err.Error(),
			})
			continue
		}

		// Convert to Parquet record
		parquetTick := filestore.TickRecord{
			ID:                int64(tick.ID),
			InstrumentToken:   int64(tick.InstrumentToken),
			ExchangeTimestamp: tick.ExchangeUnixTimestamp,
			LastPrice:         tick.LastPrice,
			OpenInterest:      int32(tick.OpenInterest),
			VolumeTraded:      int32(tick.VolumeTraded),
			AverageTradePrice: tick.AverageTradePrice,
		}

		batch = append(batch, parquetTick)
		tickCount++

		// Write batch when it reaches the batch size
		if len(batch) >= batchSize {
			if err := parquetStore.WriteBatch(req.IndexName, batch); err != nil {
				logger.L().Error("Failed to write batch", map[string]interface{}{
					"error":      err.Error(),
					"batch_size": len(batch),
				})
			}
			batch = batch[:0] // Clear batch
		}

		if tickCount%100000 == 0 {
			logger.L().Info("Export progress", map[string]interface{}{
				"index":       req.IndexName,
				"ticks_so_far": tickCount,
			})
		}
	}

	// Write any remaining records
	if len(batch) > 0 {
		if err := parquetStore.WriteBatch(req.IndexName, batch); err != nil {
			logger.L().Error("Failed to write final batch", map[string]interface{}{
				"error":      err.Error(),
				"batch_size": len(batch),
			})
		}
	}

	if err := rows.Err(); err != nil {
		logger.L().Error("Error iterating tick data rows", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Get file stats
	stats, err := writer.GetStats()
	if err != nil {
		logger.L().Error("Failed to get file stats", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Return success response
	response := ExportTickDataResponse{
		IndexName:     req.IndexName,
		StartDate:     req.StartDate,
		EndDate:       req.EndDate,
		TicksExported: tickCount,
		FilePath:      filePath,
		FileSize:      stats.CompressedSize,
	}

	logger.L().Info("Completed tick data export", map[string]interface{}{
		"index":         req.IndexName,
		"start_date":    req.StartDate,
		"end_date":      req.EndDate,
		"ticks_exported": tickCount,
		"file_size_bytes": stats.CompressedSize,
		"file_path":     filePath,
	})

	// Return success response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "Tick data exported successfully",
		Data:    response,
	})
}

// DeleteTickDataRequest represents a request to delete tick data
type DeleteTickDataRequest struct {
	IndexName string `json:"index_name"`
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
}

// handleDeleteTickData deletes tick data for a specific date range
func handleDeleteTickData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodOptions {
		sendErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	// Handle CORS preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Parse request
	var req DeleteTickDataRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Validate request
	if req.IndexName == "" {
		http.Error(w, "Index name is required", http.StatusBadRequest)
		return
	}
	if req.IndexName != "NIFTY" && req.IndexName != "SENSEX" {
		http.Error(w, "Invalid index name. Must be 'NIFTY' or 'SENSEX'", http.StatusBadRequest)
		return
	}
	if req.StartDate == "" {
		http.Error(w, "Start date is required", http.StatusBadRequest)
		return
	}
	if req.EndDate == "" {
		http.Error(w, "End date is required", http.StatusBadRequest)
		return
	}

	// Parse dates
	startDate, err := time.Parse("2006-01-02", req.StartDate)
	if err != nil {
		http.Error(w, "Invalid start date format. Use YYYY-MM-DD", http.StatusBadRequest)
		return
	}
	endDate, err := time.Parse("2006-01-02", req.EndDate)
	if err != nil {
		http.Error(w, "Invalid end date format. Use YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	// Validate date range
	if endDate.Before(startDate) {
		http.Error(w, "End date must be after start date", http.StatusBadRequest)
		return
	}

	// Delete data from database
	rowsDeleted, err := db.GetTimescaleDB().DeleteTickData(r.Context(), req.IndexName, startDate, endDate)
	if err != nil {
		logger.L().Error("Failed to delete data", map[string]interface{}{
			"error":      err.Error(),
			"index":      req.IndexName,
			"start_date": req.StartDate,
			"end_date":   req.EndDate,
		})
		sendErrorResponse(w, fmt.Sprintf("Failed to delete data: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	// Return success response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "Tick data deleted successfully",
		Data: map[string]interface{}{
			"rows_deleted": rowsDeleted,
			"index":        req.IndexName,
			"start_date":   req.StartDate,
			"end_date":     req.EndDate,
		},
	})
}

// handleListExportedFiles returns a list of exported files
func handleListExportedFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodOptions {
		sendErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	// Handle CORS preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// List files in the parquet directory
	files, err := filepath.Glob(filepath.Join(filestore.DefaultParquetDir, "*.parquet"))
	if err != nil {
		logger.L().Error("Failed to list exported files", map[string]interface{}{
			"error": err.Error(),
			"path":  filestore.DefaultParquetDir,
		})
		sendErrorResponse(w, "Failed to list exported files", http.StatusInternalServerError)
		return
	}

	// Convert to relative paths
	relFiles := make([]string, 0, len(files))
	for _, file := range files {
		relFile, err := filepath.Rel(filestore.DefaultParquetDir, file)
		if err != nil {
			logger.L().Error("Failed to get relative path", map[string]interface{}{
				"error": err.Error(),
				"file":  file,
			})
			continue
		}
		relFiles = append(relFiles, relFile)
	}

	// Return as JSON response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "Exported files retrieved successfully",
		Data: map[string]interface{}{
			"files": relFiles,
		},
	})
}

// TickSamplesRequest represents a request to get tick samples
type TickSamplesRequest struct {
	FilePath    string `json:"file_path"`
	NumSamples  int    `json:"num_samples"`
}

// handleGetTickSamples returns samples from a parquet file
func handleGetTickSamples(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodOptions {
		sendErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	// Handle CORS preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Parse request
	var req TickSamplesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Validate request
	if req.FilePath == "" {
		sendErrorResponse(w, "File path is required", http.StatusBadRequest)
		return
	}
	if req.NumSamples <= 0 {
		req.NumSamples = 10 // Default to 10 samples
	}

	// Prevent directory traversal
	if filepath.IsAbs(req.FilePath) || filepath.Clean(req.FilePath) != req.FilePath {
		sendErrorResponse(w, "Invalid file path", http.StatusBadRequest)
		return
	}

	// Get full path
	fullPath := filepath.Join(filestore.DefaultParquetDir, req.FilePath)

	// Get samples
	parquetStore := filestore.GetParquetStore()
	samples, err := parquetStore.ReadSamples(fullPath, req.NumSamples)
	if err != nil {
		logger.L().Error("Failed to read samples", map[string]interface{}{
			"error": err.Error(),
			"file":  fullPath,
		})
		sendErrorResponse(w, fmt.Sprintf("Failed to read samples: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	// Return as JSON response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "Tick samples retrieved successfully",
		Data: map[string]interface{}{
			"file":    req.FilePath,
			"samples": samples,
		},
	})
}
