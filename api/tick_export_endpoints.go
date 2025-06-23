package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

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

	// Get available dates from parquet files
	parquetDir := filestore.DefaultParquetDir
	files, err := filepath.Glob(filepath.Join(parquetDir, "*.parquet"))
	if err != nil {
		logger.L().Error("Failed to list parquet files", map[string]interface{}{
			"error": err.Error(),
			"path":  parquetDir,
		})
		sendErrorResponse(w, "Failed to get available tick dates", http.StatusInternalServerError)
		return
	}

	// Extract dates from filenames
	type DateInfo struct {
		Date      string `json:"date"`
		IndexName string `json:"index_name"`
	}

	dateMap := make(map[string]DateInfo)
	for _, file := range files {
		filename := filepath.Base(file)
		// Expected format: INDEX_YYYY-MM-DD.parquet or INDEX_YYYY-MM-DD_to_YYYY-MM-DD.parquet
		parts := strings.Split(filename, "_")
		if len(parts) >= 2 {
			fileIndexName := parts[0]

			// Skip if index name filter is provided and doesn't match
			if indexName != "" && fileIndexName != indexName {
				continue
			}

			// Extract date from filename
			var dateStr string
			if len(parts) >= 3 && parts[1] == "to" {
				// Range export format
				dateStr = parts[0]
			} else {
				// Single date format
				dateStr = parts[1]
			}

			// Remove .parquet extension if present
			dateStr = strings.TrimSuffix(dateStr, ".parquet")

			// Add to map to deduplicate
			dateMap[dateStr] = DateInfo{
				Date:      dateStr,
				IndexName: fileIndexName,
			}
		}
	}

	// Convert map to slice
	var dates []DateInfo
	for _, info := range dateMap {
		dates = append(dates, info)
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

	// Check if file already exists
	if _, err := os.Stat(filePath); err == nil {
		// File exists, return it directly
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			logger.L().Error("Failed to get file info", map[string]interface{}{
				"error": err.Error(),
				"file":  filePath,
			})
			sendErrorResponse(w, "Failed to get file info", http.StatusInternalServerError)
			return
		}

		// Return success with file info
		sendJSONResponse(w, Response{
			Success: true,
			Message: "Tick data export file already exists",
			Data: ExportTickDataResponse{
				IndexName:     req.IndexName,
				StartDate:     req.StartDate,
				EndDate:       req.EndDate,
				FilePath:      fileName, // Return relative path
				FileSize:      fileInfo.Size(),
				TicksExported: 0, // Unknown without reading the file
			},
		})
		return
	}

	// Since we're not using TimescaleDB anymore, we need to inform the user
	// that direct export from raw data is not available
	sendJSONResponse(w, Response{
		Success: false,
		Message: "Direct export from raw tick data is no longer available. Please use the tick data collection system to generate Parquet files directly.",
		Data: map[string]interface{}{
			"index_name": req.IndexName,
			"start_date": req.StartDate,
			"end_date":   req.EndDate,
		},
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

	// Find matching parquet files
	parquetDir := filestore.DefaultParquetDir
	pattern := fmt.Sprintf("%s_*.parquet", req.IndexName)
	files, err := filepath.Glob(filepath.Join(parquetDir, pattern))
	if err != nil {
		logger.L().Error("Failed to list parquet files", map[string]interface{}{
			"error": err.Error(),
			"path":  parquetDir,
		})
		sendErrorResponse(w, "Failed to list parquet files", http.StatusInternalServerError)
		return
	}

	// Filter files by date range
	var filesToDelete []string
	var deletedCount int

	for _, file := range files {
		filename := filepath.Base(file)
		// Check if file matches date range
		if strings.Contains(filename, req.StartDate) || strings.Contains(filename, req.EndDate) {
			// Simple check - if filename contains either date, consider it for deletion
			filesToDelete = append(filesToDelete, file)
		}
	}

	// Delete matching files
	for _, file := range filesToDelete {
		if err := os.Remove(file); err != nil {
			logger.L().Error("Failed to delete file", map[string]interface{}{
				"error": err.Error(),
				"file":  file,
			})
			continue
		}
		deletedCount++
	}

	// Return success response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "Tick data files deleted successfully",
		Data: map[string]interface{}{
			"files_deleted": deletedCount,
			"index":         req.IndexName,
			"start_date":    req.StartDate,
			"end_date":      req.EndDate,
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
	FilePath   string `json:"file_path"`
	NumSamples int    `json:"num_samples"`
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
