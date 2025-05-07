package api

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"gohustle/db"
	"gohustle/filestore"
	"gohustle/logger"
)

// TickDataDashboardResponse represents the dashboard overview for tick data
type TickDataDashboardResponse struct {
	// Data availability
	DataAvailability struct {
		FirstTickTime  string   `json:"first_tick_time"`
		LastTickTime   string   `json:"last_tick_time"`
		TotalTickCount int64    `json:"total_tick_count"`
		UniqueDates    []string `json:"unique_dates"`
		IndexName      string   `json:"index_name"`
	} `json:"data_availability"`

	// Archive jobs
	ArchiveJobs struct {
		JobsSummary struct {
			TotalJobs      int `json:"total_jobs"`
			CompletedJobs  int `json:"completed_jobs"`
			FailedJobs     int `json:"failed_jobs"`
			InProgressJobs int `json:"in_progress_jobs"`
		} `json:"summary"`
		RecentJobs []struct {
			JobID         string    `json:"job_id"`
			IndexName     string    `json:"index_name"`
			Status        string    `json:"status"`
			ExecutedAt    time.Time `json:"executed_at"`
			CompletedAt   time.Time `json:"completed_at,omitempty"`
			TickStartTime time.Time `json:"tick_start_time"`
			TickEndTime   time.Time `json:"tick_end_time"`
			TickCount     int       `json:"tick_count"`
			FilePath      string    `json:"file_path,omitempty"`
			FileSizeBytes int64     `json:"file_size_bytes,omitempty"`
		} `json:"recent_jobs"`
	} `json:"archive_jobs"`

	// Archived files
	ArchivedFiles struct {
		TotalFiles     int     `json:"total_files"`
		TotalSizeMB    float64 `json:"total_size_mb"`
		NIFTYFiles     int     `json:"nifty_files"`
		SENSEXFiles    int     `json:"sensex_files"`
		LatestArchiveAt string  `json:"latest_archive_at,omitempty"`
	} `json:"archived_files"`
}

// handleGetTickDataDashboard returns an overview of tick data statistics
func handleGetTickDataDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodOptions {
		sendErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Handle CORS preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Get index_name from query parameters
	indexName := r.URL.Query().Get("index_name")
	if indexName == "" {
		indexName = "NIFTY" // Default to NIFTY if not specified
	}

	// Validate index name
	if indexName != "NIFTY" && indexName != "SENSEX" {
		sendErrorResponse(w, "Invalid index name. Must be 'NIFTY' or 'SENSEX'", http.StatusBadRequest)
		return
	}

	// Initialize response
	var response TickDataDashboardResponse

	// Get data availability
	dataAvailability, err := getIndexDataSummary(r.Context(), indexName)
	if err != nil {
		logger.L().Error("Failed to get data summary", map[string]interface{}{
			"error": err.Error(),
			"index": indexName,
		})
	} else {
		response.DataAvailability = dataAvailability
	}

	// Get archive job statistics
	jobStats, err := getArchiveJobStats(r.Context())
	if err != nil {
		logger.L().Error("Failed to get archive job statistics", map[string]interface{}{
			"error": err.Error(),
		})
	} else {
		response.ArchiveJobs = jobStats
	}

	// Get archived files statistics
	fileStats, err := getArchivedFileStats()
	if err != nil {
		logger.L().Error("Failed to get archived file statistics", map[string]interface{}{
			"error": err.Error(),
		})
	} else {
		response.ArchivedFiles = fileStats
	}

	// Return as JSON response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "Tick data dashboard retrieved successfully",
		Data:    response,
	})
}

// getIndexDataSummary gets a summary of data availability for a specific index
func getIndexDataSummary(ctx context.Context, indexName string) (struct {
	FirstTickTime  string   `json:"first_tick_time"`
	LastTickTime   string   `json:"last_tick_time"`
	TotalTickCount int64    `json:"total_tick_count"`
	UniqueDates    []string `json:"unique_dates"`
	IndexName      string   `json:"index_name"`
}, error) {
	var result struct {
		FirstTickTime  string   `json:"first_tick_time"`
		LastTickTime   string   `json:"last_tick_time"`
		TotalTickCount int64    `json:"total_tick_count"`
		UniqueDates    []string `json:"unique_dates"`
		IndexName      string   `json:"index_name"`
	}

	// Set the index name
	result.IndexName = indexName

	// Get available dates from database
	dates, err := db.GetTimescaleDB().GetAvailableTickDates(ctx, indexName)
	if err != nil {
		return result, fmt.Errorf("failed to get available tick dates: %w", err)
	}

	// Calculate summary
	var totalTicks int64
	var firstTime, lastTime time.Time
	uniqueDatesMap := make(map[string]bool)

	for _, summaries := range dates {
		for _, summary := range summaries {
			if summary.IndexName != indexName {
				continue
			}

			// Add date to unique dates map
			uniqueDatesMap[summary.Date] = true
			totalTicks += summary.TickCount

			// Parse first and last tick times
			firstTickTime, err := time.Parse(time.RFC3339, summary.FirstTickTime)
			if err != nil {
				continue
			}

			lastTickTime, err := time.Parse(time.RFC3339, summary.LastTickTime)
			if err != nil {
				continue
			}

			// Update first time if this is earlier
			if firstTime.IsZero() || firstTickTime.Before(firstTime) {
				firstTime = firstTickTime
			}

			// Update last time if this is later
			if lastTime.IsZero() || lastTickTime.After(lastTime) {
				lastTime = lastTickTime
			}
		}
	}

	// Convert unique dates map to slice
	for date := range uniqueDatesMap {
		result.UniqueDates = append(result.UniqueDates, date)
	}

	// Sort dates in descending order (newest first)
	sort.Slice(result.UniqueDates, func(i, j int) bool {
		return result.UniqueDates[i] > result.UniqueDates[j]
	})

	// Set result
	if !firstTime.IsZero() {
		result.FirstTickTime = firstTime.Format(time.RFC3339)
	}
	if !lastTime.IsZero() {
		result.LastTickTime = lastTime.Format(time.RFC3339)
	}
	result.TotalTickCount = totalTicks

	return result, nil
}

// getArchiveJobStats gets statistics about archive jobs
func getArchiveJobStats(ctx context.Context) (struct {
	JobsSummary struct {
		TotalJobs      int `json:"total_jobs"`
		CompletedJobs  int `json:"completed_jobs"`
		FailedJobs     int `json:"failed_jobs"`
		InProgressJobs int `json:"in_progress_jobs"`
	} `json:"summary"`
	RecentJobs []struct {
		JobID         string    `json:"job_id"`
		IndexName     string    `json:"index_name"`
		Status        string    `json:"status"`
		ExecutedAt    time.Time `json:"executed_at"`
		CompletedAt   time.Time `json:"completed_at,omitempty"`
		TickStartTime time.Time `json:"tick_start_time"`
		TickEndTime   time.Time `json:"tick_end_time"`
		TickCount     int       `json:"tick_count"`
		FilePath      string    `json:"file_path,omitempty"`
		FileSizeBytes int64     `json:"file_size_bytes,omitempty"`
	} `json:"recent_jobs"`
}, error) {
	var result struct {
		JobsSummary struct {
			TotalJobs      int `json:"total_jobs"`
			CompletedJobs  int `json:"completed_jobs"`
			FailedJobs     int `json:"failed_jobs"`
			InProgressJobs int `json:"in_progress_jobs"`
		} `json:"summary"`
		RecentJobs []struct {
			JobID         string    `json:"job_id"`
			IndexName     string    `json:"index_name"`
			Status        string    `json:"status"`
			ExecutedAt    time.Time `json:"executed_at"`
			CompletedAt   time.Time `json:"completed_at,omitempty"`
			TickStartTime time.Time `json:"tick_start_time"`
			TickEndTime   time.Time `json:"tick_end_time"`
			TickCount     int       `json:"tick_count"`
			FilePath      string    `json:"file_path,omitempty"`
			FileSizeBytes int64     `json:"file_size_bytes,omitempty"`
		} `json:"recent_jobs"`
	}

	// Query database for job statistics summary
	summaryQuery := `
		SELECT 
			COUNT(*) as total_jobs,
			SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_jobs,
			SUM(CASE WHEN status IN ('failed', 'failed_permanent') THEN 1 ELSE 0 END) as failed_jobs,
			SUM(CASE WHEN status IN ('pending', 'running') THEN 1 ELSE 0 END) as in_progress_jobs
		FROM tick_archive_jobs
	`

	err := db.GetTimescaleDB().GetPool().QueryRow(ctx, summaryQuery).Scan(
		&result.JobsSummary.TotalJobs,
		&result.JobsSummary.CompletedJobs,
		&result.JobsSummary.FailedJobs,
		&result.JobsSummary.InProgressJobs,
	)
	if err != nil {
		// If the table doesn't exist yet, return empty stats
		result.JobsSummary.TotalJobs = 0
		result.JobsSummary.CompletedJobs = 0
		result.JobsSummary.FailedJobs = 0
		result.JobsSummary.InProgressJobs = 0
		return result, nil
	}

	// Query database for recent jobs
	recentJobsQuery := `
		SELECT 
			job_id, 
			index_name, 
			status, 
			started_at as executed_at, 
			completed_at, 
			start_time as tick_start_time, 
			end_time as tick_end_time, 
			tick_count, 
			file_path, 
			file_size_bytes
		FROM tick_archive_jobs
		WHERE status = 'completed'
		ORDER BY completed_at DESC
		LIMIT 10
	`

	rows, err := db.GetTimescaleDB().GetPool().Query(ctx, recentJobsQuery)
	if err != nil {
		// If there's an error, just return the summary without recent jobs
		return result, nil
	}
	defer rows.Close()

	// Parse recent jobs
	for rows.Next() {
		var job struct {
			JobID         string    `json:"job_id"`
			IndexName     string    `json:"index_name"`
			Status        string    `json:"status"`
			ExecutedAt    time.Time `json:"executed_at"`
			CompletedAt   time.Time `json:"completed_at,omitempty"`
			TickStartTime time.Time `json:"tick_start_time"`
			TickEndTime   time.Time `json:"tick_end_time"`
			TickCount     int       `json:"tick_count"`
			FilePath      string    `json:"file_path,omitempty"`
			FileSizeBytes int64     `json:"file_size_bytes,omitempty"`
		}

		// Use nullable types for optional fields
		var completedAt sql.NullTime
		var filePath sql.NullString
		var tickCount sql.NullInt32
		var fileSizeBytes sql.NullInt64

		err := rows.Scan(
			&job.JobID,
			&job.IndexName,
			&job.Status,
			&job.ExecutedAt,
			&completedAt,
			&job.TickStartTime,
			&job.TickEndTime,
			&tickCount,
			&filePath,
			&fileSizeBytes,
		)
		if err != nil {
			// Skip this job if there's an error
			continue
		}

		// Set nullable fields
		if completedAt.Valid {
			job.CompletedAt = completedAt.Time
		}
		if tickCount.Valid {
			job.TickCount = int(tickCount.Int32)
		}
		if filePath.Valid {
			job.FilePath = filePath.String
		}
		if fileSizeBytes.Valid {
			job.FileSizeBytes = fileSizeBytes.Int64
		}

		// Add job to result
		result.RecentJobs = append(result.RecentJobs, job)
	}

	return result, nil
}

// getArchivedFileStats gets statistics about archived files
func getArchivedFileStats() (struct {
	TotalFiles     int     `json:"total_files"`
	TotalSizeMB    float64 `json:"total_size_mb"`
	NIFTYFiles     int     `json:"nifty_files"`
	SENSEXFiles    int     `json:"sensex_files"`
	LatestArchiveAt string  `json:"latest_archive_at,omitempty"`
}, error) {
	var result struct {
		TotalFiles     int     `json:"total_files"`
		TotalSizeMB    float64 `json:"total_size_mb"`
		NIFTYFiles     int     `json:"nifty_files"`
		SENSEXFiles    int     `json:"sensex_files"`
		LatestArchiveAt string  `json:"latest_archive_at,omitempty"`
	}

	// Get the archive directory
	archiveDir := filepath.Join(filestore.DefaultParquetDir, "archive")

	// List files in the archive directory
	files, err := filepath.Glob(filepath.Join(archiveDir, "*.parquet"))
	if err != nil {
		return result, fmt.Errorf("failed to list archived files: %w", err)
	}

	// Calculate statistics
	var totalSize int64
	var latestModTime time.Time

	for _, file := range files {
		// Get file info
		fileInfo, err := os.Stat(file)
		if err != nil {
			continue
		}

		// Update total size
		totalSize += fileInfo.Size()

		// Count by index
		if filepath.Base(file)[:5] == "NIFTY" {
			result.NIFTYFiles++
		} else if filepath.Base(file)[:6] == "SENSEX" {
			result.SENSEXFiles++
		}

		// Update latest modification time
		if fileInfo.ModTime().After(latestModTime) {
			latestModTime = fileInfo.ModTime()
		}
	}

	// Set result
	result.TotalFiles = len(files)
	result.TotalSizeMB = float64(totalSize) / (1024 * 1024)
	if !latestModTime.IsZero() {
		result.LatestArchiveAt = latestModTime.Format(time.RFC3339)
	}

	return result, nil
}

// RegisterTickDataDashboardEndpoints registers the tick data dashboard endpoints
func RegisterTickDataDashboardEndpoints(router *http.ServeMux) {
	router.HandleFunc("/api/tick-data/dashboard", handleGetTickDataDashboard)
}
