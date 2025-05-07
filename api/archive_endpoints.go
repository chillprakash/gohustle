package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"gohustle/archive"
	"gohustle/db"
	"gohustle/logger"
	"io"
	"net/http"
	"strconv"
	"time"
)

// ArchiveJobResponse represents the response for archive job endpoints
type ArchiveJobResponse struct {
	ID            int        `json:"id"`
	JobID         string     `json:"job_id"`
	IndexName     string     `json:"index_name"`
	StartTime     time.Time  `json:"start_time"`
	EndTime       time.Time  `json:"end_time"`
	Status        string     `json:"status"`
	CreatedAt     time.Time  `json:"created_at"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	TickCount     *int       `json:"tick_count,omitempty"`
	FilePath      *string    `json:"file_path,omitempty"`
	FileSizeBytes *int64     `json:"file_size_bytes,omitempty"`
	ErrorMessage  *string    `json:"error_message,omitempty"`
	RetryCount    int        `json:"retry_count"`
	NextRetryAt   *time.Time `json:"next_retry_at,omitempty"`
}

// RetryJobRequest represents a request to retry a failed job
type RetryJobRequest struct {
	JobID string `json:"job_id"`
}

// handleGetArchiveJobs returns the status of archive jobs
func handleGetArchiveJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodOptions {
		sendErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Handle CORS preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Get query parameters
	status := r.URL.Query().Get("status")
	indexName := r.URL.Query().Get("index_name")
	limit := 50 // Default limit
	
	// Parse limit if provided
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	// Build query based on filters
	query := `
		SELECT id, job_id, index_name, start_time, end_time, status, 
		       created_at, started_at, completed_at, tick_count, 
		       file_path, file_size_bytes, error_message, retry_count, next_retry_at
		FROM tick_archive_jobs
		WHERE 1=1
	`
	args := []interface{}{}
	argIndex := 1

	if status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIndex)
		args = append(args, status)
		argIndex++
	}

	if indexName != "" {
		query += fmt.Sprintf(" AND index_name = $%d", argIndex)
		args = append(args, indexName)
		argIndex++
	}

	query += " ORDER BY created_at DESC LIMIT " + strconv.Itoa(limit)

	// Execute query
	rows, err := db.GetTimescaleDB().GetPool().Query(r.Context(), query, args...)
	if err != nil {
		logger.L().Error("Failed to query archive jobs", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Failed to query archive jobs", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Parse results
	var jobs []ArchiveJobResponse
	for rows.Next() {
		var job ArchiveJobResponse
		var startedAt, completedAt, nextRetryAt interface{}
		
		// Use sql.Null types to handle nullable fields
	var tickCount sql.NullInt64
	var filePath sql.NullString
	var fileSizeBytes sql.NullInt64
	var errorMessage sql.NullString

	err := rows.Scan(
			&job.ID,
			&job.JobID,
			&job.IndexName,
			&job.StartTime,
			&job.EndTime,
			&job.Status,
			&job.CreatedAt,
			&startedAt,
			&completedAt,
			&tickCount,
			&filePath,
			&fileSizeBytes,
			&errorMessage,
			&job.RetryCount,
			&nextRetryAt,
		)
		
	// Convert sql.Null types to pointer types
	if tickCount.Valid {
		tCount := int(tickCount.Int64)
		job.TickCount = &tCount
	}
	if filePath.Valid {
		fPath := filePath.String
		job.FilePath = &fPath
	}
	if fileSizeBytes.Valid {
		fSize := fileSizeBytes.Int64
		job.FileSizeBytes = &fSize
	}
	if errorMessage.Valid {
		errMsg := errorMessage.String
		job.ErrorMessage = &errMsg
	}
		if err != nil {
			logger.L().Error("Failed to scan job row", map[string]interface{}{
				"error": err.Error(),
			})
			continue
		}

		// Handle nullable fields
		if startedAt != nil {
			if t, ok := startedAt.(time.Time); ok {
				tCopy := t // Create a copy to avoid referencing a loop variable
				job.StartedAt = &tCopy
			}
		}
		
		if completedAt != nil {
			if t, ok := completedAt.(time.Time); ok {
				tCopy := t // Create a copy to avoid referencing a loop variable
				job.CompletedAt = &tCopy
			}
		}
		
		if nextRetryAt != nil {
			if t, ok := nextRetryAt.(time.Time); ok {
				tCopy := t // Create a copy to avoid referencing a loop variable
				job.NextRetryAt = &tCopy
			}
		}

		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		logger.L().Error("Error iterating job rows", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, "Error retrieving archive jobs", http.StatusInternalServerError)
		return
	}

	// Return as JSON response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "Archive jobs retrieved successfully",
		Data:    jobs,
	})
}

// handleRetryArchiveJob manually retries a failed job
func handleRetryArchiveJob(w http.ResponseWriter, r *http.Request) {
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
	var req RetryJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Validate request
	if req.JobID == "" {
		sendErrorResponse(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	// Get job by ID
	query := `
		SELECT id, status FROM tick_archive_jobs
		WHERE job_id = $1
	`
	var jobID int
	var status string
	err := db.GetTimescaleDB().QueryRow(r.Context(), query, req.JobID).Scan(&jobID, &status)
	if err != nil {
		logger.L().Error("Failed to get job by ID", map[string]interface{}{
			"job_id": req.JobID,
			"error":  err.Error(),
		})
		sendErrorResponse(w, "Job not found", http.StatusNotFound)
		return
	}

	// Check if job is in a failed state
	if status != "failed" && status != "failed_permanent" {
		sendErrorResponse(w, "Only failed jobs can be retried", http.StatusBadRequest)
		return
	}

	// Update job for retry
	updateQuery := `
		UPDATE tick_archive_jobs
		SET status = 'pending',
		    error_message = NULL,
		    next_retry_at = NULL,
		    started_at = NULL,
		    completed_at = NULL
		WHERE id = $1
	`
	_, err = db.GetTimescaleDB().Exec(r.Context(), updateQuery, jobID)
	if err != nil {
		logger.L().Error("Failed to update job for retry", map[string]interface{}{
			"job_id": req.JobID,
			"error":  err.Error(),
		})
		sendErrorResponse(w, "Failed to update job for retry", http.StatusInternalServerError)
		return
	}

	// Return success response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "Job scheduled for retry",
		Data: map[string]interface{}{
			"job_id": req.JobID,
		},
	})
}

// RunArchiveJobRequest represents a request to run an archive job for a specific index
type RunArchiveJobRequest struct {
	IndexName string `json:"index_name"` // Optional, if empty will run for all indices
}

// handleRunArchiveJob manually triggers an archive job
func handleRunArchiveJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodOptions {
		sendErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Handle CORS preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Parse request body to get index name (if specified)
	var req RunArchiveJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		logger.L().Error("Failed to parse request body", map[string]interface{}{
			"error": err.Error(),
		})
		// Continue with empty index name (will run for all indices)
	}

	// Run the archive job in a goroutine
	go func() {
		ctx := context.Background()
		if req.IndexName != "" {
			// Run for specific index
			if err := archive.ExecuteTickArchiveJobForIndex(ctx, req.IndexName); err != nil {
				logger.L().Error("Failed to run archive job for index", map[string]interface{}{
					"index_name": req.IndexName,
					"error":      err.Error(),
				})
			}
		} else {
			// Run for all indices
			if err := archive.ExecuteTickArchiveJob(ctx); err != nil {
				logger.L().Error("Failed to run archive job", map[string]interface{}{
					"error": err.Error(),
				})
			}
		}
	}()

	// Return success response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "Archive job triggered for " + func() string {
			if req.IndexName != "" {
				return req.IndexName
			}
			return "all indices"
		}(),
	})
}

// handleRunConsolidationJob manually triggers a consolidation job
func handleRunConsolidationJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodOptions {
		sendErrorResponse(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Handle CORS preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Run the consolidation job in a goroutine
	go func() {
		ctx := context.Background()
		if err := archive.ExecuteTickConsolidationJob(ctx); err != nil {
			logger.L().Error("Failed to run consolidation job", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}()

	// Return success response
	sendJSONResponse(w, Response{
		Success: true,
		Message: "Consolidation job triggered",
	})
}
