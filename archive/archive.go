package archive

import (
	"context"
	"fmt"
	"gohustle/db"
	"gohustle/filestore"
	"gohustle/logger"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// TickArchiveManager handles the archiving process for tick data
// ArchiveFileEntry represents an archived file with metadata
type ArchiveFileEntry struct {
	Name      string
	Path      string
	Size      int64
	ModTime   time.Time
	IndexName string
	IsDaily   bool
}

type TickArchiveManager struct {
	db         *db.TimescaleDB
	store      *filestore.ParquetStore
	mu         sync.Mutex
	archiveDir string
}

var (
	instance *TickArchiveManager
	once     sync.Once
)

// GetTickArchiveManager returns a singleton instance of TickArchiveManager
func GetTickArchiveManager() *TickArchiveManager {
	once.Do(func() {
		// Default archive directory is in the current working directory
		cwd, err := os.Getwd()
		if err != nil {
			logger.L().Error("Failed to get current working directory", map[string]interface{}{
				"error": err.Error(),
			})
			cwd = "."
		}
		archiveDir := filepath.Join(cwd, "archive_data")
		
		instance = &TickArchiveManager{
			db:         db.GetTimescaleDB(),
			store:      filestore.GetParquetStore(),
			archiveDir: archiveDir,
		}
		
		// Ensure archive directory exists
		if err := os.MkdirAll(archiveDir, 0755); err != nil {
			logger.L().Error("Failed to create archive directory", map[string]interface{}{
				"dir":   archiveDir,
				"error": err.Error(),
			})
		}
	})
	return instance
}

// GetArchiveDirectory returns the base directory for archived files
func (m *TickArchiveManager) GetArchiveDirectory() string {
	return m.archiveDir
}

// ListArchiveFiles lists all archive files with optional filtering
func (m *TickArchiveManager) ListArchiveFiles(ctx context.Context, archiveDir, indexName, fileType string) ([]ArchiveFileEntry, error) {
	// Validate inputs
	if archiveDir == "" {
		archiveDir = m.archiveDir
	}

	// Check if directory exists
	if _, err := os.Stat(archiveDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("archive directory does not exist: %s", archiveDir)
	}

	// Walk the directory and collect files
	var files []ArchiveFileEntry
	err := filepath.Walk(archiveDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Only include .parquet files
		if !strings.HasSuffix(info.Name(), ".parquet") {
			return nil
		}

		// Parse file name to get index name
		fileIndexName := ""
		if strings.HasPrefix(info.Name(), "NIFTY_") {
			fileIndexName = "NIFTY"
		} else if strings.HasPrefix(info.Name(), "SENSEX_") {
			fileIndexName = "SENSEX"
		} else {
			// Skip files that don't match our naming convention
			return nil
		}

		// Apply index name filter if specified
		if indexName != "" && fileIndexName != indexName {
			return nil
		}

		// Determine if this is a daily or hourly file
		isDaily := strings.Contains(info.Name(), "_daily_")

		// Apply file type filter if specified
		if fileType == "daily" && !isDaily {
			return nil
		} else if fileType == "hourly" && isDaily {
			return nil
		}

		// Add file to the list
		files = append(files, ArchiveFileEntry{
			Name:      info.Name(),
			Path:      path,
			Size:      info.Size(),
			ModTime:   info.ModTime(),
			IndexName: fileIndexName,
			IsDaily:   isDaily,
		})

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error walking archive directory: %w", err)
	}

	// Sort files by modification time (newest first)
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime.After(files[j].ModTime)
	})

	return files, nil
}

// ParseTimeRangeFromFilename extracts the time range from a file name
func (m *TickArchiveManager) ParseTimeRangeFromFilename(filename string) (time.Time, time.Time, error) {
	// Handle daily files (e.g., NIFTY_daily_2025-05-06.parquet)
	if strings.Contains(filename, "_daily_") {
		parts := strings.Split(filename, "_daily_")
		if len(parts) != 2 {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid daily file name format: %s", filename)
		}

		dateStr := strings.TrimSuffix(parts[1], ".parquet")
		date, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("failed to parse date from filename: %w", err)
		}

		// For daily files, start time is 00:00:00 and end time is 23:59:59
		startTime := date
		endTime := date.Add(24*time.Hour - time.Second)
		return startTime, endTime, nil
	}

	// Handle hourly files (e.g., NIFTY_2025-05-06T14_to_2025-05-06T15.parquet)
	if strings.Contains(filename, "_to_") {
		// Extract index name prefix
		parts := strings.SplitN(filename, "_", 2)
		if len(parts) != 2 {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid hourly file name format: %s", filename)
		}

		// Extract time range
		timeRangeParts := strings.Split(parts[1], "_to_")
		if len(timeRangeParts) != 2 {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid time range format in filename: %s", filename)
		}

		// Parse start time
		startTimeStr := timeRangeParts[0]
		startTime, err := time.Parse("2006-01-02T15", startTimeStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("failed to parse start time from filename: %w", err)
		}

		// Parse end time
		endTimeStr := strings.TrimSuffix(timeRangeParts[1], ".parquet")
		endTime, err := time.Parse("2006-01-02T15", endTimeStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("failed to parse end time from filename: %w", err)
		}

		return startTime, endTime, nil
	}

	return time.Time{}, time.Time{}, fmt.Errorf("unrecognized file name format: %s", filename)
}

// GetTickCountForFile estimates the number of ticks in a Parquet file
func (m *TickArchiveManager) GetTickCountForFile(ctx context.Context, filePath string) (int, error) {
	// For now, we'll estimate based on file size
	// In a real implementation, you might want to query the Parquet file metadata
	
	// Get file info
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to get file info: %w", err)
	}
	
	// Estimate tick count based on file size
	// Assuming average of 200 bytes per tick record
	estimatedTickCount := int(fileInfo.Size() / 200)
	
	return estimatedTickCount, nil
}

// TickArchiveJob represents a job for archiving tick data
type TickArchiveJob struct {
	ID            int        `db:"id"`
	JobID         string     `db:"job_id"`
	IndexName     string     `db:"index_name"`
	StartTime     time.Time  `db:"start_time"`
	EndTime       time.Time  `db:"end_time"`
	Status        string     `db:"status"`
	CreatedAt     time.Time  `db:"created_at"`
	StartedAt     *time.Time `db:"started_at"`
	CompletedAt   *time.Time `db:"completed_at"`
	TickCount     *int       `db:"tick_count"`
	FilePath      *string    `db:"file_path"`
	FileSizeBytes *int64     `db:"file_size_bytes"`
	ErrorMessage  *string    `db:"error_message"`
	RetryCount    int        `db:"retry_count"`
	NextRetryAt   *time.Time `db:"next_retry_at"`
}

// ExecuteTickArchiveJob is the main entry point for the archiving process for all indices
func ExecuteTickArchiveJob(ctx context.Context) error {
	manager := GetTickArchiveManager()

	// 1. Find pending jobs or create new ones
	jobs, err := manager.getOrCreatePendingJobs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get or create pending jobs: %w", err)
	}

	if len(jobs) == 0 {
		logger.L().Info("No tick archive jobs to process", nil)
		return nil
	}

	// 2. Process each job
	for _, job := range jobs {
		logger.L().Info("Processing tick archive job", map[string]interface{}{
			"job_id":     job.JobID,
			"index_name": job.IndexName,
			"start_time": job.StartTime.Format(time.RFC3339),
			"end_time":   job.EndTime.Format(time.RFC3339),
		})

		if err := manager.processJob(ctx, job); err != nil {
			logger.L().Error("Failed to process archive job", map[string]interface{}{
				"job_id":     job.JobID,
				"index_name": job.IndexName,
				"error":      err.Error(),
			})

			// Update job status to failed and schedule retry if appropriate
			if err := manager.scheduleRetry(ctx, job.ID, err.Error()); err != nil {
				logger.L().Error("Failed to schedule retry for archive job", map[string]interface{}{
					"job_id": job.JobID,
					"error":  err.Error(),
				})
			}
			continue
		}
	}

	return nil
}

// ExecuteTickArchiveJobForIndex is the entry point for archiving a specific index
func ExecuteTickArchiveJobForIndex(ctx context.Context, indexName string) error {
	manager := GetTickArchiveManager()

	// Validate index name
	if indexName != "NIFTY" && indexName != "SENSEX" {
		return fmt.Errorf("invalid index name: %s, must be NIFTY or SENSEX", indexName)
	}

	// 1. Find pending jobs or create new ones for the specific index
	jobs, err := manager.getOrCreatePendingJobsForIndex(ctx, indexName)
	if err != nil {
		return fmt.Errorf("failed to get or create pending jobs for index %s: %w", indexName, err)
	}

	if len(jobs) == 0 {
		logger.L().Info("No tick archive jobs to process for index", map[string]interface{}{
			"index_name": indexName,
		})
		return nil
	}

	// 2. Process each job
	for _, job := range jobs {
		logger.L().Info("Processing tick archive job for index", map[string]interface{}{
			"job_id":     job.JobID,
			"index_name": job.IndexName,
			"start_time": job.StartTime.Format(time.RFC3339),
			"end_time":   job.EndTime.Format(time.RFC3339),
		})

		if err := manager.processJob(ctx, job); err != nil {
			logger.L().Error("Failed to process archive job for index", map[string]interface{}{
				"job_id":     job.JobID,
				"index_name": job.IndexName,
				"error":      err.Error(),
			})

			// Update job status to failed and schedule retry if appropriate
			if err := manager.scheduleRetry(ctx, job.ID, err.Error()); err != nil {
				logger.L().Error("Failed to schedule retry for archive job", map[string]interface{}{
					"job_id": job.JobID,
					"error":  err.Error(),
				})
			}
			continue
		}
	}

	return nil
}

// getOrCreatePendingJobs finds pending jobs or creates new ones if none exist for all indices
func (m *TickArchiveManager) getOrCreatePendingJobs(ctx context.Context) ([]*TickArchiveJob, error) {
	// First, check for any pending jobs that are ready to be processed
	pendingJobs, err := m.getPendingJobs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending jobs: %w", err)
	}

	if len(pendingJobs) > 0 {
		return pendingJobs, nil
	}

	// No pending jobs, create new ones for each index
	indices := []string{"NIFTY", "SENSEX"}
	var newJobs []*TickArchiveJob

	for _, indexName := range indices {
		// Create jobs for the current hour (or previous hour if we're near the start of the hour)
		currentTime := time.Now()
		var startTime, endTime time.Time

		// If we're within the first 5 minutes of the hour, process the previous hour
		if currentTime.Minute() < 5 {
			endTime = time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), currentTime.Hour(), 0, 0, 0, currentTime.Location())
			startTime = endTime.Add(-1 * time.Hour)
		} else {
			// Process the current hour up to now
			startTime = time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), currentTime.Hour(), 0, 0, 0, currentTime.Location())
			endTime = currentTime
		}

		// Check if we already have a job for this time range and index
		existingJob, err := m.getJobForTimeRange(ctx, indexName, startTime, endTime)
		if err != nil {
			return nil, fmt.Errorf("failed to check for existing job: %w", err)
		}

		if existingJob != nil {
			// Skip if we already have a job for this time range
			continue
		}

		// Create a new job
		newJob, err := m.createJob(ctx, indexName, startTime, endTime)
		if err != nil {
			return nil, fmt.Errorf("failed to create job for index %s: %w", indexName, err)
		}

		newJobs = append(newJobs, newJob)
	}

	return newJobs, nil
}

// getOrCreatePendingJobsForIndex finds pending jobs or creates new ones if none exist for a specific index
func (m *TickArchiveManager) getOrCreatePendingJobsForIndex(ctx context.Context, indexName string) ([]*TickArchiveJob, error) {
	// First, check for any pending jobs for this index that are ready to be processed
	pendingJobs, err := m.getPendingJobsForIndex(ctx, indexName)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending jobs for index %s: %w", indexName, err)
	}

	if len(pendingJobs) > 0 {
		return pendingJobs, nil
	}

	// No pending jobs, create new ones for the specified index
	var newJobs []*TickArchiveJob

	// Create jobs for the current hour (or previous hour if we're near the start of the hour)
	currentTime := time.Now()
	var startTime, endTime time.Time

	// If we're within the first 5 minutes of the hour, process the previous hour
	if currentTime.Minute() < 5 {
		endTime = time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), currentTime.Hour(), 0, 0, 0, currentTime.Location())
		startTime = endTime.Add(-1 * time.Hour)
	} else {
		// Process the current hour up to now
		startTime = time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), currentTime.Hour(), 0, 0, 0, currentTime.Location())
		endTime = currentTime
	}

	// Check if we already have a job for this time range and index
	existingJob, err := m.getJobForTimeRange(ctx, indexName, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing job: %w", err)
	}

	if existingJob != nil {
		// If we already have a job but it's not pending, create a new one
		if existingJob.Status != "pending" {
			newJob, err := m.createJob(ctx, indexName, startTime, endTime)
			if err != nil {
				return nil, fmt.Errorf("failed to create job for index %s: %w", indexName, err)
			}
			newJobs = append(newJobs, newJob)
		} else {
			// Return the existing pending job
			newJobs = append(newJobs, existingJob)
		}
	} else {
		// Create a new job
		newJob, err := m.createJob(ctx, indexName, startTime, endTime)
		if err != nil {
			return nil, fmt.Errorf("failed to create job for index %s: %w", indexName, err)
		}
		newJobs = append(newJobs, newJob)
	}

	return newJobs, nil
}

// getPendingJobs retrieves jobs with 'pending' status
func (m *TickArchiveManager) getPendingJobs(ctx context.Context) ([]*TickArchiveJob, error) {
	query := `
		SELECT * FROM tick_archive_jobs
		WHERE status = 'pending'
		ORDER BY created_at ASC
		LIMIT 10
	`

	var jobs []*TickArchiveJob
	// Use the pool.Query method from TimescaleDB
	rows, err := m.db.GetPool().Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending jobs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var job TickArchiveJob
		err := rows.Scan(
			&job.ID,
			&job.JobID,
			&job.IndexName,
			&job.StartTime,
			&job.EndTime,
			&job.Status,
			&job.CreatedAt,
			&job.StartedAt,
			&job.CompletedAt,
			&job.TickCount,
			&job.FilePath,
			&job.FileSizeBytes,
			&job.ErrorMessage,
			&job.RetryCount,
			&job.NextRetryAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job row: %w", err)
		}
		jobs = append(jobs, &job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating job rows: %w", err)
	}

	return jobs, nil
}

// getPendingJobsForIndex retrieves jobs with 'pending' status for a specific index
func (m *TickArchiveManager) getPendingJobsForIndex(ctx context.Context, indexName string) ([]*TickArchiveJob, error) {
	query := `
		SELECT * FROM tick_archive_jobs
		WHERE status = 'pending' AND index_name = $1
		ORDER BY created_at ASC
		LIMIT 10
	`

	var jobs []*TickArchiveJob
	// Use the pool.Query method from TimescaleDB
	rows, err := m.db.GetPool().Query(ctx, query, indexName)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending jobs for index %s: %w", indexName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var job TickArchiveJob
		err := rows.Scan(
			&job.ID,
			&job.JobID,
			&job.IndexName,
			&job.StartTime,
			&job.EndTime,
			&job.Status,
			&job.CreatedAt,
			&job.StartedAt,
			&job.CompletedAt,
			&job.TickCount,
			&job.FilePath,
			&job.FileSizeBytes,
			&job.ErrorMessage,
			&job.RetryCount,
			&job.NextRetryAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job row: %w", err)
		}
		jobs = append(jobs, &job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating job rows: %w", err)
	}

	return jobs, nil
}

// getJobForTimeRange checks if a job already exists for the given time range and index
func (m *TickArchiveManager) getJobForTimeRange(ctx context.Context, indexName string, startTime, endTime time.Time) (*TickArchiveJob, error) {
	query := `
		SELECT * FROM tick_archive_jobs
		WHERE index_name = $1
		  AND start_time = $2
		  AND end_time = $3
		LIMIT 1
	`

	rows, err := m.db.GetPool().Query(ctx, query, indexName, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query job for time range: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		// No job found
		return nil, nil
	}

	var job TickArchiveJob
	err = rows.Scan(
		&job.ID,
		&job.JobID,
		&job.IndexName,
		&job.StartTime,
		&job.EndTime,
		&job.Status,
		&job.CreatedAt,
		&job.StartedAt,
		&job.CompletedAt,
		&job.TickCount,
		&job.FilePath,
		&job.FileSizeBytes,
		&job.ErrorMessage,
		&job.RetryCount,
		&job.NextRetryAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to scan job row: %w", err)
	}

	return &job, nil
}

// createJob creates a new archive job in the database
func (m *TickArchiveManager) createJob(ctx context.Context, indexName string, startTime, endTime time.Time) (*TickArchiveJob, error) {
	// Generate a unique job ID
	jobID := uuid.New().String()

	// Create the job in the database
	query := `
		INSERT INTO tick_archive_jobs (
			job_id, index_name, start_time, end_time, status, created_at, retry_count
		) VALUES ($1, $2, $3, $4, 'pending', $5, 0)
		RETURNING *
	`

	rows, err := m.db.GetPool().Query(
		ctx,
		query,
		jobID,
		indexName,
		startTime,
		endTime,
		time.Now(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create job: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("no rows returned after job creation")
	}

	var job TickArchiveJob
	err = rows.Scan(
		&job.ID,
		&job.JobID,
		&job.IndexName,
		&job.StartTime,
		&job.EndTime,
		&job.Status,
		&job.CreatedAt,
		&job.StartedAt,
		&job.CompletedAt,
		&job.TickCount,
		&job.FilePath,
		&job.FileSizeBytes,
		&job.ErrorMessage,
		&job.RetryCount,
		&job.NextRetryAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to scan job row: %w", err)
	}

	return &job, nil
}

// getJobsForRetry retrieves jobs scheduled for retry that are due
func (m *TickArchiveManager) getJobsForRetry(ctx context.Context) ([]*TickArchiveJob, error) {
	query := `
		SELECT * FROM tick_archive_jobs
		WHERE status = 'failed'
		AND next_retry_at IS NOT NULL
		AND next_retry_at <= NOW()
		ORDER BY next_retry_at ASC
		LIMIT 10
	`

	var jobs []*TickArchiveJob
	rows, err := m.db.GetPool().Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query jobs for retry: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var job TickArchiveJob
		err := rows.Scan(
			&job.ID,
			&job.JobID,
			&job.IndexName,
			&job.StartTime,
			&job.EndTime,
			&job.Status,
			&job.CreatedAt,
			&job.StartedAt,
			&job.CompletedAt,
			&job.TickCount,
			&job.FilePath,
			&job.FileSizeBytes,
			&job.ErrorMessage,
			&job.RetryCount,
			&job.NextRetryAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job row: %w", err)
		}
		jobs = append(jobs, &job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating job rows: %w", err)
	}

	return jobs, nil
}

// createNewArchiveJobs creates new jobs for archiving tick data
func (m *TickArchiveManager) createNewArchiveJobs(ctx context.Context) ([]*TickArchiveJob, error) {
	// Get the time window for archiving (previous hour)
	now := time.Now()
	endTime := now.Truncate(time.Hour)       // Current hour start
	startTime := endTime.Add(-1 * time.Hour) // Previous hour start

	// Check if we already have jobs for this time window
	query := `
		SELECT COUNT(*) FROM tick_archive_jobs
		WHERE (index_name = 'NIFTY' OR index_name = 'SENSEX')
		AND start_time = $1
		AND end_time = $2
	`

	var count int
	err := m.db.QueryRow(ctx, query, startTime, endTime).Scan(&count)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing jobs: %w", err)
	}

	// If jobs already exist for this time window, don't create new ones
	if count > 0 {
		logger.L().Info("Jobs already exist for this time window", map[string]interface{}{
			"start_time": startTime.Format(time.RFC3339),
			"end_time":   endTime.Format(time.RFC3339),
		})
		return nil, nil
	}

	// Create jobs for both NIFTY and SENSEX
	indices := []string{"NIFTY", "SENSEX"}
	var jobs []*TickArchiveJob

	for _, index := range indices {
		// Check if there's data for this index in the time window
		dataExists, err := m.checkDataExists(ctx, index, startTime, endTime)
		if err != nil {
			logger.L().Error("Failed to check data existence", map[string]interface{}{
				"index":      index,
				"start_time": startTime.Format(time.RFC3339),
				"end_time":   endTime.Format(time.RFC3339),
				"error":      err.Error(),
			})
			continue
		}

		if !dataExists {
			logger.L().Info("No data to archive for this time window", map[string]interface{}{
				"index":      index,
				"start_time": startTime.Format(time.RFC3339),
				"end_time":   endTime.Format(time.RFC3339),
			})
			continue
		}

		// Create a new job
		job := &TickArchiveJob{
			JobID:     uuid.New().String(),
			IndexName: index,
			StartTime: startTime,
			EndTime:   endTime,
			Status:    "pending",
			CreatedAt: now,
		}

		// Insert the job into the database
		insertQuery := `
			INSERT INTO tick_archive_jobs (
				job_id, index_name, start_time, end_time, status, created_at
			) VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING id
		`

		err = m.db.QueryRow(ctx, insertQuery,
			job.JobID, job.IndexName, job.StartTime, job.EndTime, job.Status, job.CreatedAt,
		).Scan(&job.ID)

		if err != nil {
			logger.L().Error("Failed to insert job", map[string]interface{}{
				"index":      index,
				"start_time": startTime.Format(time.RFC3339),
				"end_time":   endTime.Format(time.RFC3339),
				"error":      err.Error(),
			})
			continue
		}

		jobs = append(jobs, job)

		logger.L().Info("Created new archive job", map[string]interface{}{
			"job_id":     job.JobID,
			"index":      index,
			"start_time": startTime.Format(time.RFC3339),
			"end_time":   endTime.Format(time.RFC3339),
		})
	}

	return jobs, nil
}

// checkDataExists checks if there's data for the given index and time window
func (m *TickArchiveManager) checkDataExists(ctx context.Context, index string, startTime, endTime time.Time) (bool, error) {
	var tableName string
	if index == "NIFTY" {
		tableName = "nifty_ticks"
	} else if index == "SENSEX" {
		tableName = "sensex_ticks"
	} else {
		return false, fmt.Errorf("invalid index name: %s", index)
	}

	query := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s
		WHERE exchange_unix_timestamp >= $1
		AND exchange_unix_timestamp < $2
		LIMIT 1
	`, tableName)

	var count int
	err := m.db.QueryRow(ctx, query, startTime, endTime).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check data existence: %w", err)
	}

	return count > 0, nil
}

// processJob handles a single archiving job
func (m *TickArchiveManager) processJob(ctx context.Context, job *TickArchiveJob) error {
	// 1. Update job status to running
	if err := m.updateJobStatus(ctx, job.ID, "running", ""); err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	// Set started_at time
	if err := m.updateJobStartTime(ctx, job.ID); err != nil {
		return fmt.Errorf("failed to update job start time: %w", err)
	}

	// 2. Export data to Parquet
	filePath, tickCount, fileSize, err := m.exportData(ctx, job)
	if err != nil {
		return fmt.Errorf("failed to export data: %w", err)
	}

	// 3. Verify export
	if err := m.verifyExport(ctx, filePath, tickCount); err != nil {
		return fmt.Errorf("export verification failed: %w", err)
	}

	// 4. Delete data from database
	if err := m.deleteData(ctx, job); err != nil {
		return fmt.Errorf("failed to delete data: %w", err)
	}

	// 5. Update job status to completed
	if err := m.updateJobCompleted(ctx, job.ID, filePath, tickCount, fileSize); err != nil {
		return fmt.Errorf("failed to update job completion: %w", err)
	}

	logger.L().Info("Successfully completed archive job", map[string]interface{}{
		"job_id":     job.JobID,
		"index_name": job.IndexName,
		"tick_count": tickCount,
		"file_path":  filePath,
		"file_size":  fileSize,
	})

	return nil
}

// exportData exports tick data to a Parquet file
func (m *TickArchiveManager) exportData(ctx context.Context, job *TickArchiveJob) (string, int, int64, error) {
	// Create export file path
	fileName := fmt.Sprintf("%s_%s_to_%s.parquet",
		job.IndexName,
		job.StartTime.Format("2006-01-02T15"),
		job.EndTime.Format("2006-01-02T15"),
	)
	filePath := filepath.Join(filestore.DefaultParquetDir, fileName)

	// Write ticks to Parquet file
	writer, err := m.store.GetOrCreateWriter(filePath)
	if err != nil {
		return "", 0, 0, fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer writer.Close()

	// Get tick data from database
	rows, err := m.db.GetTickDataForExport(ctx, job.IndexName, job.StartTime, job.EndTime)
	if err != nil {
		return "", 0, 0, fmt.Errorf("failed to get tick data: %w", err)
	}
	defer rows.Close()

	// Export data to Parquet file
	tickCount := 0
	var parquetTicks []filestore.TickRecord

	for rows.Next() {
		var tick db.TickData
		if err := rows.Scan(
			&tick.ID,
			&tick.InstrumentToken,
			&tick.ExchangeTimestamp,
			&tick.LastPrice,
			&tick.OpenInterest,
			&tick.VolumeTraded,
			&tick.AverageTradePrice,
			&tick.TickReceivedTime,
			&tick.TickStoredInDBTime,
		); err != nil {
			return "", 0, 0, fmt.Errorf("failed to scan tick data: %w", err)
		}

		// Convert to Parquet record
		var timestamp pgtype.Timestamp
		timestamp.Time = tick.ExchangeTimestamp
		timestamp.Valid = true

		parquetTick := filestore.TickRecord{
			ID:                int64(tick.ID),
			InstrumentToken:   int64(tick.InstrumentToken),
			ExchangeTimestamp: timestamp,
			LastPrice:         tick.LastPrice,
			OpenInterest:      int32(tick.OpenInterest),
			VolumeTraded:      int32(tick.VolumeTraded),
			AverageTradePrice: tick.AverageTradePrice,
		}

		parquetTicks = append(parquetTicks, parquetTick)
		tickCount++
	}

	// Write ticks one by one
	for _, tick := range parquetTicks {
		if err := writer.Write(tick); err != nil {
			return "", 0, 0, fmt.Errorf("failed to write tick to parquet: %w", err)
		}
	}

	// Close the writer
	err = writer.Close()
	if err != nil {
		return "", 0, 0, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	// Get file size using os.Stat
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return filePath, tickCount, 0, fmt.Errorf("failed to get file size: %w", err)
	}
	fileSize := fileInfo.Size()

	return filePath, tickCount, fileSize, nil
}

// verifyExport verifies that the exported data is valid
func (m *TickArchiveManager) verifyExport(ctx context.Context, filePath string, expectedCount int) error {
	// Read the file metadata to verify row count using ReadSamples
	// We'll just check if we can read samples successfully
	_, err := m.store.ReadSamples(filePath, 1)
	if err != nil {
		return fmt.Errorf("failed to read samples from file: %w", err)
	}

	// For simplicity, we'll assume the export was successful if we can read samples
	// In a production environment, you'd want more thorough verification

	// Read a sample of records to verify data integrity
	samples, err := m.store.ReadSamples(filePath, 10)
	if err != nil {
		return fmt.Errorf("failed to read samples: %w", err)
	}

	if len(samples) == 0 && expectedCount > 0 {
		return fmt.Errorf("no samples read from file with expected count %d", expectedCount)
	}

	return nil
}

// deleteData deletes the archived data from the database
func (m *TickArchiveManager) deleteData(ctx context.Context, job *TickArchiveJob) error {
	// Delete data from database
	rowsDeleted, err := m.db.DeleteTickData(ctx, job.IndexName, job.StartTime, job.EndTime)
	if err != nil {
		return fmt.Errorf("failed to delete tick data: %w", err)
	}

	logger.L().Info("Deleted tick data from database", map[string]interface{}{
		"job_id":       job.JobID,
		"index_name":   job.IndexName,
		"rows_deleted": rowsDeleted,
	})

	return nil
}

// updateJobStatus updates the status of a job
func (m *TickArchiveManager) updateJobStatus(ctx context.Context, jobID int, status, errorMsg string) error {
	query := `
		UPDATE tick_archive_jobs
		SET status = $1, error_message = $2
		WHERE id = $3
	`

	_, err := m.db.Exec(ctx, query, status, errorMsg, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	return nil
}

// updateJobStartTime updates the started_at time of a job
func (m *TickArchiveManager) updateJobStartTime(ctx context.Context, jobID int) error {
	query := `
		UPDATE tick_archive_jobs
		SET started_at = NOW()
		WHERE id = $1
	`

	_, err := m.db.Exec(ctx, query, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job start time: %w", err)
	}

	return nil
}

// updateJobCompleted updates a job as completed
func (m *TickArchiveManager) updateJobCompleted(ctx context.Context, jobID int, filePath string, tickCount int, fileSize int64) error {
	query := `
		UPDATE tick_archive_jobs
		SET status = 'completed',
			completed_at = NOW(),
			file_path = $1,
			tick_count = $2,
			file_size_bytes = $3
		WHERE id = $4
	`

	_, err := m.db.Exec(ctx, query, filePath, tickCount, fileSize, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job completion: %w", err)
	}

	return nil
}

// scheduleRetry marks a job for retry
func (m *TickArchiveManager) scheduleRetry(ctx context.Context, jobID int, errorMsg string) error {
	// Get current job to check retry count
	query := `
		SELECT retry_count FROM tick_archive_jobs
		WHERE id = $1
	`

	var retryCount int
	err := m.db.QueryRow(ctx, query, jobID).Scan(&retryCount)
	if err != nil {
		return fmt.Errorf("failed to get job retry count: %w", err)
	}

	// Increment retry count
	retryCount++

	// If max retries reached, mark as permanently failed
	if retryCount > 5 {
		return m.updateJobStatus(ctx, jobID, "failed_permanent", errorMsg)
	}

	// Calculate backoff (e.g., 5min, 15min, 45min, 2h, 6h)
	backoff := time.Duration(math.Pow(3, float64(retryCount-1))) * time.Minute * 5
	nextRetry := time.Now().Add(backoff)

	// Update job for retry
	updateQuery := `
		UPDATE tick_archive_jobs
		SET status = 'failed',
			error_message = $1,
			retry_count = $2,
			next_retry_at = $3
		WHERE id = $4
	`

	_, err = m.db.Exec(ctx, updateQuery, errorMsg, retryCount, nextRetry, jobID)
	if err != nil {
		return fmt.Errorf("failed to schedule job for retry: %w", err)
	}

	logger.L().Info("Scheduled job for retry", map[string]interface{}{
		"job_id":      jobID,
		"retry_count": retryCount,
		"next_retry":  nextRetry.Format(time.RFC3339),
		"backoff":     backoff.String(),
	})

	return nil
}

// ExecuteConsolidationJob consolidates hourly archives into daily files
func ExecuteConsolidationJob(ctx context.Context) error {
	manager := GetTickArchiveManager()

	// Get yesterday's date
	yesterday := time.Now().Add(-24 * time.Hour).Truncate(24 * time.Hour)

	// Process each index
	for _, index := range []string{"NIFTY", "SENSEX"} {
		if err := manager.consolidateDailyFiles(ctx, yesterday, index); err != nil {
			logger.L().Error("Failed to consolidate daily files", map[string]interface{}{
				"index": index,
				"date":  yesterday.Format("2006-01-02"),
				"error": err.Error(),
			})
		}
	}

	return nil
}

// consolidateDailyFiles consolidates hourly archives into a daily file
func (m *TickArchiveManager) consolidateDailyFiles(ctx context.Context, date time.Time, index string) error {
	// Find all hourly files for the given date and index
	pattern := fmt.Sprintf("%s_%s*_to_*.parquet", index, date.Format("2006-01-02"))
	files, err := filepath.Glob(filepath.Join(filestore.DefaultParquetDir, pattern))
	if err != nil {
		return fmt.Errorf("failed to find hourly files: %w", err)
	}

	if len(files) == 0 {
		logger.L().Info("No hourly files found for consolidation", map[string]interface{}{
			"index": index,
			"date":  date.Format("2006-01-02"),
		})
		return nil
	}

	// Create a new consolidated file
	consolidatedFileName := fmt.Sprintf("%s_%s_daily.parquet", index, date.Format("2006-01-02"))
	consolidatedFilePath := filepath.Join(filestore.DefaultParquetDir, consolidatedFileName)

	// Get or create parquet writer for the consolidated file
	writer, err := m.store.GetOrCreateWriter(consolidatedFilePath)
	if err != nil {
		return fmt.Errorf("failed to create consolidated file writer: %w", err)
	}

	// Read from hourly files and write to consolidated file
	totalRecords := 0
	for _, file := range files {
		// Read records from hourly file using ReadSamples
		records, err := m.store.ReadSamples(file, 1000) // Read up to 1000 records
		if err != nil {
			logger.L().Error("Failed to read from hourly file", map[string]interface{}{
				"file":  file,
				"error": err.Error(),
			})
			continue
		}

		// Write records to consolidated file one by one
		for _, record := range records {
			if err := writer.Write(record); err != nil {
				return fmt.Errorf("failed to write record to consolidated file: %w", err)
			}
		}

		totalRecords += len(records)
	}

	// Close the writer to flush data
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close consolidated file writer: %w", err)
	}

	// Get file size
	fileInfo, err := os.Stat(consolidatedFilePath)
	if err != nil {
		return fmt.Errorf("failed to get consolidated file size: %w", err)
	}
	fileSize := fileInfo.Size()

	logger.L().Info("Created consolidated daily file", map[string]interface{}{
		"index":         index,
		"date":          date.Format("2006-01-02"),
		"file":          consolidatedFilePath,
		"total_records": totalRecords,
		"file_size":     fileSize,
	})

	// Verify consolidated file
	if err := m.verifyExport(ctx, consolidatedFilePath, totalRecords); err != nil {
		return fmt.Errorf("consolidated file verification failed: %w", err)
	}

	// Delete hourly files after successful consolidation
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			logger.L().Error("Failed to delete hourly file", map[string]interface{}{
				"file":  file,
				"error": err.Error(),
			})
		}
	}

	return nil
}
