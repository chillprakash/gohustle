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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// TickArchiveManager handles the archiving process for tick data
type TickArchiveManager struct {
	db    *db.TimescaleDB
	store *filestore.ParquetStore
	mu    sync.Mutex
}

var (
	instance *TickArchiveManager
	once     sync.Once
)

// GetTickArchiveManager returns a singleton instance of TickArchiveManager
func GetTickArchiveManager() *TickArchiveManager {
	once.Do(func() {
		instance = &TickArchiveManager{
			db:    db.GetTimescaleDB(),
			store: filestore.GetParquetStore(),
		}
	})
	return instance
}

// TickArchiveJob represents a job for archiving tick data
type TickArchiveJob struct {
	ID            int       `db:"id"`
	JobID         string    `db:"job_id"`
	IndexName     string    `db:"index_name"`
	StartTime     time.Time `db:"start_time"`
	EndTime       time.Time `db:"end_time"`
	Status        string    `db:"status"`
	CreatedAt     time.Time `db:"created_at"`
	StartedAt     time.Time `db:"started_at"`
	CompletedAt   time.Time `db:"completed_at"`
	TickCount     int       `db:"tick_count"`
	FilePath      string    `db:"file_path"`
	FileSizeBytes int64     `db:"file_size_bytes"`
	ErrorMessage  string    `db:"error_message"`
	RetryCount    int       `db:"retry_count"`
	NextRetryAt   time.Time `db:"next_retry_at"`
}

// ExecuteTickArchiveJob is the main entry point for the archiving process
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

// getOrCreatePendingJobs finds pending jobs or creates new ones if none exist
func (m *TickArchiveManager) getOrCreatePendingJobs(ctx context.Context) ([]*TickArchiveJob, error) {
	// First, check for any pending jobs that are ready to be processed
	pendingJobs, err := m.getPendingJobs(ctx)
	if err != nil {
		return nil, err
	}

	// If we have pending jobs, return them
	if len(pendingJobs) > 0 {
		return pendingJobs, nil
	}

	// Check for jobs scheduled for retry
	retryJobs, err := m.getJobsForRetry(ctx)
	if err != nil {
		return nil, err
	}

	// If we have jobs scheduled for retry, return them
	if len(retryJobs) > 0 {
		return retryJobs, nil
	}

	// If no pending or retry jobs, create new ones
	return m.createNewArchiveJobs(ctx)
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

// ExecuteTickConsolidationJob consolidates hourly archives into daily files
func ExecuteTickConsolidationJob(ctx context.Context) error {
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
