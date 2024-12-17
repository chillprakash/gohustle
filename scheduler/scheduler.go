package scheduler

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"gohustle/config"
	"gohustle/db"
	"gohustle/filestore"
	"gohustle/logger"
	"gohustle/notify"
)

type Scheduler struct {
	ctx        context.Context
	timescale  *db.TimescaleDB
	log        *logger.Logger
	wg         *sync.WaitGroup
	exportPath string
	notifier   *notify.TelegramNotifier
}

// Tables to export
var tables = []string{
	"nifty_ticks",
	"sensex_ticks",
	"nifty_upcoming_expiry_ticks",
	"sensex_upcoming_expiry_ticks",
}

const (
	maxTelegramFileSize = 50 * 1024 * 1024 // 50MB - Telegram's limit
	maxRetries          = 3
	retryDelay          = 5 * time.Second
)

func NewScheduler(ctx context.Context, exportPath string, telegramCfg *config.TelegramConfig) *Scheduler {
	return &Scheduler{
		ctx:        ctx,
		timescale:  db.GetTimescaleDB(),
		log:        logger.GetLogger(),
		wg:         &sync.WaitGroup{},
		exportPath: exportPath,
		notifier:   notify.NewTelegramNotifier(telegramCfg),
	}
}

func (s *Scheduler) Start() {
	s.wg.Add(1)
	go s.runDailyExport()
}

func (s *Scheduler) runDailyExport() {
	defer s.wg.Done()

	for {
		now := time.Now()
		// Calculate next run time (3:45 PM IST)
		nextRun := time.Date(
			now.Year(), now.Month(), now.Day(),
			17, 50, 0, 0,
			time.FixedZone("IST", 5*60*60+30*60), // IST offset: +5:30
		)

		s.log.Info("Scheduled next export", map[string]interface{}{
			"current_time": now.Format(time.RFC3339),
			"next_run":     nextRun.Format(time.RFC3339),
		})

		// If it's past 3:45 PM today, schedule for tomorrow
		if now.After(nextRun) {
			nextRun = nextRun.Add(24 * time.Hour)
			s.log.Info("Rescheduled to tomorrow", map[string]interface{}{
				"next_run": nextRun.Format(time.RFC3339),
			})
		}

		// Wait until next run time
		timer := time.NewTimer(time.Until(nextRun))

		select {
		case <-s.ctx.Done():
			timer.Stop()
			s.log.Info("Scheduler shutdown requested", nil)
			return
		case <-timer.C:
			s.log.Info("Starting scheduled export", map[string]interface{}{
				"time": time.Now().Format(time.RFC3339),
			})

			// Export data first
			s.exportData()

			// Now collect exported files and send notification
			date := time.Now().Format("20060102")
			var files []string
			var totalRows int

			// Verify exports and collect file information
			for _, tableName := range tables {
				filename := fmt.Sprintf("%s_%s.pb.gz", tableName, date)
				fullPath := filepath.Join(s.exportPath, filename)

				// Verify file exists and is not empty
				if fileInfo, err := os.Stat(fullPath); err == nil && fileInfo.Size() > 0 {
					files = append(files, fullPath)

					// Get row count
					var count int
					countQuery := fmt.Sprintf(`
							SELECT COUNT(*) FROM %s 
							WHERE timestamp::date = CURRENT_DATE
						`, tableName)

					if err := s.timescale.GetPool().QueryRow(s.ctx, countQuery).Scan(&count); err == nil {
						totalRows += count
					}
				} else {
					s.log.Error("Export verification failed", map[string]interface{}{
						"table": tableName,
						"file":  fullPath,
						"error": err,
					})
				}
			}

			// Send notification only if we have successful exports
			if len(files) > 0 {
				if err := s.notifyExportComplete(files, totalRows); err != nil {
					s.log.Error("Failed to send export notification", map[string]interface{}{
						"error": err.Error(),
						"files": len(files),
					})
				} else {
					s.log.Info("Export notification sent", map[string]interface{}{
						"files":      len(files),
						"total_rows": totalRows,
					})
				}
			} else {
				s.log.Error("No successful exports to notify", nil)
			}
		}
	}
}

func (s *Scheduler) notifyExportComplete(files []string, totalRows int) error {
	// First send the summary message
	message := fmt.Sprintf(`🔄 Daily Data Export Complete

📊 Summary:
- Total Files: %d
- Total Rows: %d
- Time: %s

📁 Files:
`, len(files), totalRows, time.Now().Format("15:04:05"))

	// Add file details with size check
	var largeFiles []string
	var regularFiles []string

	for _, file := range files {
		fileInfo, err := os.Stat(file)
		if err != nil {
			s.log.Error("Failed to get file info", map[string]interface{}{
				"error": err.Error(),
				"file":  file,
			})
			continue
		}

		sizeMB := float64(fileInfo.Size()) / 1024 / 1024
		message += fmt.Sprintf("- %s (%.2f MB)", filepath.Base(file), sizeMB)

		// Mark files that exceed Telegram's limit
		if fileInfo.Size() > maxTelegramFileSize {
			message += " ⚠️ Large file - will be split\n"
			largeFiles = append(largeFiles, file)
		} else {
			message += "\n"
			regularFiles = append(regularFiles, file)
		}
	}

	// Send initial message
	if err := s.notifier.SendMessage(message); err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}

	// Send regular files with retries
	for _, file := range regularFiles {
		if err := s.sendFileWithRetry(file); err != nil {
			s.log.Error("Failed to send file after retries", map[string]interface{}{
				"error": err.Error(),
				"file":  filepath.Base(file),
			})
		}
	}

	// Handle large files
	for _, file := range largeFiles {
		if err := s.handleLargeFile(file); err != nil {
			s.log.Error("Failed to handle large file", map[string]interface{}{
				"error": err.Error(),
				"file":  filepath.Base(file),
			})
		}
	}

	return nil
}

func (s *Scheduler) sendFileWithRetry(file string) error {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		fileStartTime := time.Now()
		s.log.Info("Sending file to Telegram", map[string]interface{}{
			"file":    filepath.Base(file),
			"attempt": attempt,
			"max":     maxRetries,
		})

		if err := s.notifier.SendFile(file); err != nil {
			lastErr = err
			s.log.Error("File send attempt failed", map[string]interface{}{
				"error":   err.Error(),
				"file":    filepath.Base(file),
				"attempt": attempt,
			})

			if attempt < maxRetries {
				time.Sleep(retryDelay * time.Duration(attempt)) // Exponential backoff
				continue
			}
			return fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
		}

		s.log.Info("File sent successfully", map[string]interface{}{
			"file":     filepath.Base(file),
			"attempt":  attempt,
			"duration": time.Since(fileStartTime).String(),
		})
		return nil
	}
	return lastErr
}

func (s *Scheduler) handleLargeFile(file string) error {
	fileInfo, err := os.Stat(file)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Send notification about splitting
	splitMsg := fmt.Sprintf("📦 Splitting large file: %s (%.2f MB)",
		filepath.Base(file),
		float64(fileInfo.Size())/1024/1024,
	)
	if err := s.notifier.SendMessage(splitMsg); err != nil {
		s.log.Error("Failed to send split notification", map[string]interface{}{
			"error": err.Error(),
			"file":  filepath.Base(file),
		})
	}

	// Create temporary directory for splits
	splitDir := filepath.Join(os.TempDir(), "gohustle_splits")
	if err := os.MkdirAll(splitDir, 0755); err != nil {
		return fmt.Errorf("failed to create split directory: %w", err)
	}
	defer os.RemoveAll(splitDir) // Clean up after sending

	// Split the file
	splitFiles, err := s.splitFile(file, splitDir)
	if err != nil {
		return fmt.Errorf("failed to split file: %w", err)
	}

	// Send each part
	for i, splitFile := range splitFiles {
		partMsg := fmt.Sprintf("📤 Sending part %d/%d of %s",
			i+1,
			len(splitFiles),
			filepath.Base(file),
		)
		if err := s.notifier.SendMessage(partMsg); err != nil {
			s.log.Error("Failed to send part notification", map[string]interface{}{
				"error": err.Error(),
				"file":  filepath.Base(splitFile),
				"part":  i + 1,
			})
		}

		if err := s.sendFileWithRetry(splitFile); err != nil {
			s.log.Error("Failed to send file part", map[string]interface{}{
				"error": err.Error(),
				"file":  filepath.Base(splitFile),
				"part":  i + 1,
			})
			continue
		}
	}

	return nil
}

func (s *Scheduler) splitFile(file string, splitDir string) ([]string, error) {
	// Use split command for reliable file splitting
	baseName := filepath.Join(splitDir, filepath.Base(file))
	cmd := exec.Command("split",
		"-b", fmt.Sprintf("%dM", maxTelegramFileSize/(1024*1024)-1), // Leave 1MB buffer
		"-d", // Use numeric suffixes
		file,
		baseName+"_part_",
	)

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("split command failed: %w", err)
	}

	// Get list of split files
	pattern := baseName + "_part_*"
	splitFiles, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list split files: %w", err)
	}

	return splitFiles, nil
}

func (s *Scheduler) exportData() {
	startTime := time.Now()
	s.log.Info("Starting daily data export", map[string]interface{}{
		"start_time": startTime.Format(time.RFC3339),
	})

	var exportedFiles []string
	totalRows := 0
	var exportErrors []string

	// Export each table
	for _, tableName := range tables {
		tableStartTime := time.Now()
		s.log.Info("Starting table export", map[string]interface{}{
			"table": tableName,
			"time":  tableStartTime.Format(time.RFC3339),
		})

		count, err := s.exportTable(tableName)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to export table %s: %v", tableName, err)
			exportErrors = append(exportErrors, errMsg)
			s.log.Error("Failed to export table", map[string]interface{}{
				"table":    tableName,
				"error":    err.Error(),
				"duration": time.Since(tableStartTime).String(),
			})
			continue
		}

		totalRows += count

		// Add file to list
		date := time.Now().Format("20060102")
		filename := fmt.Sprintf("%s_%s.pb.gz", tableName, date)
		fullPath := filepath.Join(s.exportPath, filename)

		// Verify file exists and is not empty
		fileInfo, err := os.Stat(fullPath)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to verify file %s: %v", filename, err)
			exportErrors = append(exportErrors, errMsg)
			s.log.Error("File verification failed", map[string]interface{}{
				"file":  filename,
				"error": err.Error(),
			})
			continue
		}

		if fileInfo.Size() == 0 {
			errMsg := fmt.Sprintf("File %s is empty", filename)
			exportErrors = append(exportErrors, errMsg)
			s.log.Error("Empty file generated", map[string]interface{}{
				"file": filename,
			})
			continue
		}

		exportedFiles = append(exportedFiles, fullPath)

		s.log.Info("Table export completed", map[string]interface{}{
			"table":     tableName,
			"rows":      count,
			"file":      fullPath,
			"file_size": fmt.Sprintf("%.2f MB", float64(fileInfo.Size())/1024/1024),
			"duration":  time.Since(tableStartTime).String(),
		})
	}

	// Prepare notification message
	message := fmt.Sprintf(`🔄 Daily Data Export Complete

📊 Summary:
- Total Files: %d
- Total Rows: %d
- Duration: %s
- Start Time: %s
- End Time: %s

📁 Files:
`, len(exportedFiles), totalRows, time.Since(startTime),
		startTime.Format("15:04:05"),
		time.Now().Format("15:04:05"))

	// Add file details
	for _, file := range exportedFiles {
		fileInfo, err := os.Stat(file)
		if err != nil {
			s.log.Error("Failed to get file info", map[string]interface{}{
				"error": err.Error(),
				"file":  file,
			})
			continue
		}
		sizeMB := float64(fileInfo.Size()) / 1024 / 1024
		message += fmt.Sprintf("- %s (%.2f MB)\n", filepath.Base(file), sizeMB)
	}

	// Add errors if any
	if len(exportErrors) > 0 {
		message += "\n⚠️ Errors:\n"
		for _, err := range exportErrors {
			message += fmt.Sprintf("- %s\n", err)
		}
	}

	// Send notification
	if err := s.notifier.SendMessage(message); err != nil {
		s.log.Error("Failed to send export notification", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Send files
	for _, file := range exportedFiles {
		fileStartTime := time.Now()
		s.log.Info("Sending file to Telegram", map[string]interface{}{
			"file": filepath.Base(file),
		})

		if err := s.notifier.SendFile(file); err != nil {
			s.log.Error("Failed to send file", map[string]interface{}{
				"error": err.Error(),
				"file":  file,
			})
			continue
		}

		s.log.Info("File sent successfully", map[string]interface{}{
			"file":     filepath.Base(file),
			"duration": time.Since(fileStartTime).String(),
		})
	}

	s.log.Info("Daily data export completed", map[string]interface{}{
		"files":       len(exportedFiles),
		"total_rows":  totalRows,
		"duration":    time.Since(startTime).String(),
		"error_count": len(exportErrors),
	})
}

func (s *Scheduler) exportTable(tableName string) (int, error) {
	exporter := filestore.NewExporter(s.timescale.GetPool(), s.exportPath)

	// Use ExportTableToProto instead of ExportTable
	exportedFile, err := exporter.ExportTableToProto(s.ctx, tableName)
	if err != nil {
		return 0, fmt.Errorf("failed to export table %s: %w", tableName, err)
	}

	// Get row count for reporting
	var count int
	countQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s 
		WHERE timestamp::date = CURRENT_DATE
	`, tableName)

	err = s.timescale.GetPool().QueryRow(s.ctx, countQuery).Scan(&count)
	if err != nil {
		s.log.Error("Failed to get row count", map[string]interface{}{
			"table": tableName,
			"error": err.Error(),
		})
		// Don't return error as export was successful
	}

	s.log.Info("Table export completed", map[string]interface{}{
		"table": tableName,
		"file":  exportedFile,
		"rows":  count,
	})

	return count, nil
}

func (s *Scheduler) Stop() {
	s.wg.Wait()
}
