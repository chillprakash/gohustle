package scheduler

import (
	"context"
	"fmt"
	"os"
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
			15, 52, 0, 0,
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
			s.exportData()
		}
	}
}

func (s *Scheduler) notifyExportComplete(files []string, totalRows int) error {
	message := fmt.Sprintf(`🔄 Daily Data Export Complete

📊 Summary:
- Total Files: %d
- Total Rows: %d

📁 Files:
`, len(files), totalRows)

	// Add file details
	for _, file := range files {
		fileInfo, err := os.Stat(file)
		if err != nil {
			s.log.Error("Failed to get file info", map[string]interface{}{
				"error": err.Error(),
				"file":  file,
			})
			continue
		}

		// Add file size in MB
		sizeMB := float64(fileInfo.Size()) / 1024 / 1024
		message += fmt.Sprintf("- %s (%.2f MB)\n", filepath.Base(file), sizeMB)
	}

	// Send message
	if err := s.notifier.SendMessage(message); err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}

	// Send files
	for _, file := range files {
		if err := s.notifier.SendFile(file); err != nil {
			s.log.Error("Failed to send file", map[string]interface{}{
				"error": err.Error(),
				"file":  file,
			})
			continue
		}
	}

	return nil
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
		filename := fmt.Sprintf("%s_%s.parquet", tableName, date)
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

	exportedFile, err := exporter.ExportTable(s.ctx, tableName)
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
