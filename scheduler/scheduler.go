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
			15, 0, 0, 0,
			time.FixedZone("IST", 5*60*60+30*60), // IST offset: +5:30
		)

		// If it's past 3:45 PM today, schedule for tomorrow
		if now.After(nextRun) {
			nextRun = nextRun.Add(24 * time.Hour)
		}

		// Wait until next run time
		timer := time.NewTimer(time.Until(nextRun))

		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
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
	s.log.Info("Starting daily data export", nil)

	var exportedFiles []string
	totalRows := 0

	for _, tableName := range tables {
		if err := s.exportTable(tableName); err != nil {
			s.log.Error("Failed to export table", map[string]interface{}{
				"table": tableName,
				"error": err.Error(),
			})
			continue
		}

		// Add file to list
		date := time.Now().Format("20060102")
		filename := fmt.Sprintf("%s_%s.parquet", tableName, date)
		filepath := fmt.Sprintf("%s/%s", s.exportPath, filename)
		exportedFiles = append(exportedFiles, filepath)
	}

	// Send notification
	if err := s.notifyExportComplete(exportedFiles, totalRows); err != nil {
		s.log.Error("Failed to send export notification", map[string]interface{}{
			"error": err.Error(),
		})
	}

	s.log.Info("Daily data export completed", map[string]interface{}{
		"files":      len(exportedFiles),
		"total_rows": totalRows,
	})
}

func (s *Scheduler) exportTable(tableName string) error {
	// Get today's date for the filename
	date := time.Now().Format("20060102")
	filename := fmt.Sprintf("%s_%s.parquet", tableName, date)
	filepath := fmt.Sprintf("%s/%s", s.exportPath, filename)

	// Query data
	query := fmt.Sprintf(`
		SELECT * FROM %s 
		WHERE timestamp::date = CURRENT_DATE
		ORDER BY timestamp ASC
	`, tableName)

	rows, err := s.timescale.GetPool().Query(s.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query data: %w", err)
	}
	defer rows.Close()

	// Create parquet writer
	writer, err := filestore.NewParquetWriter(filepath)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer writer.Close()

	// Write data
	count := 0
	for rows.Next() {
		var tick filestore.TickRecord
		if err := rows.Scan(
			&tick.ID,
			&tick.InstrumentToken,
			&tick.Timestamp,
			&tick.IsTradable,
			&tick.IsIndex,
			&tick.Mode,
			&tick.LastPrice,
			&tick.LastTradedQuantity,
			&tick.AverageTradePrice,
			&tick.VolumeTraded,
			&tick.TotalBuyQuantity,
			&tick.TotalSellQuantity,
			&tick.TotalBuy,
			&tick.TotalSell,
			&tick.OhlcOpen,
			&tick.OhlcHigh,
			&tick.OhlcLow,
			&tick.OhlcClose,
			&tick.DepthBuyPrice1,
			&tick.DepthBuyQuantity1,
			&tick.DepthBuyOrders1,
			&tick.DepthBuyPrice2,
			&tick.DepthBuyQuantity2,
			&tick.DepthBuyOrders2,
			&tick.DepthBuyPrice3,
			&tick.DepthBuyQuantity3,
			&tick.DepthBuyOrders3,
			&tick.DepthBuyPrice4,
			&tick.DepthBuyQuantity4,
			&tick.DepthBuyOrders4,
			&tick.DepthBuyPrice5,
			&tick.DepthBuyQuantity5,
			&tick.DepthBuyOrders5,
			&tick.DepthSellPrice1,
			&tick.DepthSellQuantity1,
			&tick.DepthSellOrders1,
			&tick.DepthSellPrice2,
			&tick.DepthSellQuantity2,
			&tick.DepthSellOrders2,
			&tick.DepthSellPrice3,
			&tick.DepthSellQuantity3,
			&tick.DepthSellOrders3,
			&tick.DepthSellPrice4,
			&tick.DepthSellQuantity4,
			&tick.DepthSellOrders4,
			&tick.DepthSellPrice5,
			&tick.DepthSellQuantity5,
			&tick.DepthSellOrders5,
			&tick.LastTradeTime,
			&tick.NetChange,
			&tick.TickReceivedTime,
			&tick.TickStoredInDbTime,
		); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		if err := writer.Write(tick); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
		count++
	}

	s.log.Info("Table export completed", map[string]interface{}{
		"table":     tableName,
		"file":      filepath,
		"row_count": count,
	})

	return nil
}

func (s *Scheduler) Stop() {
	s.wg.Wait()
}
