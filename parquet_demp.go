package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"gohustle/db"
	"gohustle/filestore"
	"gohustle/logger"
)

func main() {
	log := logger.GetLogger()
	ctx := context.Background()

	// Initialize TimescaleDB
	timescale := db.GetTimescaleDB()
	defer timescale.Close()

	// Create export directory
	exportPath := "data/exports"
	if err := os.MkdirAll(exportPath, 0755); err != nil {
		log.Error("Failed to create export directory", map[string]interface{}{
			"error": err.Error(),
			"path":  exportPath,
		})
		os.Exit(1)
	}

	// Create exporter
	exporter := filestore.NewExporter(timescale.GetPool(), exportPath)

	// Test export for each table
	tables := []string{
		"nifty_ticks",
		"sensex_ticks",
		"nifty_upcoming_expiry_ticks",
		"sensex_upcoming_expiry_ticks",
	}

	startTime := time.Now()
	log.Info("Starting export process", map[string]interface{}{
		"tables": tables,
		"time":   startTime,
	})

	for _, tableName := range tables {
		log.Info("Starting export for table", map[string]interface{}{
			"table": tableName,
		})

		exportedFile, err := exporter.ExportTable(ctx, tableName)
		if err != nil {
			log.Error("Failed to export table", map[string]interface{}{
				"error": err.Error(),
				"table": tableName,
			})
			continue
		}

		// Get row count for verification
		var count int64
		countQuery := fmt.Sprintf(`
			SELECT COUNT(*) 
			FROM %s 
			WHERE timestamp::date = CURRENT_DATE
		`, tableName)

		if err := timescale.GetPool().QueryRow(ctx, countQuery).Scan(&count); err != nil {
			log.Error("Failed to get row count", map[string]interface{}{
				"error": err.Error(),
				"table": tableName,
			})
			continue
		}

		log.Info("Table export successful", map[string]interface{}{
			"table":     tableName,
			"file":      exportedFile,
			"row_count": count,
			"duration":  time.Since(startTime),
		})
	}

	log.Info("Export process completed", map[string]interface{}{
		"total_duration": time.Since(startTime),
		"tables":         len(tables),
	})
}
