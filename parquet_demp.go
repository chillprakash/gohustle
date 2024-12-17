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

	// Test export for each table
	tables := []string{
		"nifty_ticks",
		"sensex_ticks",
		"nifty_upcoming_expiry_ticks",
		"sensex_upcoming_expiry_ticks",
	}

	for _, tableName := range tables {
		log.Info("Starting export for table", map[string]interface{}{
			"table": tableName,
		})

		// Generate filename with timestamp
		filename := fmt.Sprintf("%s/%s_%s.parquet",
			exportPath,
			tableName,
			time.Now().Format("20060102_150405"),
		)

		// Query data
		query := fmt.Sprintf(`
			SELECT * FROM %s 
			WHERE timestamp::date = CURRENT_DATE
			ORDER BY timestamp ASC
			LIMIT 1000  -- Remove this limit in production
		`, tableName)

		rows, err := timescale.GetPool().Query(ctx, query)
		if err != nil {
			log.Error("Failed to query data", map[string]interface{}{
				"error": err.Error(),
				"table": tableName,
			})
			continue
		}
		defer rows.Close()

		// Create parquet writer
		writer, err := filestore.NewParquetWriter(filename)
		if err != nil {
			log.Error("Failed to create parquet writer", map[string]interface{}{
				"error": err.Error(),
				"file":  filename,
			})
			continue
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
				log.Error("Failed to scan row", map[string]interface{}{
					"error": err.Error(),
					"table": tableName,
					"row":   count,
				})
				continue
			}

			if err := writer.Write(tick); err != nil {
				log.Error("Failed to write row", map[string]interface{}{
					"error": err.Error(),
					"table": tableName,
					"row":   count,
				})
				continue
			}
			count++

			if count%1000 == 0 {
				log.Info("Export progress", map[string]interface{}{
					"table": tableName,
					"rows":  count,
				})
			}
		}

		log.Info("Export completed", map[string]interface{}{
			"table":     tableName,
			"file":      filename,
			"row_count": count,
		})
	}
}
