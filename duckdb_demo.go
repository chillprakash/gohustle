package main

import (
	"fmt"
	"gohustle/db"
	"gohustle/logger"
	"gohustle/proto"
	"time"
)

// DemoDuckDB runs a demonstration of DuckDB functionality
func DemoDuckDB() {
	log := logger.GetLogger()
	log.Info("Starting DuckDB demo", nil)

	// Get DuckDB instance
	duckDB := db.GetDuckDB()
	defer duckDB.Close()

	// Create sample tick data
	sampleTicks := createSampleTicks()

	// Write sample ticks
	for i, tick := range sampleTicks {
		if err := duckDB.WriteTick(tick); err != nil {
			log.Error("Failed to write tick", map[string]interface{}{
				"error": err.Error(),
				"index": i,
			})
			continue
		}
		log.Info("Successfully wrote tick", map[string]interface{}{
			"index":            i,
			"instrument_token": tick.InstrumentToken,
		})
	}

	// Run some demo queries
	demoQueries(duckDB)

	log.Info("DuckDB demo completed", nil)
}

func createSampleTicks() []*proto.TickData {
	now := time.Now()
	return []*proto.TickData{
		{
			InstrumentToken:    256265, // NIFTY
			Timestamp:          now.Unix(),
			IsTradable:         true,
			IsIndex:            true,
			Mode:               "full",
			LastPrice:          21500.50,
			LastTradedQuantity: 100,
			AverageTradePrice:  21495.25,
			VolumeTraded:       10000,
			TotalBuyQuantity:   50000,
			TotalSellQuantity:  45000,
			Ohlc: &proto.TickData_OHLC{
				Open:  21490.0,
				High:  21510.0,
				Low:   21485.5,
				Close: 21500.50,
			},
			Depth: &proto.TickData_MarketDepth{
				Buy: []*proto.TickData_DepthItem{
					{Price: 21500.45, Quantity: 100, Orders: 2},
					{Price: 21500.40, Quantity: 150, Orders: 3},
					{Price: 21500.35, Quantity: 200, Orders: 4},
					{Price: 21500.30, Quantity: 250, Orders: 2},
					{Price: 21500.25, Quantity: 300, Orders: 1},
				},
				Sell: []*proto.TickData_DepthItem{
					{Price: 21500.55, Quantity: 100, Orders: 1},
					{Price: 21500.60, Quantity: 150, Orders: 2},
					{Price: 21500.65, Quantity: 200, Orders: 3},
					{Price: 21500.70, Quantity: 250, Orders: 2},
					{Price: 21500.75, Quantity: 300, Orders: 1},
				},
			},
			ChangePercent: 0.5,
			LastTradeTime: now.Unix(),
			Oi:            10000,
			OiDayHigh:     12000,
			OiDayLow:      8000,
			NetChange:     0.5,
			TargetFile:    fmt.Sprintf("NIFTY_%s.parquet", now.Format("20060102")),
		},
		{
			InstrumentToken:    260105, // BANKNIFTY
			Timestamp:          now.Unix(),
			IsTradable:         true,
			IsIndex:            true,
			Mode:               "full",
			LastPrice:          47500.50,
			LastTradedQuantity: 50,
			AverageTradePrice:  47495.25,
			VolumeTraded:       5000,
			TotalBuyQuantity:   25000,
			TotalSellQuantity:  22500,
			Ohlc: &proto.TickData_OHLC{
				Open:  47490.0,
				High:  47510.0,
				Low:   47485.5,
				Close: 47500.50,
			},
			Depth: &proto.TickData_MarketDepth{
				Buy: []*proto.TickData_DepthItem{
					{Price: 47500.45, Quantity: 50, Orders: 3},
					{Price: 47500.40, Quantity: 75, Orders: 4},
					{Price: 47500.35, Quantity: 100, Orders: 5},
					{Price: 47500.30, Quantity: 125, Orders: 3},
					{Price: 47500.25, Quantity: 150, Orders: 2},
				},
				Sell: []*proto.TickData_DepthItem{
					{Price: 47500.55, Quantity: 50, Orders: 2},
					{Price: 47500.60, Quantity: 75, Orders: 3},
					{Price: 47500.65, Quantity: 100, Orders: 4},
					{Price: 47500.70, Quantity: 125, Orders: 3},
					{Price: 47500.75, Quantity: 150, Orders: 2},
				},
			},
			ChangePercent: 0.75,
			LastTradeTime: now.Unix(),
			Oi:            5000,
			OiDayHigh:     6000,
			OiDayLow:      4000,
			NetChange:     0.75,
			TargetFile:    fmt.Sprintf("BANKNIFTY_%s.parquet", now.Format("20060102")),
		},
	}
}

func demoQueries(duckDB *db.DuckDB) {
	log := logger.GetLogger()

	// Get stats for NIFTY
	stats, err := duckDB.GetTickStats(256265)
	if err != nil {
		log.Error("Failed to get NIFTY stats", map[string]interface{}{
			"error": err.Error(),
		})
	} else {
		log.Info("NIFTY stats", stats)
	}

	// Get stats for BANKNIFTY
	stats, err = duckDB.GetTickStats(260105)
	if err != nil {
		log.Error("Failed to get BANKNIFTY stats", map[string]interface{}{
			"error": err.Error(),
		})
	} else {
		log.Info("BANKNIFTY stats", stats)
	}

	// Export to parquet
	if err := duckDB.ExportToParquet(); err != nil {
		log.Error("Failed to export to parquet", map[string]interface{}{
			"error": err.Error(),
		})
	} else {
		log.Info("Successfully exported to parquet", nil)
	}
}

func main() {
	DemoDuckDB()
}
