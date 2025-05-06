package db

import (
	"context"
	"fmt"
	"time"
)

// PositionPnLTimeseriesRecord represents a single point in the position P&L time series
type PositionPnLTimeseriesRecord struct {
	ID            int64     `json:"id"`
	PositionID    string    `json:"position_id"`
	TradingSymbol string    `json:"trading_symbol"`
	StrategyID    int       `json:"strategy_id"`
	Quantity      int       `json:"quantity"`
	AveragePrice  float64   `json:"average_price"`
	LastPrice     float64   `json:"last_price"`
	RealizedPnL   float64   `json:"realized_pnl"`
	UnrealizedPnL float64   `json:"unrealized_pnl"`
	TotalPnL      float64   `json:"total_pnl"`
	PaperTrading  bool      `json:"paper_trading"`
	Timestamp     time.Time `json:"timestamp"`
}

// BatchInsertPositionPnLTimeseries inserts multiple position P&L timeseries records efficiently
func (t *TimescaleDB) BatchInsertPositionPnLTimeseries(ctx context.Context, records []PositionPnLTimeseriesRecord) error {
	if len(records) == 0 {
		return nil
	}

	// Begin transaction
	tx, err := t.pool.Begin(ctx)
	if err != nil {
		t.log.Error("Failed to begin transaction for batch insert", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		}
	}()

	// Prepare the batch insert statement
	_, err = tx.Prepare(ctx, "batch_insert_pnl", `
		INSERT INTO position_pnl_timeseries 
		(position_id, trading_symbol, strategy_id, quantity, average_price, last_price, 
		realized_pnl, unrealized_pnl, total_pnl, paper_trading, timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`)
	if err != nil {
		t.log.Error("Failed to prepare batch insert statement", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to prepare batch insert statement: %w", err)
	}

	// Insert each record
	for _, record := range records {
		_, err = tx.Exec(ctx, "batch_insert_pnl",
			record.PositionID,
			record.TradingSymbol,
			record.StrategyID,
			record.Quantity,
			record.AveragePrice,
			record.LastPrice,
			record.RealizedPnL,
			record.UnrealizedPnL,
			record.TotalPnL,
			record.PaperTrading,
			record.Timestamp,
		)
		if err != nil {
			t.log.Error("Failed to insert position P&L record", map[string]interface{}{
				"error":          err.Error(),
				"position_id":    record.PositionID,
				"trading_symbol": record.TradingSymbol,
			})
			return fmt.Errorf("failed to insert position P&L record: %w", err)
		}
	}

	// Commit transaction
	if err = tx.Commit(ctx); err != nil {
		t.log.Error("Failed to commit transaction for batch insert", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	t.log.Info("Successfully inserted position P&L timeseries records", map[string]interface{}{
		"count": len(records),
	})
	return nil
}

// GetPositionPnLTimeseries retrieves position P&L time series data for a specific position
func (t *TimescaleDB) GetPositionPnLTimeseries(ctx context.Context, positionID string, startTime, endTime time.Time) ([]PositionPnLTimeseriesRecord, error) {
	query := `
		SELECT id, position_id, trading_symbol, strategy_id, quantity, average_price, last_price, 
		       realized_pnl, unrealized_pnl, total_pnl, paper_trading, timestamp
		FROM position_pnl_timeseries
		WHERE position_id = $1 AND timestamp BETWEEN $2 AND $3
		ORDER BY timestamp ASC
	`

	rows, err := t.pool.Query(ctx, query, positionID, startTime, endTime)
	if err != nil {
		t.log.Error("Failed to query position P&L timeseries", map[string]interface{}{
			"error":       err.Error(),
			"position_id": positionID,
		})
		return nil, fmt.Errorf("failed to query position P&L timeseries: %w", err)
	}
	defer rows.Close()

	var records []PositionPnLTimeseriesRecord
	for rows.Next() {
		var record PositionPnLTimeseriesRecord
		if err := rows.Scan(
			&record.ID,
			&record.PositionID,
			&record.TradingSymbol,
			&record.StrategyID,
			&record.Quantity,
			&record.AveragePrice,
			&record.LastPrice,
			&record.RealizedPnL,
			&record.UnrealizedPnL,
			&record.TotalPnL,
			&record.PaperTrading,
			&record.Timestamp,
		); err != nil {
			t.log.Error("Failed to scan position P&L record", map[string]interface{}{
				"error":       err.Error(),
				"position_id": positionID,
			})
			return nil, fmt.Errorf("failed to scan position P&L record: %w", err)
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		t.log.Error("Error iterating over position P&L records", map[string]interface{}{
			"error":       err.Error(),
			"position_id": positionID,
		})
		return nil, fmt.Errorf("error iterating over position P&L records: %w", err)
	}

	return records, nil
}

// GetAggregatedPositionPnL retrieves aggregated position P&L data (from the continuous aggregate)
func (t *TimescaleDB) GetAggregatedPositionPnL(ctx context.Context, positionID string, startTime, endTime time.Time, interval string) ([]map[string]interface{}, error) {
	// Determine which continuous aggregate view to use based on interval
	var tableName string
	switch interval {
	case "1m":
		tableName = "position_pnl_minute"
	default:
		// For other intervals, use the raw data
		tableName = "position_pnl_timeseries"
	}

	query := fmt.Sprintf(`
		SELECT 
			bucket as time,
			position_id,
			trading_symbol,
			strategy_id,
			paper_trading,
			avg_quantity,
			avg_last_price,
			avg_realized_pnl,
			avg_unrealized_pnl,
			avg_total_pnl,
			min_total_pnl,
			max_total_pnl
		FROM %s
		WHERE position_id = $1 AND bucket BETWEEN $2 AND $3
		ORDER BY bucket ASC
	`, tableName)

	rows, err := t.pool.Query(ctx, query, positionID, startTime, endTime)
	if err != nil {
		t.log.Error("Failed to query aggregated position P&L", map[string]interface{}{
			"error":       err.Error(),
			"position_id": positionID,
			"interval":    interval,
		})
		return nil, fmt.Errorf("failed to query aggregated position P&L: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var (
			timestamp        time.Time
			posID            string
			tradingSymbol    string
			strategyID       int
			paperTrading     bool
			avgQuantity      float64
			avgLastPrice     float64
			avgRealizedPnL   float64
			avgUnrealizedPnL float64
			avgTotalPnL      float64
			minTotalPnL      float64
			maxTotalPnL      float64
		)

		if err := rows.Scan(
			&timestamp,
			&posID,
			&tradingSymbol,
			&strategyID,
			&paperTrading,
			&avgQuantity,
			&avgLastPrice,
			&avgRealizedPnL,
			&avgUnrealizedPnL,
			&avgTotalPnL,
			&minTotalPnL,
			&maxTotalPnL,
		); err != nil {
			t.log.Error("Failed to scan aggregated position P&L record", map[string]interface{}{
				"error":       err.Error(),
				"position_id": positionID,
			})
			return nil, fmt.Errorf("failed to scan aggregated position P&L record: %w", err)
		}

		result := map[string]interface{}{
			"time":               timestamp,
			"position_id":        posID,
			"trading_symbol":     tradingSymbol,
			"strategy_id":        strategyID,
			"paper_trading":      paperTrading,
			"avg_quantity":       avgQuantity,
			"avg_last_price":     avgLastPrice,
			"avg_realized_pnl":   avgRealizedPnL,
			"avg_unrealized_pnl": avgUnrealizedPnL,
			"avg_total_pnl":      avgTotalPnL,
			"min_total_pnl":      minTotalPnL,
			"max_total_pnl":      maxTotalPnL,
		}
		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		t.log.Error("Error iterating over aggregated position P&L records", map[string]interface{}{
			"error":       err.Error(),
			"position_id": positionID,
		})
		return nil, fmt.Errorf("error iterating over aggregated position P&L records: %w", err)
	}

	return results, nil
}
