package db

import (
	"context"
	"fmt"
	"time"
)

// StrategyPnLTimeseriesRecord represents a P&L record for a strategy at a point in time
type StrategyPnLTimeseriesRecord struct {
	StrategyID    int       `json:"strategy_id"`
	StrategyName  string    `json:"strategy_name"`
	TotalPnL      float64   `json:"total_pnl"`
	PaperTrading  bool      `json:"paper_trading"`
	Timestamp     time.Time `json:"timestamp"`
}

// BatchInsertStrategyPnLTimeseries inserts multiple strategy P&L timeseries records efficiently
func (t *TimescaleDB) BatchInsertStrategyPnLTimeseries(ctx context.Context, records []StrategyPnLTimeseriesRecord) error {
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
	_, err = tx.Prepare(ctx, "batch_insert_strategy_pnl", `
		INSERT INTO strategy_pnl_timeseries 
		(strategy_id, strategy_name, total_pnl, paper_trading, timestamp)
		VALUES ($1, $2, $3, $4, $5)
	`)
	if err != nil {
		t.log.Error("Failed to prepare batch insert statement", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to prepare batch insert statement: %w", err)
	}

	// Insert each record
	for _, record := range records {
		_, err = tx.Exec(ctx, "batch_insert_strategy_pnl", 
			record.StrategyID, 
			record.StrategyName,
			record.TotalPnL,
			record.PaperTrading,
			record.Timestamp,
		)
		if err != nil {
			t.log.Error("Failed to insert strategy P&L record", map[string]interface{}{
				"error": err.Error(),
				"strategy_id": record.StrategyID,
				"timestamp": record.Timestamp,
			})
			return fmt.Errorf("failed to insert strategy P&L record: %w", err)
		}
	}

	// Commit transaction
	if err = tx.Commit(ctx); err != nil {
		t.log.Error("Failed to commit transaction", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetStrategyPnL retrieves strategy P&L data for a specific time range
func (t *TimescaleDB) GetStrategyPnL(ctx context.Context, strategyID int, paperTrading bool, startTime, endTime time.Time) ([]StrategyPnLTimeseriesRecord, error) {
	query := `
		SELECT strategy_id, strategy_name, total_pnl, paper_trading, timestamp
		FROM strategy_pnl_timeseries
		WHERE strategy_id = $1 AND paper_trading = $2 AND timestamp BETWEEN $3 AND $4
		ORDER BY timestamp
	`
	
	rows, err := t.pool.Query(ctx, query, strategyID, paperTrading, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query strategy P&L: %w", err)
	}
	defer rows.Close()
	
	var results []StrategyPnLTimeseriesRecord
	for rows.Next() {
		var record StrategyPnLTimeseriesRecord
		if err := rows.Scan(
			&record.StrategyID,
			&record.StrategyName,
			&record.TotalPnL,
			&record.PaperTrading,
			&record.Timestamp,
		); err != nil {
			return nil, fmt.Errorf("failed to scan strategy P&L record: %w", err)
		}
		results = append(results, record)
	}
	
	return results, nil
}

// GetAggregatedStrategyPnL retrieves aggregated strategy P&L data from continuous aggregate
func (t *TimescaleDB) GetAggregatedStrategyPnL(ctx context.Context, strategyID int, paperTrading bool, startTime, endTime time.Time) ([]map[string]interface{}, error) {
	query := `
		SELECT 
			bucket as time,
			strategy_id,
			strategy_name,
			paper_trading,
			avg_pnl,
			min_pnl,
			max_pnl,
			last_pnl
		FROM strategy_pnl_hourly
		WHERE strategy_id = $1 AND paper_trading = $2 AND bucket BETWEEN $3 AND $4
		ORDER BY bucket
	`
	
	rows, err := t.pool.Query(ctx, query, strategyID, paperTrading, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query aggregated strategy P&L: %w", err)
	}
	defer rows.Close()
	
	var results []map[string]interface{}
	for rows.Next() {
		var (
			timestamp     time.Time
			strategyID    int
			strategyName  string
			paperTrading  bool
			avgPnL        float64
			minPnL        float64
			maxPnL        float64
			lastPnL       float64
		)

		if err := rows.Scan(
			&timestamp,
			&strategyID,
			&strategyName,
			&paperTrading,
			&avgPnL,
			&minPnL,
			&maxPnL,
			&lastPnL,
		); err != nil {
			return nil, fmt.Errorf("failed to scan aggregated strategy P&L record: %w", err)
		}

		result := map[string]interface{}{
			"time":           timestamp,
			"strategy_id":    strategyID,
			"strategy_name":  strategyName,
			"paper_trading":  paperTrading,
			"avg_pnl":        avgPnL,
			"min_pnl":        minPnL,
			"max_pnl":        maxPnL,
			"last_pnl":       lastPnL,
		}
		results = append(results, result)
	}
	
	return results, nil
}
