package db

import (
	"context"
	"fmt"
	"time"
)

// GetLatestStrategyPnLSummary retrieves the latest P&L summary for all strategies
func (t *TimescaleDB) GetLatestStrategyPnLSummary(ctx context.Context) (map[string]interface{}, error) {
	// Query to get the latest P&L for each strategy, grouped by paper_trading flag
	query := `
		WITH latest_timestamps AS (
			SELECT 
				paper_trading,
				MAX(timestamp) as latest_time
			FROM 
				strategy_pnl_timeseries
			GROUP BY 
				paper_trading
		)
		SELECT 
			s.paper_trading,
			SUM(s.total_pnl) as total_pnl,
			lt.latest_time as timestamp
		FROM 
			strategy_pnl_timeseries s
		JOIN 
			latest_timestamps lt ON s.paper_trading = lt.paper_trading AND s.timestamp = lt.latest_time
		GROUP BY 
			s.paper_trading, lt.latest_time
		ORDER BY 
			s.paper_trading
	`

	rows, err := t.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query latest strategy P&L: %w", err)
	}
	defer rows.Close()

	result := map[string]interface{}{
		"real_trading_pnl":  0.0,
		"paper_trading_pnl": 0.0,
		"timestamp":         time.Now(),
	}

	for rows.Next() {
		var (
			paperTrading bool
			totalPnL     float64
			timestamp    time.Time
		)

		if err := rows.Scan(&paperTrading, &totalPnL, &timestamp); err != nil {
			return nil, fmt.Errorf("failed to scan strategy P&L row: %w", err)
		}

		if paperTrading {
			result["paper_trading_pnl"] = totalPnL
		} else {
			result["real_trading_pnl"] = totalPnL
		}

		// Use the most recent timestamp
		if timestamp.After(result["timestamp"].(time.Time)) {
			result["timestamp"] = timestamp
		}
	}

	return result, nil
}
