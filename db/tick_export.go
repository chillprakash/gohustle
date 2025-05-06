package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// TickDataSummary represents available tick data by date
type TickDataSummary struct {
	Date          string  `json:"date"`
	IndexName     string  `json:"index_name"`
	TickCount     int64   `json:"tick_count"`
	FirstTickTime string  `json:"first_tick_time"`
	LastTickTime  string  `json:"last_tick_time"`
	SizeEstimateKB float64 `json:"size_estimate_kb"`
}

// TickData represents a single market tick for export
type TickData struct {
	ID                   int64     `json:"id"`
	InstrumentToken      int       `json:"instrument_token"`
	ExchangeTimestamp    time.Time `json:"exchange_timestamp"`
	LastPrice            float64   `json:"last_price"`
	OpenInterest         int       `json:"open_interest"`
	VolumeTraded         int       `json:"volume_traded"`
	AverageTradePrice    float64   `json:"average_trade_price"`
	TickReceivedTime     time.Time `json:"tick_received_time"`
	TickStoredInDBTime   time.Time `json:"tick_stored_in_db_time"`
}

// GetAvailableTickDates returns a list of dates with available tick data
// If indexName is provided, only returns data for that index
func (t *TimescaleDB) GetAvailableTickDates(ctx context.Context, indexName string) (map[string][]TickDataSummary, error) {
	// Build query based on whether we're filtering by index
	var query string
	if indexName == "" {
		// Query for both NIFTY and SENSEX ticks
		query = `
			WITH nifty_summary AS (
				SELECT 
					date_trunc('day', exchange_unix_timestamp) as date,
					'NIFTY' as index_name,
					COUNT(*) as tick_count,
					MIN(exchange_unix_timestamp) as first_tick,
					MAX(exchange_unix_timestamp) as last_tick,
					COUNT(*) * 100 / 1024.0 as size_estimate_kb
				FROM nifty_ticks
				GROUP BY date_trunc('day', exchange_unix_timestamp)
			),
			sensex_summary AS (
				SELECT 
					date_trunc('day', exchange_unix_timestamp) as date,
					'SENSEX' as index_name,
					COUNT(*) as tick_count,
					MIN(exchange_unix_timestamp) as first_tick,
					MAX(exchange_unix_timestamp) as last_tick,
					COUNT(*) * 100 / 1024.0 as size_estimate_kb
				FROM sensex_ticks
				GROUP BY date_trunc('day', exchange_unix_timestamp)
			)
			SELECT * FROM nifty_summary
			UNION ALL
			SELECT * FROM sensex_summary
			ORDER BY date DESC, index_name
		`
	} else if indexName == "NIFTY" {
		// Query only for NIFTY ticks
		query = `
			SELECT 
				date_trunc('day', exchange_unix_timestamp) as date,
				'NIFTY' as index_name,
				COUNT(*) as tick_count,
				MIN(exchange_unix_timestamp) as first_tick,
				MAX(exchange_unix_timestamp) as last_tick,
				COUNT(*) * 100 / 1024.0 as size_estimate_kb
			FROM nifty_ticks
			GROUP BY date_trunc('day', exchange_unix_timestamp)
			ORDER BY date DESC
		`
	} else if indexName == "SENSEX" {
		// Query only for SENSEX ticks
		query = `
			SELECT 
				date_trunc('day', exchange_unix_timestamp) as date,
				'SENSEX' as index_name,
				COUNT(*) as tick_count,
				MIN(exchange_unix_timestamp) as first_tick,
				MAX(exchange_unix_timestamp) as last_tick,
				COUNT(*) * 100 / 1024.0 as size_estimate_kb
			FROM sensex_ticks
			GROUP BY date_trunc('day', exchange_unix_timestamp)
			ORDER BY date DESC
		`
	} else {
		return nil, fmt.Errorf("invalid index name: %s", indexName)
	}
	
	rows, err := t.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tick data summary: %w", err)
	}
	defer rows.Close()
	
	result := make(map[string][]TickDataSummary)
	
	for rows.Next() {
		var summary TickDataSummary
		var date time.Time
		var firstTick, lastTick time.Time
		
		err := rows.Scan(
			&date,
			&summary.IndexName,
			&summary.TickCount,
			&firstTick,
			&lastTick,
			&summary.SizeEstimateKB,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan tick data summary: %w", err)
		}
		
		summary.Date = date.Format("2006-01-02")
		summary.FirstTickTime = firstTick.Format(time.RFC3339)
		summary.LastTickTime = lastTick.Format(time.RFC3339)
		
		dateKey := summary.Date
		result[dateKey] = append(result[dateKey], summary)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tick data summary rows: %w", err)
	}
	
	return result, nil
}

// GetTickDataForExport retrieves tick data for a specific index and date range
func (t *TimescaleDB) GetTickDataForExport(ctx context.Context, indexName string, startDate, endDate time.Time) (pgx.Rows, error) {
	tableName := ""
	switch indexName {
	case "NIFTY":
		tableName = "nifty_ticks"
	case "SENSEX":
		tableName = "sensex_ticks"
	default:
		return nil, fmt.Errorf("invalid index name: %s", indexName)
	}
	
	query := fmt.Sprintf(`
		SELECT 
			id,
			instrument_token,
			exchange_unix_timestamp,
			last_price,
			open_interest,
			volume_traded,
			average_trade_price,
			tick_received_time,
			tick_stored_in_db_time
		FROM %s
		WHERE 
			exchange_unix_timestamp >= $1 AND 
			exchange_unix_timestamp < $2
		ORDER BY exchange_unix_timestamp
	`, tableName)
	
	// Add one day to end date to include the entire day
	endDateInclusive := endDate.Add(24 * time.Hour)
	
	rows, err := t.pool.Query(ctx, query, startDate, endDateInclusive)
	if err != nil {
		return nil, fmt.Errorf("failed to query tick data: %w", err)
	}
	
	return rows, nil
}

// DeleteTickData deletes tick data for a specific index and date range
func (t *TimescaleDB) DeleteTickData(ctx context.Context, indexName string, startDate, endDate time.Time) (int64, error) {
	tableName := ""
	switch indexName {
	case "NIFTY":
		tableName = "nifty_ticks"
	case "SENSEX":
		tableName = "sensex_ticks"
	default:
		return 0, fmt.Errorf("invalid index name: %s", indexName)
	}
	
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE 
			exchange_unix_timestamp >= $1 AND 
			exchange_unix_timestamp < $2
	`, tableName)
	
	// Add one day to end date to include the entire day
	endDateInclusive := endDate.Add(24 * time.Hour)
	
	result, err := t.pool.Exec(ctx, query, startDate, endDateInclusive)
	if err != nil {
		return 0, fmt.Errorf("failed to delete tick data: %w", err)
	}
	
	return result.RowsAffected(), nil
}
