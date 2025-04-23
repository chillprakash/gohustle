package db

import (
	"database/sql"
	"fmt"
	"time"

	"gohustle/types"
)

// MetricsStore handles database operations for time series metrics
type MetricsStore struct {
	db *sql.DB
}

var metricsStore *MetricsStore

// GetMetricsStore returns a singleton instance of MetricsStore
func GetMetricsStore() (*MetricsStore, error) {
	if metricsStore != nil {
		return metricsStore, nil
	}

	sqlHelper, err := GetSQLiteHelper()
	if err != nil {
		return nil, fmt.Errorf("failed to get SQLite helper: %w", err)
	}

	metricsStore = &MetricsStore{
		db: sqlHelper.db,
	}

	if err := metricsStore.initTable(); err != nil {
		return nil, fmt.Errorf("failed to initialize metrics table: %w", err)
	}

	return metricsStore, nil
}

// initTable creates the metrics table if it doesn't exist
func (m *MetricsStore) initTable() error {
	_, err := m.db.Exec(`
		CREATE TABLE IF NOT EXISTS metrics (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp INTEGER NOT NULL,
			index_name TEXT NOT NULL,
			interval TEXT NOT NULL,
			underlying_price REAL NOT NULL,
			synthetic_future REAL NOT NULL,
			lowest_straddle REAL NOT NULL,
			atm_strike REAL NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(timestamp, index_name, interval)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create metrics table: %w", err)
	}

	// Create indexes for better query performance
	_, err = m.db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp DESC);
		CREATE INDEX IF NOT EXISTS idx_metrics_index_interval ON metrics(index_name, interval);
	`)
	if err != nil {
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	return nil
}

// StoreMetrics stores a single metrics data point
func (m *MetricsStore) StoreMetrics(indexName, interval string, data *types.MetricsData) error {
	_, err := m.db.Exec(`
		INSERT INTO metrics (
			timestamp, index_name, interval,
			underlying_price, synthetic_future, lowest_straddle, atm_strike
		) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(timestamp, index_name, interval) DO UPDATE SET
			underlying_price = excluded.underlying_price,
			synthetic_future = excluded.synthetic_future,
			lowest_straddle = excluded.lowest_straddle,
			atm_strike = excluded.atm_strike
	`,
		data.Timestamp, indexName, interval,
		data.UnderlyingPrice, data.SyntheticFuture,
		data.LowestStraddle, data.ATMStrike,
	)
	return err
}

// GetMetrics retrieves metrics for a given index and interval
func (m *MetricsStore) GetMetrics(indexName, interval string, count int) ([]*types.MetricsData, error) {
	rows, err := m.db.Query(`
		SELECT timestamp, underlying_price, synthetic_future, lowest_straddle, atm_strike
		FROM metrics
		WHERE index_name = ? AND interval = ?
		ORDER BY timestamp DESC
		LIMIT ?
	`, indexName, interval, count)
	if err != nil {
		return nil, fmt.Errorf("failed to query metrics: %w", err)
	}
	defer rows.Close()

	var metrics []*types.MetricsData
	for rows.Next() {
		var m types.MetricsData
		if err := rows.Scan(
			&m.Timestamp,
			&m.UnderlyingPrice,
			&m.SyntheticFuture,
			&m.LowestStraddle,
			&m.ATMStrike,
		); err != nil {
			return nil, fmt.Errorf("failed to scan metric row: %w", err)
		}
		metrics = append(metrics, &m)
	}

	return metrics, nil
}

// CleanupOldMetrics removes metrics older than the retention period
func (m *MetricsStore) CleanupOldMetrics(retentionPeriod time.Duration) error {
	cutoff := time.Now().Add(-retentionPeriod).UnixNano() / int64(time.Millisecond)
	_, err := m.db.Exec("DELETE FROM metrics WHERE timestamp < ?", cutoff)
	return err
}

// GetMetricsAfterTimestamp retrieves metrics newer than the specified timestamp
func (m *MetricsStore) GetMetricsAfterTimestamp(indexName string, timestamp int64) ([]*types.MetricsData, error) {
	rows, err := m.db.Query(`
		SELECT timestamp, underlying_price, synthetic_future, lowest_straddle, atm_strike
		FROM metrics
		WHERE index_name = ? AND timestamp > ?
		ORDER BY timestamp ASC
	`, indexName, timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to query metrics after timestamp: %w", err)
	}
	defer rows.Close()

	var metrics []*types.MetricsData
	for rows.Next() {
		var m types.MetricsData
		if err := rows.Scan(
			&m.Timestamp,
			&m.UnderlyingPrice,
			&m.SyntheticFuture,
			&m.LowestStraddle,
			&m.ATMStrike,
		); err != nil {
			return nil, fmt.Errorf("failed to scan metric row: %w", err)
		}
		metrics = append(metrics, &m)
	}

	return metrics, nil
}

// GetMetricsInTimeRange retrieves metrics within the specified time range
func (m *MetricsStore) GetMetricsInTimeRange(indexName, interval string, startTime, endTime int64) ([]*types.MetricsData, error) {
	rows, err := m.db.Query(`
		SELECT timestamp, underlying_price, synthetic_future, lowest_straddle, atm_strike
		FROM metrics
		WHERE index_name = ? 
		AND interval = ?
		AND timestamp >= ? 
		AND timestamp <= ?
		ORDER BY timestamp DESC
	`, indexName, interval, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query metrics in time range: %w", err)
	}
	defer rows.Close()

	var metrics []*types.MetricsData
	for rows.Next() {
		var m types.MetricsData
		if err := rows.Scan(
			&m.Timestamp,
			&m.UnderlyingPrice,
			&m.SyntheticFuture,
			&m.LowestStraddle,
			&m.ATMStrike,
		); err != nil {
			return nil, fmt.Errorf("failed to scan metric row: %w", err)
		}
		metrics = append(metrics, &m)
	}

	return metrics, nil
}
