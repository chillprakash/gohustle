package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// IndexMetrics holds the calculated metrics for an index
type IndexMetrics struct {
	ID            int64     `db:"id"`
	IndexName     string    `db:"index_name"`
	SpotPrice     float64   `db:"spot_price"`
	FairPrice     float64   `db:"fair_price"`
	StraddlePrice float64   `db:"straddle_price"`
	Timestamp     time.Time `db:"timestamp"`
}

// StraddleMetrics holds detailed information about straddle calculations
type StraddleMetrics struct {
	ID            int64     `db:"id"`
	IndexName     string    `db:"index_name"`
	Strike        float64   `db:"strike"`
	StraddlePrice float64   `db:"straddle_price"`
	FairPrice     float64   `db:"fair_price"`
	CallLTP       float64   `db:"call_ltp"`
	PutLTP        float64   `db:"put_ltp"`
	Timestamp     time.Time `db:"timestamp"`
}

// StoreIndexMetrics stores index metrics in TimescaleDB
func (t *TimescaleDB) StoreIndexMetrics(indexName string, metrics *IndexMetrics) error {
	ctx := context.Background()
	query := `
		INSERT INTO index_metrics (
			index_name, spot_price, fair_price, straddle_price, timestamp
		) VALUES (
			@index_name, @spot_price, @fair_price, @straddle_price, @timestamp
		)
	`

	args := pgx.NamedArgs{
		"index_name":     indexName,
		"spot_price":     metrics.SpotPrice,
		"fair_price":     metrics.FairPrice,
		"straddle_price": metrics.StraddlePrice,
		"timestamp":      pgtype.Timestamp{Time: metrics.Timestamp, Valid: true},
	}

	_, err := t.pool.Exec(ctx, query, args)
	if err != nil {
		t.log.Error("Failed to store index metrics", map[string]interface{}{
			"error":      err.Error(),
			"index_name": indexName,
		})
		return fmt.Errorf("failed to store index metrics: %w", err)
	}

	return nil
}

// StoreStraddleMetrics stores straddle metrics in TimescaleDB
func (t *TimescaleDB) StoreStraddleMetrics(indexName string, metrics *StraddleMetrics) error {
	ctx := context.Background()
	query := `
		INSERT INTO straddle_metrics (
			index_name, strike, straddle_price, fair_price, call_ltp, put_ltp, timestamp
		) VALUES (
			@index_name, @strike, @straddle_price, @fair_price, @call_ltp, @put_ltp, @timestamp
		)
	`

	args := pgx.NamedArgs{
		"index_name":     indexName,
		"strike":         metrics.Strike,
		"straddle_price": metrics.StraddlePrice,
		"fair_price":     metrics.FairPrice,
		"call_ltp":       metrics.CallLTP,
		"put_ltp":        metrics.PutLTP,
		"timestamp":      pgtype.Timestamp{Time: metrics.Timestamp, Valid: true},
	}

	_, err := t.pool.Exec(ctx, query, args)
	if err != nil {
		t.log.Error("Failed to store straddle metrics", map[string]interface{}{
			"error":      err.Error(),
			"index_name": indexName,
			"strike":     metrics.Strike,
		})
		return fmt.Errorf("failed to store straddle metrics: %w", err)
	}

	return nil
}

// GetLatestIndexMetrics retrieves the latest metrics for a given index
func (t *TimescaleDB) GetLatestIndexMetrics(indexName string, limit int) ([]*IndexMetrics, error) {
	ctx := context.Background()
	query := `
		SELECT id, index_name, spot_price, fair_price, straddle_price, timestamp
		FROM index_metrics
		WHERE index_name = $1
		ORDER BY timestamp DESC
		LIMIT $2
	`

	rows, err := t.pool.Query(ctx, query, indexName, limit)
	if err != nil {
		t.log.Error("Failed to query index metrics", map[string]interface{}{
			"error":      err.Error(),
			"index_name": indexName,
		})
		return nil, fmt.Errorf("failed to query index metrics: %w", err)
	}
	defer rows.Close()

	var metrics []*IndexMetrics
	for rows.Next() {
		m := &IndexMetrics{}
		if err := rows.Scan(
			&m.ID,
			&m.IndexName,
			&m.SpotPrice,
			&m.FairPrice,
			&m.StraddlePrice,
			&m.Timestamp,
		); err != nil {
			t.log.Error("Failed to scan index metrics row", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, fmt.Errorf("failed to scan index metrics row: %w", err)
		}
		metrics = append(metrics, m)
	}

	return metrics, nil
}

// GetStraddleMetrics retrieves straddle metrics for a given index and strike
func (t *TimescaleDB) GetStraddleMetrics(indexName string, strike float64, limit int) ([]*StraddleMetrics, error) {
	ctx := context.Background()
	query := `
		SELECT id, index_name, strike, straddle_price, fair_price, call_ltp, put_ltp, timestamp
		FROM straddle_metrics
		WHERE index_name = $1 AND strike = $2
		ORDER BY timestamp DESC
		LIMIT $3
	`

	rows, err := t.pool.Query(ctx, query, indexName, strike, limit)
	if err != nil {
		t.log.Error("Failed to query straddle metrics", map[string]interface{}{
			"error":      err.Error(),
			"index_name": indexName,
			"strike":     strike,
		})
		return nil, fmt.Errorf("failed to query straddle metrics: %w", err)
	}
	defer rows.Close()

	var metrics []*StraddleMetrics
	for rows.Next() {
		m := &StraddleMetrics{}
		if err := rows.Scan(
			&m.ID,
			&m.IndexName,
			&m.Strike,
			&m.StraddlePrice,
			&m.FairPrice,
			&m.CallLTP,
			&m.PutLTP,
			&m.Timestamp,
		); err != nil {
			t.log.Error("Failed to scan straddle metrics row", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, fmt.Errorf("failed to scan straddle metrics row: %w", err)
		}
		metrics = append(metrics, m)
	}

	return metrics, nil
}

// GetIndexMetricsInTimeRange retrieves index metrics within the specified time range
func (t *TimescaleDB) GetIndexMetricsInTimeRange(indexName string, startTime, endTime time.Time) ([]*IndexMetrics, error) {
	ctx := context.Background()
	query := `
		SELECT id, index_name, spot_price, fair_price, straddle_price, timestamp
		FROM index_metrics
		WHERE index_name = $1 AND timestamp >= $2 AND timestamp <= $3
		ORDER BY timestamp ASC
	`

	rows, err := t.pool.Query(ctx, query, indexName, startTime, endTime)
	if err != nil {
		t.log.Error("Failed to query index metrics in time range", map[string]interface{}{
			"error":      err.Error(),
			"index_name": indexName,
			"start_time": startTime,
			"end_time":   endTime,
		})
		return nil, fmt.Errorf("failed to query index metrics in time range: %w", err)
	}
	defer rows.Close()

	var metrics []*IndexMetrics
	for rows.Next() {
		m := &IndexMetrics{}
		if err := rows.Scan(
			&m.ID,
			&m.IndexName,
			&m.SpotPrice,
			&m.FairPrice,
			&m.StraddlePrice,
			&m.Timestamp,
		); err != nil {
			t.log.Error("Failed to scan index metrics row", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, fmt.Errorf("failed to scan index metrics row: %w", err)
		}
		metrics = append(metrics, m)
	}

	return metrics, nil
}

// GetStraddleMetricsInTimeRange retrieves straddle metrics within the specified time range
func (t *TimescaleDB) GetStraddleMetricsInTimeRange(indexName string, strike float64, startTime, endTime time.Time) ([]*StraddleMetrics, error) {
	ctx := context.Background()
	query := `
		SELECT id, index_name, strike, straddle_price, fair_price, call_ltp, put_ltp, timestamp
		FROM straddle_metrics
		WHERE index_name = $1 AND strike = $2 AND timestamp >= $3 AND timestamp <= $4
		ORDER BY timestamp ASC
	`

	rows, err := t.pool.Query(ctx, query, indexName, strike, startTime, endTime)
	if err != nil {
		t.log.Error("Failed to query straddle metrics in time range", map[string]interface{}{
			"error":      err.Error(),
			"index_name": indexName,
			"strike":     strike,
			"start_time": startTime,
			"end_time":   endTime,
		})
		return nil, fmt.Errorf("failed to query straddle metrics in time range: %w", err)
	}
	defer rows.Close()

	var metrics []*StraddleMetrics
	for rows.Next() {
		m := &StraddleMetrics{}
		if err := rows.Scan(
			&m.ID,
			&m.IndexName,
			&m.Strike,
			&m.StraddlePrice,
			&m.FairPrice,
			&m.CallLTP,
			&m.PutLTP,
			&m.Timestamp,
		); err != nil {
			t.log.Error("Failed to scan straddle metrics row", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, fmt.Errorf("failed to scan straddle metrics row: %w", err)
		}
		metrics = append(metrics, m)
	}

	return metrics, nil
}

// CleanupOldMetrics removes metrics older than the retention period
func (t *TimescaleDB) CleanupOldMetrics(retentionPeriod time.Duration) error {
	ctx := context.Background()
	cutoff := time.Now().Add(-retentionPeriod)

	// Delete old index metrics
	_, err := t.pool.Exec(ctx, "DELETE FROM index_metrics WHERE timestamp < $1", cutoff)
	if err != nil {
		t.log.Error("Failed to clean up old index metrics", map[string]interface{}{
			"error":  err.Error(),
			"cutoff": cutoff,
		})
		return fmt.Errorf("failed to clean up old index metrics: %w", err)
	}

	// Delete old straddle metrics
	_, err = t.pool.Exec(ctx, "DELETE FROM straddle_metrics WHERE timestamp < $1", cutoff)
	if err != nil {
		t.log.Error("Failed to clean up old straddle metrics", map[string]interface{}{
			"error":  err.Error(),
			"cutoff": cutoff,
		})
		return fmt.Errorf("failed to clean up old straddle metrics: %w", err)
	}

	return nil
}
