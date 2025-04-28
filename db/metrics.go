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
	ATMStrike     float64   `db:"atm_strike"`
	Timestamp     time.Time `db:"timestamp"`
}


// StoreIndexMetrics stores index metrics in TimescaleDB
func (t *TimescaleDB) StoreIndexMetrics(indexName string, metrics *IndexMetrics) error {
	ctx := context.Background()
	query := `
		INSERT INTO index_metrics (
			index_name, spot_price, fair_price, straddle_price, atm_strike, timestamp
		) VALUES (
			@index_name, @spot_price, @fair_price, @straddle_price, @atm_strike, @timestamp
		)
	`

	args := pgx.NamedArgs{
		"index_name":     indexName,
		"spot_price":     metrics.SpotPrice,
		"fair_price":     metrics.FairPrice,
		"straddle_price": metrics.StraddlePrice,
		"atm_strike":     metrics.ATMStrike,
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


	return nil
}
