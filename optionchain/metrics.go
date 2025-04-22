package optionchain

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/logger"

	"github.com/redis/go-redis/v9"
)

// IntervalConfig defines the configuration for each time interval
type IntervalConfig struct {
	Name     string
	Duration time.Duration
	TTL      time.Duration
}

// MetricsManager handles the storage and retrieval of time series metrics
type MetricsManager struct {
	intervals []IntervalConfig
	mu        sync.RWMutex
}

// MetricsData represents the calculated metrics for storage
type MetricsData struct {
	Timestamp       int64
	UnderlyingPrice float64
	SyntheticFuture float64
	LowestStraddle  float64
	CallLTP         float64
	PutLTP          float64
}

// NewMetricsManager creates a new instance of MetricsManager with default intervals
func NewMetricsManager() *MetricsManager {
	return &MetricsManager{
		intervals: []IntervalConfig{
			{Name: "5s", Duration: 5 * time.Second, TTL: 1 * time.Hour},
			{Name: "10s", Duration: 10 * time.Second, TTL: 2 * time.Hour},
			{Name: "20s", Duration: 20 * time.Second, TTL: 4 * time.Hour},
			{Name: "30s", Duration: 30 * time.Second, TTL: 6 * time.Hour},
		},
	}
}

// shouldStoreForInterval determines if metrics should be stored for the given interval
func (m *MetricsManager) shouldStoreForInterval(timestamp time.Time, duration time.Duration) bool {
	// Get seconds from the start of the day
	secondsFromStart := timestamp.Sub(timestamp.Truncate(24 * time.Hour)).Seconds()

	// Check if current second is divisible by the interval duration
	return int(secondsFromStart)%int(duration.Seconds()) == 0
}

// storeMetricsInPipeline adds commands to the Redis pipeline for storing metrics
func (m *MetricsManager) storeMetricsInPipeline(pipe redis.Pipeliner, baseKey string, data *MetricsData, ttl time.Duration) {
	// Only store non-zero values
	if data.UnderlyingPrice > 0 {
		pipe.ZAdd(context.Background(), fmt.Sprintf("%s:spot", baseKey),
			redis.Z{Score: float64(data.Timestamp), Member: fmt.Sprintf("%.2f", data.UnderlyingPrice)})
		pipe.Expire(context.Background(), fmt.Sprintf("%s:spot", baseKey), ttl)
	}

	if data.SyntheticFuture > 0 {
		pipe.ZAdd(context.Background(), fmt.Sprintf("%s:fair", baseKey),
			redis.Z{Score: float64(data.Timestamp), Member: fmt.Sprintf("%.2f", data.SyntheticFuture)})
		pipe.Expire(context.Background(), fmt.Sprintf("%s:fair", baseKey), ttl)
	}

	if data.LowestStraddle > 0 {
		pipe.ZAdd(context.Background(), fmt.Sprintf("%s:straddle", baseKey),
			redis.Z{Score: float64(data.Timestamp), Member: fmt.Sprintf("%.2f", data.LowestStraddle)})
		pipe.Expire(context.Background(), fmt.Sprintf("%s:straddle", baseKey), ttl)
	}
}

// calculateMetrics calculates all required metrics from the option chain data
func (m *MetricsManager) calculateMetrics(chain []*StrikeData, underlyingPrice float64) *MetricsData {
	var straddle float64
	var callLTP, putLTP float64

	// Find ATM strike data
	for _, item := range chain {
		if item.IsATM && item.CE != nil && item.PE != nil {
			logger.L().Info("Found ATM strike data", map[string]interface{}{
				"strike":      item.Strike,
				"ce_ltp":      item.CE.LTP,
				"pe_ltp":      item.PE.LTP,
				"ce_pe_total": item.CEPETotal,
			})
			straddle = item.CEPETotal
			callLTP = item.CE.LTP
			putLTP = item.PE.LTP
			break
		}
	}

	// Calculate synthetic future only if we have valid values
	var syntheticFuture float64
	if callLTP > 0 && putLTP > 0 {
		syntheticFuture = underlyingPrice + callLTP - putLTP
	}

	return &MetricsData{
		Timestamp:       time.Now().UnixNano() / int64(time.Millisecond),
		UnderlyingPrice: underlyingPrice,
		SyntheticFuture: syntheticFuture,
		LowestStraddle:  straddle,
		CallLTP:         callLTP,
		PutLTP:          putLTP,
	}
}
