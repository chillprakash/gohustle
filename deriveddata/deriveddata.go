package deriveddata

import (
	"context"
	"fmt"
	"gohustle/cache"
	"gohustle/core"
	"gohustle/logger"
	"math"
	"time"
)

// IndexMetrics holds the calculated metrics for an index
type IndexMetrics struct {
	SpotPrice     float64
	FairPrice     float64
	StraddlePrice float64
	Timestamp     int64
}

// IndexDataManager handles derived data calculations
type IndexDataManager struct {
	log        *logger.Logger
	redisCache *cache.RedisCache
}

// NewIndexDataManager creates a new instance of IndexDataManager
func NewIndexDataManager() (*IndexDataManager, error) {
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return nil, fmt.Errorf("failed to get redis cache: %w", err)
	}

	return &IndexDataManager{
		log:        logger.L(),
		redisCache: redisCache,
	}, nil
}

// CalculateAndStoreIndexMetrics calculates and stores metrics for the given index
func (idm *IndexDataManager) CalculateAndStoreIndexMetrics(ctx context.Context, index core.Index) error {
	// Get time series DB for storing derived data
	tsDB := idm.redisCache.GetTimeSeriesDB()
	if tsDB == nil {
		return fmt.Errorf("time series DB not available")
	}

	// Calculate metrics
	metrics, err := idm.calculateIndexMetrics(ctx, index)
	if err != nil {
		return fmt.Errorf("failed to calculate metrics: %w", err)
	}

	// Store in Redis time series DB
	pipe := tsDB.Pipeline()
	baseKey := fmt.Sprintf("derived:%s", index.NameInOptions)

	// Store each metric with current timestamp
	pipe.Set(ctx, fmt.Sprintf("%s:spot", baseKey), fmt.Sprintf("%.2f", metrics.SpotPrice), time.Hour)
	pipe.Set(ctx, fmt.Sprintf("%s:fair", baseKey), fmt.Sprintf("%.2f", metrics.FairPrice), time.Hour)
	pipe.Set(ctx, fmt.Sprintf("%s:straddle", baseKey), fmt.Sprintf("%.2f", metrics.StraddlePrice), time.Hour)
	pipe.Set(ctx, fmt.Sprintf("%s:timestamp", baseKey), fmt.Sprintf("%d", metrics.Timestamp), time.Hour)

	// Execute pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to store metrics: %w", err)
	}

	idm.log.Info("Stored index metrics", map[string]interface{}{
		"index":     index.NameInOptions,
		"spot":      metrics.SpotPrice,
		"fair":      metrics.FairPrice,
		"straddle":  metrics.StraddlePrice,
		"timestamp": metrics.Timestamp,
	})

	return nil
}

// calculateIndexMetrics calculates all required metrics for an index
func (idm *IndexDataManager) calculateIndexMetrics(ctx context.Context, index core.Index) (*IndexMetrics, error) {
	// Get LTP DB for current prices
	ltpDB := idm.redisCache.GetLTPDB3()
	if ltpDB == nil {
		return nil, fmt.Errorf("LTP DB not available")
	}

	// Get spot price from Redis
	spotKey := fmt.Sprintf("%s_ltp", index.InstrumentToken)
	spotStr, err := ltpDB.Get(ctx, spotKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get spot price: %w", err)
	}

	var spotPrice float64
	if _, err := fmt.Sscanf(spotStr, "%f", &spotPrice); err != nil {
		return nil, fmt.Errorf("failed to parse spot price: %w", err)
	}

	// Calculate ATM strike
	var strikeInterval float64
	switch index.NameInOptions {
	case "NIFTY":
		strikeInterval = 50
	case "BANKNIFTY":
	case "SENSEX":
		strikeInterval = 100
	default:
		strikeInterval = 50
	}
	atmStrike := getNearestRound(spotPrice, strikeInterval)

	// TODO: Calculate fair price using futures
	fairPrice := spotPrice // Temporary, need to implement futures calculation

	// Calculate straddle price using ATM options
	straddlePrice, err := idm.calculateStraddlePrice(ctx, index, atmStrike)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate straddle price: %w", err)
	}

	return &IndexMetrics{
		SpotPrice:     spotPrice,
		FairPrice:     fairPrice,
		StraddlePrice: straddlePrice,
		Timestamp:     time.Now().Unix(),
	}, nil
}

// calculateStraddlePrice calculates the straddle price for ATM strike
func (idm *IndexDataManager) calculateStraddlePrice(ctx context.Context, index core.Index, atmStrike float64) (float64, error) {
	ltpDB := idm.redisCache.GetLTPDB3()
	if ltpDB == nil {
		return 0, fmt.Errorf("LTP DB not available")
	}

	// Get current expiry from Redis or cache
	// TODO: Implement expiry lookup
	currentExpiry := "25APR" // Temporary hardcoded value

	// Form CE and PE symbols
	ceSymbol := fmt.Sprintf("%s%s%.0fCE", index.NameInOptions, currentExpiry, atmStrike)
	peSymbol := fmt.Sprintf("%s%s%.0fPE", index.NameInOptions, currentExpiry, atmStrike)

	// Get instrument tokens for CE and PE
	ceKey := fmt.Sprintf("token:%s", ceSymbol)
	peKey := fmt.Sprintf("token:%s", peSymbol)

	// Get tokens from Redis
	ceToken, err := ltpDB.Get(ctx, ceKey).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get CE token: %w", err)
	}

	peToken, err := ltpDB.Get(ctx, peKey).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get PE token: %w", err)
	}

	// Get LTP for both options
	ceLTPKey := fmt.Sprintf("%s_ltp", ceToken)
	peLTPKey := fmt.Sprintf("%s_ltp", peToken)

	// Use pipeline for efficient fetching
	pipe := ltpDB.Pipeline()
	ceLTPCmd := pipe.Get(ctx, ceLTPKey)
	peLTPCmd := pipe.Get(ctx, peLTPKey)

	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("failed to get option LTPs: %w", err)
	}

	// Parse LTP values
	var ceLTP, peLTP float64
	if _, err := fmt.Sscanf(ceLTPCmd.Val(), "%f", &ceLTP); err != nil {
		return 0, fmt.Errorf("failed to parse CE LTP: %w", err)
	}
	if _, err := fmt.Sscanf(peLTPCmd.Val(), "%f", &peLTP); err != nil {
		return 0, fmt.Errorf("failed to parse PE LTP: %w", err)
	}

	// Calculate straddle price
	straddlePrice := ceLTP + peLTP

	idm.log.Debug("Calculated straddle price", map[string]interface{}{
		"index":          index.NameInOptions,
		"atm_strike":     atmStrike,
		"ce_symbol":      ceSymbol,
		"pe_symbol":      peSymbol,
		"ce_ltp":         ceLTP,
		"pe_ltp":         peLTP,
		"straddle_price": straddlePrice,
	})

	return straddlePrice, nil
}

// Helper function to get nearest round number
func getNearestRound(number float64, base float64) float64 {
	remainder := math.Mod(number, base)
	if remainder >= base/2 {
		return number + base - remainder
	}
	return number - remainder
}

// StartScheduler starts the metrics calculation scheduler
func StartScheduler(ctx context.Context) error {
	idm, err := NewIndexDataManager()
	if err != nil {
		return fmt.Errorf("failed to create index data manager: %w", err)
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	indices := core.GetIndices()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Calculate for each index
			for _, index := range []core.Index{indices.NIFTY, indices.BANKNIFTY, indices.SENSEX} {
				if err := idm.CalculateAndStoreIndexMetrics(ctx, index); err != nil {
					idm.log.Error("Failed to calculate metrics", map[string]interface{}{
						"index": index.NameInOptions,
						"error": err.Error(),
					})
					continue
				}
			}
		}
	}
}
