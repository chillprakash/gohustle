package deriveddata

import (
	"context"
	"encoding/json"
	"fmt"
	"gohustle/cache"
	"gohustle/core"
	"gohustle/logger"
	"gohustle/optionchain"
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

// StraddleMetrics holds detailed information about straddle calculations
type StraddleMetrics struct {
	Strike        float64
	StraddlePrice float64
	FairPrice     float64
	CallLTP       float64
	PutLTP        float64
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

// CalculateAndStoreIndexMetrics is now a placeholder as metrics are calculated and stored during option chain updates
func (idm *IndexDataManager) CalculateAndStoreIndexMetrics(ctx context.Context, index core.Index) error {
	// Metrics are now calculated and stored during option chain updates in optionchain.OptionChainManager
	// This method is kept for backward compatibility and potential future use
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

	// Calculate fair price using futures
	fairPrice := spotPrice // Temporary, need to implement futures calculation

	// Calculate straddle price using ATM options
	straddlePrice, err := idm.calculateStraddlePrice(ctx, index, atmStrike)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate straddle price: %w", err)
	}

	return &IndexMetrics{
		SpotPrice:     spotPrice,
		FairPrice:     fairPrice,
		StraddlePrice: straddlePrice.StraddlePrice,
		Timestamp:     time.Now().Unix(),
	}, nil
}

// calculateStraddlePrice calculates the straddle price and related metrics for ATM strike
func (idm *IndexDataManager) calculateStraddlePrice(ctx context.Context, index core.Index, atmStrike float64) (*StraddleMetrics, error) {
	// Get option chain manager
	optionChainMgr := optionchain.GetOptionChainManager()
	if optionChainMgr == nil {
		return nil, fmt.Errorf("option chain manager not initialized")
	}

	// Get current expiry from Redis
	ltpDB := idm.redisCache.GetLTPDB3()
	if ltpDB == nil {
		return nil, fmt.Errorf("LTP DB not available")
	}

	// Get current expiry (nearest expiry)
	expiryKey := fmt.Sprintf("instrument:expiries:%s", index.NameInOptions)
	expiriesStr, err := ltpDB.Get(ctx, expiryKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get expiries: %w", err)
	}

	var expiries []string
	if err := json.Unmarshal([]byte(expiriesStr), &expiries); err != nil {
		return nil, fmt.Errorf("failed to unmarshal expiries: %w", err)
	}

	if len(expiries) == 0 {
		return nil, fmt.Errorf("no expiries found for %s", index.NameInOptions)
	}

	// Get option chain for nearest expiry
	chain, err := optionChainMgr.CalculateOptionChain(ctx, index.NameInOptions, expiries[0], 10)
	if err != nil {
		return nil, fmt.Errorf("failed to get option chain: %w", err)
	}

	// Find lowest straddle price and calculate synthetic future
	var lowestStraddle float64 = math.MaxFloat64
	var lowestStrike float64
	var callLTP, putLTP float64

	for _, item := range chain.Chain {
		if item.CE != nil && item.PE != nil {
			straddlePrice := item.CE.LTP + item.PE.LTP
			if straddlePrice < lowestStraddle {
				lowestStraddle = straddlePrice
				lowestStrike = item.Strike
				callLTP = item.CE.LTP
				putLTP = item.PE.LTP
			}
		}
	}

	if lowestStraddle == math.MaxFloat64 {
		return nil, fmt.Errorf("no valid straddle price found")
	}

	// Calculate synthetic future price
	// sf = round((strike + call - put), 2)
	syntheticFuture := math.Round((lowestStrike+callLTP-putLTP)*100) / 100

	metrics := &StraddleMetrics{
		Strike:        lowestStrike,
		StraddlePrice: lowestStraddle,
		FairPrice:     syntheticFuture,
		CallLTP:       callLTP,
		PutLTP:        putLTP,
		Timestamp:     time.Now().Unix(),
	}

	idm.log.Debug("Calculated straddle metrics", map[string]interface{}{
		"index":          index.NameInOptions,
		"strike":         metrics.Strike,
		"straddle_price": metrics.StraddlePrice,
		"fair_price":     metrics.FairPrice,
		"call_ltp":       metrics.CallLTP,
		"put_ltp":        metrics.PutLTP,
		"expiry":         expiries[0],
	})

	return metrics, nil
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
