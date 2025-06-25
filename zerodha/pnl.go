package zerodha

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"gohustle/appparameters"
	"gohustle/cache"
	"gohustle/logger"
	"gohustle/utils"

	"github.com/redis/go-redis/v9"
)

type pnl struct {
	ID         int64     `json:"id"`
	StrategyID int64     `json:"strategy_id"`
	TotalPNL   float64   `json:"total_pnl"`
	CreatedAt  time.Time `json:"created_at"`
}

// PnLManager handles P&L calculations for positions
type PnLManager struct {
	log             *logger.Logger
	redisCache      *redis.Client
	cacheMeta       *cache.CacheMeta
	appParamManager *appparameters.AppParameterManager
}

// PnLSummary represents a summary of P&L across all positions
type PnLSummary struct {
	PositionsPNL float64   `json:"position_pnl"` // Total P&L across all positions
	UpdatedAt    time.Time `json:"updated_at"`
}

var (
	pnlInstance *PnLManager
	pnlOnce     sync.Once
)

// GetPnLManager returns a singleton instance of PnLManager
func GetPnLManager() *PnLManager {
	pnlOnce.Do(func() {
		log := logger.L()
		cacheMetaInstance, err := cache.GetCacheMetaInstance()
		if err != nil {
			log.Error("Failed to get cache meta instance", map[string]interface{}{
				"error": err.Error(),
			})
		}
		redisCache, err := cache.GetRedisCache()
		if err != nil {
			log.Error("Failed to get Redis cache", map[string]interface{}{
				"error": err.Error(),
			})
		}
		pnlInstance = &PnLManager{
			log:             log,
			redisCache:      redisCache.GetLTPDB3(),
			cacheMeta:       cacheMetaInstance,
			appParamManager: appparameters.GetAppParameterManager(),
		}
		log.Info("PnL manager initialized", map[string]interface{}{})
	})
	return pnlInstance
}

// CalculatePnL calculates P&L for all positions
func (pm *PnLManager) CalculatePnL(ctx context.Context) (*PnLSummary, error) {
	summary := &PnLSummary{
		PositionsPNL: 0,
		UpdatedAt:    time.Now(),
	}

	positionManagerInstance := GetPositionManager()
	cacheMetaInstance, err := cache.GetCacheMetaInstance()
	if err != nil {
		pm.log.Error("Failed to get cache meta instance", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to get cache meta instance: %w", err)
	}

	positions, err := positionManagerInstance.ListPositionsFromDB(ctx)
	if err != nil {
		pm.log.Error("Failed to fetch positions from database", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to fetch positions: %w", err)
	}

	// Process positions from database
	for _, pos := range positions {
		// Calculate P&L using the formula: pnl = (sellValue - buyValue) + (netQuantity * lastPrice * multiplier)
		lastPrice, err := cacheMetaInstance.GetLTPforInstrumentToken(ctx, utils.Uint32ToString(pos.InstrumentToken))
		if err != nil {
			pm.log.Error("Failed to get LTP for instrument token", map[string]interface{}{
				"error":            err.Error(),
				"instrument_token": pos.InstrumentToken,
				"trading_symbol":   pos.TradingSymbol,
			})
			continue // Skip this position but continue with others
		}

		netQuantity := pos.SellQuantity - pos.BuyQuantity
		realizedPnL := pos.SellValue - pos.BuyValue
		unrealizedPnL := float64(netQuantity) * lastPrice.LTP * pos.Multiplier
		totalPnL := realizedPnL + unrealizedPnL

		// Add to total P&L
		summary.PositionsPNL += totalPnL

		// Log individual position P&L for debugging
		pm.log.Debug("Position P&L calculated", map[string]interface{}{
			"trading_symbol": pos.TradingSymbol,
			"net_quantity":   netQuantity,
			"realized_pnl":   realizedPnL,
			"unrealized_pnl": unrealizedPnL,
			"total_pnl":      totalPnL,
			"last_price":     lastPrice.LTP,
		})
	}

	return summary, nil
}

// SchedulePnLCalculation sets up periodic P&L calculation
func (pm *PnLManager) SchedulePnLCalculation(ctx context.Context, intervalSeconds int) {
	ticker := time.NewTicker(time.Duration(intervalSeconds) * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				_, err := pm.CalculatePnL(ctx)
				if err != nil {
					pm.log.Error("Failed to calculate P&L", map[string]interface{}{
						"error": err.Error(),
					})
				} else {
					pm.log.Info("Successfully calculated P&L", map[string]interface{}{
						"interval_seconds": intervalSeconds,
					})
				}
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
	pm.log.Info("Scheduled P&L calculation", map[string]interface{}{
		"interval_seconds": intervalSeconds,
	})
}

func (pm *PnLManager) GetExitPnL(ctx context.Context) (float64, bool, error) {
	param, err := pm.appParamManager.GetParameter(ctx, appparameters.AppParamExitPNL)
	if err != nil {
		return 0, false, err
	}
	value := utils.StringToFloat64(param.Value)
	return value, true, nil
}

func (pm *PnLManager) GetTargetPnL(ctx context.Context) (float64, bool, error) {
	param, err := pm.appParamManager.GetParameter(ctx, appparameters.AppParamTargetPNL)
	if err != nil {
		return 0, false, err
	}
	value := utils.StringToFloat64(param.Value)
	return value, true, nil
}

// CalculateAndStorePositionPnL calculates and stores P&L for all positions
func (pm *PnLManager) CalculateAndStorePositionPnL(ctx context.Context) error {
	calculationSummary, err := pm.CalculatePnL(ctx)
	if err != nil {
		return fmt.Errorf("failed to calculate P&L: %w", err)
	}

	exitPnL, _, _ := pm.GetExitPnL(ctx)
	targetPnL, _, _ := pm.GetTargetPnL(ctx)

	if calculationSummary.PositionsPNL > exitPnL {
		pm.log.Info("Position P&L exceeded exit P&L", map[string]interface{}{
			"total_pnl": calculationSummary.PositionsPNL,
			"exit_pnl":  exitPnL,
		})
	}

	if calculationSummary.PositionsPNL > targetPnL {
		pm.log.Info("Position P&L exceeded target P&L", map[string]interface{}{
			"total_pnl":  calculationSummary.PositionsPNL,
			"target_pnl": targetPnL,
		})
	}

	// Create a single PNL record for real trading positions
	positionPNLRecord := &pnl{
		StrategyID: 0, // 0 for real trading
		TotalPNL:   calculationSummary.PositionsPNL,
		CreatedAt:  time.Now(),
	}

	// Store PNL to Redis instead of DB
	if err := pm.storePNLToRedis(ctx, positionPNLRecord); err != nil {
		pm.log.Error("Failed to store P&L to Redis", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to store P&L to Redis: %w", err)
	}

	return nil
}

// storePNLToRedis stores a PnL record in Redis
func (pm *PnLManager) storePNLToRedis(ctx context.Context, record *pnl) error {
	if record == nil {
		return nil
	}

	if pm.redisCache == nil {
		return fmt.Errorf("Redis cache is nil")
	}

	// Create a key for the PnL record with timestamp
	timestamp := record.CreatedAt.Format("2006-01-02T15:04:05")
	key := fmt.Sprintf("pnl:%d:%s", record.StrategyID, timestamp)

	// Marshal the record to JSON
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal PnL record: %w", err)
	}

	// Store in Redis with 24-hour expiry
	if err := pm.redisCache.Set(ctx, key, string(data), 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to store PnL in Redis: %w", err)
	}

	// Also store the latest PnL value under a fixed key
	latestKey := fmt.Sprintf("pnl:latest:%d", record.StrategyID)
	if err := pm.redisCache.Set(ctx, latestKey, string(data), 0).Err(); err != nil {
		pm.log.Error("Failed to update latest PnL in Redis", map[string]interface{}{
			"error": err.Error(),
		})
		// Don't return error here, as the main storage succeeded
	}

	pm.log.Info("Stored PnL record in Redis", map[string]interface{}{
		"strategy_id": record.StrategyID,
		"total_pnl":   record.TotalPNL,
		"timestamp":   timestamp,
	})

	return nil
}

// GetLatestPnL retrieves the latest PnL record from Redis
func (pm *PnLManager) GetLatestPnL(ctx context.Context, strategyID int) (*pnl, error) {
	if pm.redisCache == nil {
		return nil, fmt.Errorf("Redis cache is nil")
	}

	key := fmt.Sprintf("pnl:latest:%d", strategyID)
	data, err := pm.redisCache.Get(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get latest PnL from Redis: %w", err)
	}

	if data == "" {
		return nil, fmt.Errorf("no PnL record found for strategy ID %d", strategyID)
	}

	record := &pnl{}
	if err := json.Unmarshal([]byte(data), record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal PnL record: %w", err)
	}

	return record, nil
}
