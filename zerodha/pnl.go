package zerodha

import (
	"context"
	"fmt"
	sync "sync"
	"time"

	"gohustle/appparameters"
	"gohustle/cache"
	"gohustle/db"
	"gohustle/logger"
	"gohustle/utils"

	"github.com/redis/go-redis/v9"
)

// PnLManager handles P&L calculations for positions
type PnLManager struct {
	log             *logger.Logger
	timescaleDB     *db.TimescaleDB
	redisCache      *redis.Client
	cacheMeta       *cache.CacheMeta
	appParamManager *appparameters.AppParameterManager
}

// PnLSummary represents a summary of P&L across all positions
type PnLSummary struct {
	TotalRealizedPnL   float64                       `json:"total_realized_pnl"`
	TotalUnrealizedPnL float64                       `json:"total_unrealized_pnl"`
	TotalPnL           float64                       `json:"total_pnl"`
	PositionPnL        map[string]float64            `json:"position_pnl"`       // Map of trading symbol to P&L
	PaperPositionPnL   map[string]float64            `json:"paper_position_pnl"` // Map of trading symbol to paper trading P&L
	StrategyPnL        map[string]StrategyPnLSummary `json:"strategy_pnl"`       // Map of strategy name to P&L summary
	PaperStrategyPnL   map[string]StrategyPnLSummary `json:"paper_strategy_pnl"` // Map of strategy name to paper trading P&L summary
	UpdatedAt          time.Time                     `json:"updated_at"`
}

// StrategyPnLSummary represents a summary of P&L for a specific strategy
type StrategyPnLSummary struct {
	RealizedPnL   float64                `json:"realized_pnl"`
	UnrealizedPnL float64                `json:"unrealized_pnl"`
	TotalPnL      float64                `json:"total_pnl"`
	Positions     map[string]PositionPnL `json:"positions"` // Map of position ID to position P&L
}

// PositionPnL represents P&L for a specific position
type PositionPnL struct {
	TradingSymbol string  `json:"trading_symbol"`
	Exchange      string  `json:"exchange"`
	Product       string  `json:"product"`
	Quantity      int     `json:"quantity"`
	AveragePrice  float64 `json:"average_price"`
	LastPrice     float64 `json:"last_price"`
	RealizedPnL   float64 `json:"realized_pnl"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`
	TotalPnL      float64 `json:"total_pnl"`
	BuyValue      float64 `json:"buy_value"`
	SellValue     float64 `json:"sell_value"`
	Multiplier    float64 `json:"multiplier"`
	StrategyID    int     `json:"strategy_id"`
	PaperTrading  bool    `json:"paper_trading"`
	PositionID    *string `json:"position_id"`
}

var (
	pnlInstance *PnLManager
	pnlOnce     sync.Once
)

// NewPnLManager creates a new PnLManager
func NewPnLManager() *PnLManager {
	return &PnLManager{
		log: logger.L(),
	}
}

// getStrategyIDValue safely extracts the strategy ID value from a potentially nil pointer
func getStrategyIDValue(strategyID *int) int {
	if strategyID == nil {
		return 0
	}
	return *strategyID
}

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
			timescaleDB:     db.GetTimescaleDB(),
			redisCache:      redisCache.GetLTPDB3(),
			cacheMeta:       cacheMetaInstance,
			appParamManager: appparameters.GetAppParameterManager(),
		}
		log.Info("PnL manager initialized", map[string]interface{}{})
	})
	return pnlInstance
}

// CalculatePnL calculates P&L for all positions (both real and paper trading)
func (pm *PnLManager) CalculatePnL(ctx context.Context) (*PnLSummary, error) {
	summary := &PnLSummary{
		PositionPnL:      make(map[string]float64),
		PaperPositionPnL: make(map[string]float64),
		StrategyPnL:      make(map[string]StrategyPnLSummary),
		PaperStrategyPnL: make(map[string]StrategyPnLSummary),
		UpdatedAt:        time.Now(),
	}

	// Fetch all positions from database
	dbPositions, err := pm.timescaleDB.ListPositions(ctx)
	if err != nil {
		pm.log.Error("Failed to fetch positions from database", map[string]interface{}{
			"error": err.Error(),
		})
		// Continue with Zerodha positions if DB fetch fails
	} else {
		pm.log.Info("Fetched positions from database for PnL calculation", map[string]interface{}{
			"count": len(dbPositions),
		})
	}

	// Process positions from database
	for _, pos := range dbPositions {
		// Calculate P&L using the formula: pnl = (sellValue - buyValue) + (netQuantity * lastPrice * multiplier)
		lastPrice := pos.LastPrice

		instrumentToken, err := pm.cacheMeta.GetTokenBySymbol(ctx, pos.TradingSymbol)
		if err != nil {
			return nil, fmt.Errorf("failed to get token metadata for trading symbol: %w", err)
		}

		ltpKey := fmt.Sprintf("%v_ltp", instrumentToken)
		ltpVal, err := pm.redisCache.Get(ctx, ltpKey).Float64()
		if err == nil && ltpVal > 0 {
			lastPrice = ltpVal
		}

		// Calculate P&L components
		realizedPnL := pos.SellValue - pos.BuyValue
		unrealizedPnL := float64(pos.Quantity) * lastPrice * pos.Multiplier
		totalPnL := realizedPnL + unrealizedPnL

		// Create position P&L record
		positionPnL := PositionPnL{
			TradingSymbol: pos.TradingSymbol,
			Exchange:      pos.Exchange,
			Product:       pos.Product,
			Quantity:      pos.Quantity,
			AveragePrice:  pos.AveragePrice,
			LastPrice:     lastPrice,
			RealizedPnL:   realizedPnL,
			UnrealizedPnL: unrealizedPnL,
			TotalPnL:      totalPnL,
			BuyValue:      pos.BuyValue,
			SellValue:     pos.SellValue,
			Multiplier:    pos.Multiplier,
			StrategyID:    getStrategyIDValue(pos.StrategyID),
			PaperTrading:  pos.PaperTrading,
			PositionID:    pos.PositionID,
		}

		// Log position P&L details
		pm.log.Debug("Calculated P&L for position", map[string]interface{}{
			"trading_symbol": pos.TradingSymbol,
			"position_id":    pos.PositionID,
			"strategy_id":    pos.StrategyID,
			"realized_pnl":   realizedPnL,
			"unrealized_pnl": unrealizedPnL,
			"total_pnl":      totalPnL,
			"paper_trading":  pos.PaperTrading,
		})

		// Update summary
		if pos.PaperTrading {
			summary.PaperPositionPnL[pos.TradingSymbol] = totalPnL

			// Update strategy P&L summary for paper trading
			if pos.StrategyID != nil && *pos.StrategyID != 0 {
				strategyID := getStrategyIDValue(pos.StrategyID)
				strategySummary, exists := summary.PaperStrategyPnL[fmt.Sprintf("%d", strategyID)]
				if !exists {
					strategySummary = StrategyPnLSummary{
						Positions: make(map[string]PositionPnL),
					}
				}

				// Add position to strategy summary
				positionKey := "unknown"
				if pos.PositionID != nil {
					positionKey = *pos.PositionID
				} else {
					// Use ID as fallback if PositionID is nil
					positionKey = fmt.Sprintf("id_%d", pos.ID)
				}
				strategySummary.Positions[positionKey] = positionPnL
				strategySummary.RealizedPnL += realizedPnL
				strategySummary.UnrealizedPnL += unrealizedPnL
				strategySummary.TotalPnL += totalPnL

				// Update the strategy summary in the main summary
				summary.PaperStrategyPnL[fmt.Sprintf("%d", strategyID)] = strategySummary
			}
		} else {
			summary.PositionPnL[pos.TradingSymbol] = totalPnL
			summary.TotalRealizedPnL += realizedPnL
			summary.TotalUnrealizedPnL += unrealizedPnL

			// Update strategy P&L summary for real trading
			if pos.StrategyID != nil && *pos.StrategyID != 0 {
				strategyID := getStrategyIDValue(pos.StrategyID)
				strategySummary, exists := summary.StrategyPnL[fmt.Sprintf("%d", strategyID)]
				if !exists {
					strategySummary = StrategyPnLSummary{
						Positions: make(map[string]PositionPnL),
					}
				}

				// Add position to strategy summary
				positionKey := "unknown"
				if pos.PositionID != nil {
					positionKey = *pos.PositionID
				} else {
					// Use ID as fallback if PositionID is nil
					positionKey = fmt.Sprintf("id_%d", pos.ID)
				}
				strategySummary.Positions[positionKey] = positionPnL
				strategySummary.RealizedPnL += realizedPnL
				strategySummary.UnrealizedPnL += unrealizedPnL
				strategySummary.TotalPnL += totalPnL

				// Update the strategy summary in the main summary
				summary.StrategyPnL[fmt.Sprintf("%d", strategyID)] = strategySummary
			}
		}

		// Store updated P&L in database
		if err := pm.updatePositionPnL(ctx, pos.ID, realizedPnL, unrealizedPnL, totalPnL, lastPrice); err != nil {
			pm.log.Error("Failed to update position P&L in database", map[string]interface{}{
				"error":          err.Error(),
				"position_id":    pos.PositionID,
				"trading_symbol": pos.TradingSymbol,
			})
		}
	}

	// Calculate total P&L
	summary.TotalPnL = summary.TotalRealizedPnL + summary.TotalUnrealizedPnL

	return summary, nil
}

// updatePositionPnL updates the P&L values for a position in the database
func (pm *PnLManager) updatePositionPnL(ctx context.Context, positionID int64, realizedPnL, unrealizedPnL, totalPnL, lastPrice float64) error {
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return fmt.Errorf("timescale DB is nil")
	}

	// Update position in database
	query := `
	UPDATE positions 
	SET 
		pnl = $1, 
		realized_pnl = $2, 
		unrealized_pnl = $3,
		last_price = $4,
		updated_at = NOW()
	WHERE 
		id = $5
	`

	_, err := timescaleDB.Exec(ctx, query, totalPnL, realizedPnL, unrealizedPnL, lastPrice, positionID)
	if err != nil {
		return fmt.Errorf("failed to update position P&L: %w", err)
	}

	return nil
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
	// Get Redis cache for LTP data
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return fmt.Errorf("failed to get Redis cache: %w", err)
	}
	ltpDB := redisCache.GetLTPDB3()
	inmemoryCache := cache.GetInMemoryCacheInstance()

	// Get positions from database
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return fmt.Errorf("timescale DB is nil")
	}

	// Fetch all positions from database
	dbPositions, err := timescaleDB.ListPositions(ctx)
	if err != nil {
		pm.log.Error("Failed to fetch positions from database for high-frequency tracking", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to get positions from database: %w", err)
	}

	// Prepare batch insert
	var positionPnLs []db.PositionPnLTimeseriesRecord
	timestamp := time.Now()
	totalPnL := 0.0
	// Process each position
	for _, pos := range dbPositions {
		// Get latest LTP
		lastPrice := pos.LastPrice
		if instrumentToken, exists := inmemoryCache.Get(pos.TradingSymbol); exists {
			ltpKey := fmt.Sprintf("%v_ltp", instrumentToken)
			ltpVal, err := ltpDB.Get(ctx, ltpKey).Float64()
			if err == nil && ltpVal > 0 {
				lastPrice = ltpVal
			}
		}

		// Calculate P&L components
		realizedPnL := pos.SellValue - pos.BuyValue
		unrealizedPnL := float64(pos.Quantity) * lastPrice * pos.Multiplier
		totalPnL := realizedPnL + unrealizedPnL

		// Create position P&L timeseries record
		positionID := "unknown"
		if pos.PositionID != nil {
			positionID = *pos.PositionID
		} else {
			positionID = fmt.Sprintf("id_%d", pos.ID)
		}

		strategyID := 0
		if pos.StrategyID != nil {
			strategyID = *pos.StrategyID
		}

		positionPnL := db.PositionPnLTimeseriesRecord{
			PositionID:    positionID,
			TradingSymbol: pos.TradingSymbol,
			StrategyID:    strategyID,
			Quantity:      pos.Quantity,
			AveragePrice:  pos.AveragePrice,
			LastPrice:     lastPrice,
			RealizedPnL:   realizedPnL,
			UnrealizedPnL: unrealizedPnL,
			TotalPnL:      totalPnL,
			PaperTrading:  pos.PaperTrading,
			Timestamp:     timestamp,
		}

		positionPnLs = append(positionPnLs, positionPnL)

		// Also update the position record with the latest P&L values
		if err := pm.updatePositionPnL(ctx, pos.ID, realizedPnL, unrealizedPnL, totalPnL, lastPrice); err != nil {
			pm.log.Error("Failed to update position P&L", map[string]interface{}{
				"error":          err.Error(),
				"position_id":    positionID,
				"trading_symbol": pos.TradingSymbol,
			})
			// Continue with other positions even if one fails
		}
	}

	exitPnL, _, _ := pm.GetExitPnL(ctx)
	targetPnL, _, _ := pm.GetTargetPnL(ctx)

	if totalPnL > exitPnL {
		pm.log.Info("Position P&L exceeded exit P&L", map[string]interface{}{
			"total_pnl": totalPnL,
			"exit_pnl":  exitPnL,
		})

	}

	if totalPnL > targetPnL {
		pm.log.Info("Position P&L exceeded target P&L", map[string]interface{}{
			"total_pnl":  totalPnL,
			"target_pnl": targetPnL,
		})

	}

	// Batch insert all position P&L records if there are any
	if len(positionPnLs) > 0 {
		if err := timescaleDB.BatchInsertPositionPnLTimeseries(ctx, positionPnLs); err != nil {
			pm.log.Error("Failed to batch insert position P&L timeseries", map[string]interface{}{
				"error": err.Error(),
				"count": len(positionPnLs),
			})
			return fmt.Errorf("failed to insert position P&L timeseries: %w", err)
		}

		pm.log.Debug("Successfully stored high-frequency P&L data", map[string]interface{}{
			"count":     len(positionPnLs),
			"timestamp": timestamp,
		})
	}

	return nil
}
