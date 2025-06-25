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

	"github.com/jackc/pgx/v5"
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
	timescaleDB     *db.TimescaleDB
	redisCache      *redis.Client
	cacheMeta       *cache.CacheMeta
	appParamManager *appparameters.AppParameterManager
}

// PnLSummary represents a summary of P&L across all positions
type PnLSummary struct {
	RealPositionsPNL  float64   `json:"real_position_pnl"`  // Map of trading symbol to P&L
	PaperPositionsPNL float64   `json:"paper_position_pnl"` // Map of trading symbol to paper trading P&L
	UpdatedAt         time.Time `json:"updated_at"`
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
		RealPositionsPNL:  0,
		PaperPositionsPNL: 0,
		UpdatedAt:         time.Now(),
	}
	positionManagerInstance := GetPositionManager()
	cacheMetaInstance, err := cache.GetCacheMetaInstance()
	if err != nil {
		pm.log.Error("Failed to get cache meta instance", map[string]interface{}{
			"error": err.Error(),
		})
	}

	positions, err := positionManagerInstance.ListPositionsFromDB(ctx)
	if err != nil {
		pm.log.Error("Failed to fetch positions from database", map[string]interface{}{
			"error": err.Error(),
		})
	}
	// Process positions from database
	for _, pos := range positions {
		// Calculate P&L using the formula: pnl = (sellValue - buyValue) + (netQuantity * lastPrice * multiplier)
		lastPrice, err := cacheMetaInstance.GetLTPforInstrumentToken(ctx, utils.Uint32ToString(pos.InstrumentToken))
		if err != nil {
			pm.log.Error("Failed to get LTP for instrument token", map[string]interface{}{
				"error": err.Error(),
			})
		}

		netQuantity := pos.SellQuantity - pos.BuyQuantity
		realizedPnL := pos.SellValue - pos.BuyValue
		unrealizedPnL := float64(netQuantity) * lastPrice.LTP * pos.Multiplier
		totalPnL := realizedPnL + unrealizedPnL

		summary.RealPositionsPNL += totalPnL
	}

	paperTradingPositions, err := positionManagerInstance.ListPositionsFromDB(ctx)
	if err != nil {
		pm.log.Error("Failed to fetch positions from database", map[string]interface{}{
			"error": err.Error(),
		})
	}
	// Process positions from database
	for _, pos := range paperTradingPositions {
		// Calculate P&L using the formula: pnl = (sellValue - buyValue) + (netQuantity * lastPrice * multiplier)
		lastPrice, err := cacheMetaInstance.GetLTPforInstrumentToken(ctx, utils.Uint32ToString(pos.InstrumentToken))
		if err != nil {
			pm.log.Error("Failed to get LTP for instrument token", map[string]interface{}{
				"error": err.Error(),
			})
		}

		netQuantity := pos.SellQuantity - pos.BuyQuantity
		realizedPnL := pos.SellValue - pos.BuyValue
		unrealizedPnL := float64(netQuantity) * lastPrice.LTP * pos.Multiplier
		totalPnL := realizedPnL + unrealizedPnL

		summary.PaperPositionsPNL += totalPnL
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
	// Get positions from database
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return fmt.Errorf("timescale DB is nil")
	}

	calculationSummary, err := pm.CalculatePnL(ctx)
	if err != nil {
		return fmt.Errorf("failed to calculate P&L: %w", err)
	}

	exitPnL, _, _ := pm.GetExitPnL(ctx)
	targetPnL, _, _ := pm.GetTargetPnL(ctx)

	if calculationSummary.RealPositionsPNL > exitPnL {
		pm.log.Info("Position P&L exceeded exit P&L", map[string]interface{}{
			"total_pnl": calculationSummary.RealPositionsPNL,
			"exit_pnl":  exitPnL,
		})

	}

	if calculationSummary.RealPositionsPNL > targetPnL {
		pm.log.Info("Position P&L exceeded target P&L", map[string]interface{}{
			"total_pnl":  calculationSummary.RealPositionsPNL,
			"target_pnl": targetPnL,
		})

	}

	realPositionsPNLRecord := &pnl{
		StrategyID: 0,
		TotalPNL:   calculationSummary.RealPositionsPNL,
		CreatedAt:  time.Now(),
	}

	paperPositionsPNLRecord := &pnl{
		StrategyID: 1,
		TotalPNL:   calculationSummary.PaperPositionsPNL,
		CreatedAt:  time.Now(),
	}

	pnls := []*pnl{
		realPositionsPNLRecord,
		paperPositionsPNLRecord,
	}

	if err := storePNLToDB(ctx, pnls); err != nil {
		pm.log.Error("Failed to store P&L", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to store P&L: %w", err)
	}

	return nil
}

func storePNLToDB(ctx context.Context, pnls []*pnl) error {
	if len(pnls) == 0 {
		return nil
	}

	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return fmt.Errorf("failed to get database instance")
	}

	query := `
		INSERT INTO strategy_pnl_timeseries (
			strategy_id, 
			total_pnl,
			created_at
		) VALUES ($1, $2, $3)
	`

	// Use WithTx for automatic transaction management
	err := timescaleDB.WithTx(ctx, func(tx pgx.Tx) error {
		batch := &pgx.Batch{}

		for _, pnl := range pnls {
			if pnl == nil {
				continue
			}
			batch.Queue(query, pnl.StrategyID, pnl.TotalPNL, pnl.CreatedAt)
		}

		if batch.Len() == 0 {
			return nil
		}

		results := tx.SendBatch(ctx, batch)
		defer results.Close()

		// Check for any errors in the batch
		for i := 0; i < batch.Len(); i++ {
			_, err := results.Exec()
			if err != nil {
				return fmt.Errorf("error in batch insert at position %d: %w", i, err)
			}
		}

		return results.Close()
	})

	if err != nil {
		log.Printf("Failed to store batch P&L records: %v", err)
		return fmt.Errorf("failed to store P&L records: %w", err)
	}

	return nil
}
