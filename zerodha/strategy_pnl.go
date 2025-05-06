package zerodha

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/cache"
	"gohustle/db"
	"gohustle/logger"
)

// StrategyPnLManager handles P&L calculations for strategies
type StrategyPnLManager struct {
	log *logger.Logger
}

var (
	strategyPnLInstance *StrategyPnLManager
	strategyPnLOnce     sync.Once
)

// NewStrategyPnLManager creates a new StrategyPnLManager
func NewStrategyPnLManager() *StrategyPnLManager {
	return &StrategyPnLManager{
		log: logger.L(),
	}
}

// GetStrategyPnLManager returns a singleton instance of StrategyPnLManager
func GetStrategyPnLManager() *StrategyPnLManager {
	strategyPnLOnce.Do(func() {
		log := logger.L()
		strategyPnLInstance = &StrategyPnLManager{
			log: log,
		}
		log.Info("Strategy PnL manager initialized", map[string]interface{}{})
	})
	return strategyPnLInstance
}

// CalculateStrategyPnL calculates P&L at the strategy level
func (pm *StrategyPnLManager) CalculateStrategyPnL(ctx context.Context) error {
	// Get database connection
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return fmt.Errorf("failed to get database connection")
	}

	// Get Redis cache for LTP data
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return fmt.Errorf("failed to get Redis cache: %w", err)
	}
	ltpDB := redisCache.GetLTPDB3()
	inmemoryCache := cache.GetInMemoryCacheInstance()

	// Get all strategies
	strategies, err := timescaleDB.ListStrategies(ctx)
	if err != nil {
		return fmt.Errorf("failed to list strategies: %w", err)
	}

	// Get all positions
	positions, err := timescaleDB.ListPositions(ctx)
	if err != nil {
		return fmt.Errorf("failed to list positions: %w", err)
	}

	// Group positions by strategy and paper trading flag
	strategyPositions := make(map[int]map[bool][]*db.PositionRecord)
	
	// Add a special entry for discretionary trading (strategy_id = 0)
	strategyPositions[0] = make(map[bool][]*db.PositionRecord)
	strategyPositions[0][true] = []*db.PositionRecord{}  // Paper discretionary trading
	strategyPositions[0][false] = []*db.PositionRecord{} // Real discretionary trading
	
	// Add entries for all defined strategies
	for _, strategy := range strategies {
		strategyPositions[strategy.ID] = make(map[bool][]*db.PositionRecord)
		strategyPositions[strategy.ID][true] = []*db.PositionRecord{}  // Paper trading
		strategyPositions[strategy.ID][false] = []*db.PositionRecord{} // Real trading
	}

	// Add positions to their respective strategies
	for _, pos := range positions {
		var strategyID int
		if pos.StrategyID == nil {
			// Handle as discretionary trading
			strategyID = 0
		} else {
			strategyID = *pos.StrategyID
		}

		if _, exists := strategyPositions[strategyID]; !exists {
			strategyPositions[strategyID] = make(map[bool][]*db.PositionRecord)
			strategyPositions[strategyID][true] = []*db.PositionRecord{}
			strategyPositions[strategyID][false] = []*db.PositionRecord{}
		}

		strategyPositions[strategyID][pos.PaperTrading] = append(
			strategyPositions[strategyID][pos.PaperTrading],
			pos,
		)
	}

	// Calculate P&L for each strategy and trading type
	var pnlRecords []db.StrategyPnLTimeseriesRecord
	now := time.Now()

	for strategyID, tradingTypes := range strategyPositions {
		// Get strategy name
		var strategyName string
		if strategyID == 0 {
			// Special name for discretionary trading
			strategyName = "Discretionary"
		} else {
			// Look up strategy name from the strategies list
			for _, s := range strategies {
				if s.ID == strategyID {
					strategyName = s.Name
					break
				}
			}
			
			// If we couldn't find the strategy name, use a default
			if strategyName == "" {
				strategyName = fmt.Sprintf("Strategy-%d", strategyID)
			}
		}

		// Calculate for both paper and real trading
		for paperTrading, strategyPositions := range tradingTypes {
			var totalPnL float64

			// Calculate total P&L for all positions in this strategy
			for _, pos := range strategyPositions {
				// Get latest price
				lastPrice := pos.LastPrice
				if instrumentToken, exists := inmemoryCache.Get(pos.TradingSymbol); exists {
					ltpKey := fmt.Sprintf("%v_ltp", instrumentToken)
					ltpVal, err := ltpDB.Get(ctx, ltpKey).Float64()
					if err == nil && ltpVal > 0 {
						lastPrice = ltpVal
					}
				}

				// Calculate P&L using the formula: (sellValue - buyValue) + (netQuantity * lastPrice * multiplier)
				positionTotalPnL := (pos.SellValue - pos.BuyValue) + (float64(pos.Quantity) * lastPrice * pos.Multiplier)

				// Add to strategy total
				totalPnL += positionTotalPnL
			}

			// Create record
			record := db.StrategyPnLTimeseriesRecord{
				StrategyID:   strategyID,
				StrategyName: strategyName,
				TotalPnL:     totalPnL,
				PaperTrading: paperTrading,
				Timestamp:    now,
			}

			pnlRecords = append(pnlRecords, record)
		}
	}

	// Batch insert all records
	if len(pnlRecords) > 0 {
		if err := timescaleDB.BatchInsertStrategyPnLTimeseries(ctx, pnlRecords); err != nil {
			return fmt.Errorf("failed to insert strategy P&L records: %w", err)
		}
	}

	return nil
}

// The tracking is managed by the scheduler, so we don't need these methods
// The scheduler will call CalculateStrategyPnL directly
