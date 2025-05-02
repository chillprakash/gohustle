package zerodha

import (
	"context"
	"fmt"
	"time"

	"gohustle/cache"
	"gohustle/db"
	"gohustle/logger"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

// PnLManager handles P&L calculations for positions
type PnLManager struct {
	kite *KiteConnect
	log  *logger.Logger
}

// PnLSummary represents a summary of P&L across all positions
type PnLSummary struct {
	TotalRealizedPnL   float64            `json:"total_realized_pnl"`
	TotalUnrealizedPnL float64            `json:"total_unrealized_pnl"`
	TotalPnL           float64            `json:"total_pnl"`
	PositionPnL        map[string]float64 `json:"position_pnl"`      // Map of trading symbol to P&L
	PaperPositionPnL   map[string]float64 `json:"paper_position_pnl"` // Map of trading symbol to paper trading P&L
	UpdatedAt          time.Time          `json:"updated_at"`
}

// PositionPnL represents P&L for a specific position
type PositionPnL struct {
	TradingSymbol  string  `json:"trading_symbol"`
	Exchange       string  `json:"exchange"`
	Product        string  `json:"product"`
	Quantity       int     `json:"quantity"`
	AveragePrice   float64 `json:"average_price"`
	LastPrice      float64 `json:"last_price"`
	RealizedPnL    float64 `json:"realized_pnl"`
	UnrealizedPnL  float64 `json:"unrealized_pnl"`
	TotalPnL       float64 `json:"total_pnl"`
	BuyValue       float64 `json:"buy_value"`
	SellValue      float64 `json:"sell_value"`
	Multiplier     float64 `json:"multiplier"`
	PaperTrading   bool    `json:"paper_trading"`
	PositionID     string  `json:"position_id"`
}

var (
	pnlInstance *PnLManager
	pnlOnce     sync.Once
)

// GetPnLManager returns a singleton instance of PnLManager
func GetPnLManager() *PnLManager {
	pnlOnce.Do(func() {
		log := logger.L()
		kite := GetKiteConnect()
		if kite == nil {
			log.Error("Failed to get KiteConnect instance", map[string]interface{}{})
			return
		}

		pnlInstance = &PnLManager{
			log:  log,
			kite: kite,
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
		UpdatedAt:        time.Now(),
	}

	// Get Redis cache for LTP data
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis cache: %w", err)
	}
	ltpDB := redisCache.GetLTPDB3()
	inmemoryCache := cache.GetInMemoryCacheInstance()

	// Get positions from database (includes both real and paper trading positions)
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return nil, fmt.Errorf("timescale DB is nil")
	}

	// Fetch all positions from database
	dbPositions, err := timescaleDB.ListPositions(ctx)
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

		// Try to get updated LTP from Redis if available
		if instrumentToken, exists := inmemoryCache.Get(pos.TradingSymbol); exists {
			ltpKey := fmt.Sprintf("%v_ltp", instrumentToken)
			ltpVal, err := ltpDB.Get(ctx, ltpKey).Float64()
			if err == nil && ltpVal > 0 {
				lastPrice = ltpVal
				pm.log.Info("Using updated LTP from Redis for PnL calculation", map[string]interface{}{
					"trading_symbol": pos.TradingSymbol,
					"ltp":           ltpVal,
				})
			}
		}

		// Calculate P&L components
		realizedPnL := pos.SellValue - pos.BuyValue
		unrealizedPnL := float64(pos.Quantity) * lastPrice * pos.Multiplier
		totalPnL := realizedPnL + unrealizedPnL

		// Create position P&L record
		positionPnL := &PositionPnL{
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
			PaperTrading:  pos.PaperTrading,
			PositionID:    pos.PositionID,
		}

		// Update summary
		if pos.PaperTrading {
			summary.PaperPositionPnL[pos.TradingSymbol] = totalPnL
		} else {
			summary.PositionPnL[pos.TradingSymbol] = totalPnL
			summary.TotalRealizedPnL += realizedPnL
			summary.TotalUnrealizedPnL += unrealizedPnL
		}

		// Store updated P&L in database
		if err := pm.updatePositionPnL(ctx, pos.PositionID, realizedPnL, unrealizedPnL, totalPnL, lastPrice); err != nil {
			pm.log.Error("Failed to update position P&L in database", map[string]interface{}{
				"error":          err.Error(),
				"position_id":    pos.PositionID,
				"trading_symbol": pos.TradingSymbol,
			})
		}
	}

	// Also get positions from Zerodha API for real-time data
	positions, err := GetPositionManager().GetOpenPositions(ctx)
	if err != nil {
		pm.log.Error("Failed to get positions from Zerodha for PnL calculation", map[string]interface{}{
			"error": err.Error(),
		})
		// If we have DB positions, we can continue without Zerodha positions
	} else {
		// Process positions from Zerodha API (these should be real-time and most accurate)
		for _, pos := range positions.Net {
			// Skip positions that are already processed from DB to avoid duplicates
			if _, exists := summary.PositionPnL[pos.Tradingsymbol]; exists {
				continue
			}

			// Calculate P&L using the formula: pnl = (sellValue - buyValue) + (netQuantity * lastPrice * multiplier)
			realizedPnL := pos.Realised
			unrealizedPnL := pos.Unrealised
			totalPnL := pos.PnL

			// Update summary
			summary.PositionPnL[pos.Tradingsymbol] = totalPnL
			summary.TotalRealizedPnL += realizedPnL
			summary.TotalUnrealizedPnL += unrealizedPnL

			// Create a position ID for consistency with DB positions
			positionID := fmt.Sprintf("%s_%s_%s", pos.Tradingsymbol, pos.Exchange, pos.Product)

			// Store updated P&L in database
			if err := pm.updatePositionPnL(ctx, positionID, realizedPnL, unrealizedPnL, totalPnL, pos.LastPrice); err != nil {
				pm.log.Error("Failed to update position P&L in database", map[string]interface{}{
					"error":          err.Error(),
					"position_id":    positionID,
					"trading_symbol": pos.Tradingsymbol,
				})
			}
		}
	}

	// Calculate total P&L
	summary.TotalPnL = summary.TotalRealizedPnL + summary.TotalUnrealizedPnL

	return summary, nil
}

// updatePositionPnL updates the P&L values for a position in the database
func (pm *PnLManager) updatePositionPnL(ctx context.Context, positionID string, realizedPnL, unrealizedPnL, totalPnL, lastPrice float64) error {
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
		position_id = $5
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
