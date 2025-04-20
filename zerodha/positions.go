package zerodha

import (
	"context"
	"encoding/json"
	"fmt"
	sync "sync"

	"gohustle/cache"
	"gohustle/logger"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

// PositionManager handles all position-related operations
type PositionManager struct {
	kite *KiteConnect
	log  *logger.Logger
}

var (
	positionInstance *PositionManager
	positionOnce     sync.Once
)

// GetPositionManager returns a singleton instance of PositionManager
func GetPositionManager() *PositionManager {
	positionOnce.Do(func() {
		positionInstance = &PositionManager{
			log: logger.L(),
		}
	})
	return positionInstance
}

// GetOpenPositions fetches current positions from Zerodha
func (pm *PositionManager) GetOpenPositions(ctx context.Context) (kiteconnect.Positions, error) {
	var positions kiteconnect.Positions

	positions, err := pm.kite.Kite.GetPositions()
	if err != nil {
		return kiteconnect.Positions{}, fmt.Errorf("failed to fetch positions: %w", err)
	}
	return positions, nil
}

// PollPositionsAndUpdateInRedis periodically polls positions and updates Redis
func (pm *PositionManager) PollPositionsAndUpdateInRedis(ctx context.Context) error {
	positions, err := pm.GetOpenPositions(ctx)
	if err != nil {
		return fmt.Errorf("failed to get positions: %w", err)
	}

	// Store day positions
	if err := pm.storePositionsInRedis(ctx, "day", positions.Day); err != nil {
		return err
	}

	// Store net positions
	if err := pm.storePositionsInRedis(ctx, "net", positions.Net); err != nil {
		return err
	}

	return nil
}

// storePositionsInRedis stores a list of positions in Redis
func (pm *PositionManager) storePositionsInRedis(ctx context.Context, category string, positions []kiteconnect.Position) error {
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return fmt.Errorf("failed to get Redis cache: %w", err)
	}

	positionsRedis := redisCache.GetPositionsDB2()

	for _, pos := range positions {
		key := fmt.Sprintf("position:%s:%s:%s", category, pos.Tradingsymbol, pos.Product)

		// Convert position to JSON
		posJSON, err := json.Marshal(pos)
		if err != nil {
			pm.log.Error("Failed to marshal position", map[string]interface{}{
				"symbol": pos.Tradingsymbol,
				"error":  err.Error(),
			})
			continue
		}

		// Store in Redis with 24-hour expiry
		err = positionsRedis.HSet(ctx, "positions", key, string(posJSON)).Err()
		if err != nil {
			pm.log.Error("Failed to store position in Redis", map[string]interface{}{
				"symbol": pos.Tradingsymbol,
				"error":  err.Error(),
			})
		}
	}
	return nil
}
