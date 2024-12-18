package zerodha

import (
	"context"
	"fmt"
	"gohustle/cache"
	"gohustle/logger"
)

func (z *KiteConnect) GetIndexLTPFromRedis(ctx context.Context, index string) (float64, error) {
	redisCache, err := cache.NewRedisCache()
	log := logger.GetLogger()
	if err != nil {
		return 0, err
	}
	instrumentToken := ""
	if index == "NIFTY" {
		instrumentToken = z.GetIndexTokens()["NIFTY"]
	} else if index == "SENSEX" {
		instrumentToken = z.GetIndexTokens()["SENSEX"]
	}
	key := fmt.Sprintf("ltp:%s", instrumentToken)
	log.Info("Fetching LTP from Redis", map[string]interface{}{
		"key": key,
	})
	ltp, err := redisCache.GetLTPDB3().Get(ctx, key).Float64()
	if err != nil {
		return 0, err
	}
	log.Info("LTP fetched from Redis", map[string]interface{}{
		"index": index,
		"ltp":   ltp,
	})
	return ltp, nil
}
