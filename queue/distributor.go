package queue

import (
	"context"
	"fmt"
	"gohustle/cache"
	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"

	"google.golang.org/protobuf/proto"
)

const defaultListPrefix = "tick_queue:"

type TickDistributor struct {
	numLists   int
	listPrefix string
	redisCache *cache.RedisCache
	log        *logger.Logger
}

func NewTickDistributor(cfg *config.Config) *TickDistributor {
	redisCache, _ := cache.NewRedisCache()

	return &TickDistributor{
		numLists:   cfg.Queue.NumLists,
		listPrefix: cfg.Queue.ListPrefix,
		redisCache: redisCache,
		log:        logger.GetLogger(),
	}
}

func (d *TickDistributor) getListKey(token uint32) string {
	listNum := token % uint32(d.numLists)
	return fmt.Sprintf("%s%d", d.listPrefix, listNum)
}

func (d *TickDistributor) DistributeTick(ctx context.Context, tick *proto.TickData) error {
	// Serialize the tick data
	data, err := proto.Marshal(tick)
	if err != nil {
		return fmt.Errorf("failed to marshal tick: %w", err)
	}

	// Get the appropriate list based on token
	listKey := d.getListKey(tick.InstrumentToken)

	// Push to Redis list
	if err := d.redisCache.GetSummaryDB5().LPush(ctx, listKey, data).Err(); err != nil {
		return fmt.Errorf("failed to push to list %s: %w", listKey, err)
	}

	d.log.Debug("Distributed tick", map[string]interface{}{
		"token":    tick.InstrumentToken,
		"list_key": listKey,
	})

	return nil
}
