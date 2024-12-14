package cache

import (
	"context"
	"fmt"
	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"

	googleproto "google.golang.org/protobuf/proto"
)

const defaultNumLists = 10

type TickDistributor struct {
	numLists   int
	listPrefix string
	redisCache *RedisCache
	log        *logger.Logger
}

func NewTickDistributor() *TickDistributor {
	cfg := config.GetConfig()
	redisCache, _ := NewRedisCache()

	// Use defaults if not configured
	numLists := defaultNumLists
	listPrefix := cfg.Queue.ListPrefix

	if cfg.Queue.PrimaryWorkers > 0 {
		numLists = cfg.Queue.PrimaryWorkers
	}
	if cfg.Queue.ListPrefix != "" {
		listPrefix = cfg.Queue.ListPrefix
	}

	return &TickDistributor{
		numLists:   numLists,
		listPrefix: listPrefix,
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
	data, err := googleproto.Marshal(tick)
	if err != nil {
		return fmt.Errorf("failed to marshal tick: %w", err)
	}

	// Get the appropriate list based on token
	listKey := d.getListKey(tick.InstrumentToken)

	// Push to Redis list
	if err := d.redisCache.GetListDB2().LPush(ctx, listKey, data).Err(); err != nil {
		return fmt.Errorf("failed to push to list %s: %w", listKey, err)
	}

	d.log.Debug("Distributed tick", map[string]interface{}{
		"token":    tick.InstrumentToken,
		"list_key": listKey,
	})

	return nil
}
