package core

import (
	"context"
	"fmt"
	"gohustle/cache"
	"gohustle/logger"
)

// GetTableNameForToken resolves the TimescaleDB table name for a given instrument token using cache lookup
func GetTableNameForToken(ctx context.Context, instrumentToken uint32) string {
	cacheInst := cache.GetInMemoryCacheInstance()
	// Try with the instrument_name_key prefix first
	instrumentNameKey := fmt.Sprintf("instrument_name_key:%d", instrumentToken)
	indexName, _ := cacheInst.Get(instrumentNameKey)

	indexNameStr, ok := indexName.(string)
	if !ok {
		return ""
	}
	switch indexNameStr {
	case "NIFTY":
		return "nifty_ticks"
	case "SENSEX":
		return "sensex_ticks"
	default:
		logger.L().Error("Invalid index name", map[string]interface{}{
			"index_name": indexNameStr,
		})
		return ""
	}
}
