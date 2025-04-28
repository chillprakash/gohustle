package core

import (
	"context"
	"fmt"
	"gohustle/cache"
)

// GetTableNameForToken resolves the TimescaleDB table name for a given instrument token using cache lookup
func GetTableNameForToken(ctx context.Context, instrumentToken uint32) string {
	cacheInst := cache.GetInMemoryCacheInstance()
	indexName, exists := cacheInst.Get(fmt.Sprintf("%d", instrumentToken))
	if !exists {
		return "nifty_ticks"
	}
	indexNameStr, ok := indexName.(string)
	if !ok {
		return "nifty_ticks"
	}
	switch indexNameStr {
	case "NIFTY":
		return "nifty_ticks"
	case "SENSEX":
		return "sensex_ticks"
	case "BANKNIFTY":
		return "banknifty_ticks"
	case "FINNIFTY":
		return "finnifty_ticks"
	case "MIDCPNIFTY":
		return "midcpnifty_ticks"
	default:
		return "nifty_ticks"
	}
}
