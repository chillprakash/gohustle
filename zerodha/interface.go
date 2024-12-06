package zerodha

import (
	"context"

	"github.com/zerodha/gokiteconnect/v4/models"
)

// TokenOperations handles token management
type TokenOperations interface {
	GetValidToken(ctx context.Context) (string, error)
}

// TokenOperations handles token management
type KiteOperations interface {
	GetCurrentSpotPriceOfAllIndices(ctx context.Context) (map[string]float64, error)
}

// InstrumentOperations handles instrument data
type InstrumentOperations interface {
	DownloadInstrumentData(ctx context.Context) error
	SyncInstrumentExpiriesFromFileToDB(ctx context.Context) error
	GetInstrumentExpirySymbolMap(ctx context.Context) (*InstrumentExpiryMap, error)
	CreateLookupMapWithExpiryVSTokenMap(instrumentMap *InstrumentExpiryMap) (map[string]string, map[string]TokenInfo)
	GetUpcomingExpiryTokens(ctx context.Context, instruments []string) ([]string, error)
}

// TickerOperations handles WebSocket ticker operations
type TickerOperations interface {
	// ConnectTicker(accessToken string) error
	// Subscribe(tokens []string) error
	// OnTick(handler func(tick kitemodels.Tick))
	// Close() error
	StartProcessing(ctx context.Context) error
}

// ZerodhaRedisInterface defines Zerodha-specific Redis operations
type ZerodhaRedisInterface interface {
	// Tick Stream operations
	PublishTickData(ctx context.Context, instrumentToken uint32, tick *models.Tick) error
	ConsumeTickData(ctx context.Context, instrumentToken uint32, handler func([]byte)) error
	ConsumeMultipleTickData(ctx context.Context, tokens []uint32, handler func(uint32, []byte)) error
}

// KiteOperations combines all operations
type KiteConnector interface {
	TokenOperations
	InstrumentOperations
	KiteOperations
	TickerOperations
	ZerodhaRedisInterface
}

// Ensure KiteConnect implements all interfaces
var (
	_ TokenOperations       = (*KiteConnect)(nil)
	_ InstrumentOperations  = (*KiteConnect)(nil)
	_ KiteOperations        = (*KiteConnect)(nil)
	_ TickerOperations      = (*KiteConnect)(nil)
	_ ZerodhaRedisInterface = (*KiteConnect)(nil)
)
