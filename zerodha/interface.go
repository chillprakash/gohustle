package zerodha

import (
	"context"
	proto "gohustle/proto"
	"time"
)

type TokenOperations interface {
	GetValidToken(ctx context.Context) (string, error)
}

// InstrumentOperations handles instrument data
type InstrumentOperations interface {
	DownloadInstrumentData(ctx context.Context) error
	GetInstrumentExpirySymbolMap(ctx context.Context) (*InstrumentExpiryMap, error)
	CreateLookupMapWithExpiryVSTokenMap(instrumentMap *InstrumentExpiryMap) (map[string]string, map[string]TokenInfo)
	GetUpcomingExpiryTokens(ctx context.Context, instruments []string) ([]string, error)
	GetInstrumentInfo(token string) (TokenInfo, bool)
	GetIndexTokens() map[string]string
}

type ExpiryOperations interface {
	GetIndexVsExpiryMap(ctx context.Context) (map[string][]time.Time, error)
}

// TokenOperations handles token management
type KiteOperations interface {
	GetCurrentSpotPriceOfAllIndices(ctx context.Context) (map[string]float64, error)
}

// KiteOperations combines all operations
type KiteConnector interface {
	TokenOperations
	InstrumentOperations
	KiteOperations
}

type TickWriterOperations interface {
	WriteTickData(ctx context.Context, tick *proto.TickData) error
}

// Ensure KiteConnect implements all interfaces
var (
	_ TokenOperations      = (*KiteConnect)(nil)
	_ InstrumentOperations = (*KiteConnect)(nil)
	_ KiteOperations       = (*KiteConnect)(nil)
)
