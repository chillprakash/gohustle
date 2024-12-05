package zerodha

import (
	"context"
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

// KiteOperations combines all operations
type KiteConnector interface {	
	TokenOperations
	InstrumentOperations
	KiteOperations
}

// Ensure KiteConnect implements all interfaces
var (
	_ TokenOperations      = (*KiteConnect)(nil)
	_ InstrumentOperations = (*KiteConnect)(nil)
	_ KiteOperations       = (*KiteConnect)(nil)
)
