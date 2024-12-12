package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var (
	chInstance *ClickHouseDB
	chOnce     sync.Once
	chMu       sync.RWMutex
)

type ClickHouseDB struct {
	conn   driver.Conn
	config *config.ClickHouseConfig
}

// NewClickHouseDB creates or returns existing ClickHouse instance
func NewClickHouseDB() (*ClickHouseDB, error) {
	chMu.Lock()
	defer chMu.Unlock()

	var initErr error
	chOnce.Do(func() {
		// Get config directly
		cfg := config.GetConfig()
		chInstance = &ClickHouseDB{}
		initErr = chInstance.initialize(&cfg.ClickHouse)
	})

	if initErr != nil {
		return nil, fmt.Errorf("failed to initialize ClickHouse: %w", initErr)
	}

	return chInstance, nil
}

// initialize is internal initialization method
func (db *ClickHouseDB) initialize(cfg *config.ClickHouseConfig) error {
	log := logger.GetLogger()
	ctx := context.Background()

	// Configure ClickHouse connection
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.DBName,
			Username: cfg.User,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:          time.Second * 30,
		MaxOpenConns:         50,
		MaxIdleConns:         5,
		ConnMaxLifetime:      time.Hour,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	})

	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Create table if not exists
	query := `
		CREATE TABLE IF NOT EXISTS ticks (
			instrument_token UInt32,
			timestamp DateTime64(3),
			is_tradable Bool,
			is_index Bool,
			mode LowCardinality(String),
			
			-- Price data
			last_price Float64,
			last_trade_time DateTime64(3),
			last_traded_quantity UInt32,
			average_trade_price Float64,
			
			-- Volume data
			volume_traded UInt32,
			total_buy_quantity UInt32,
			total_sell_quantity UInt32,
			total_buy UInt32,
			total_sell UInt32,
			
			-- OI data
			oi UInt32,
			oi_day_high UInt32,
			oi_day_low UInt32,
			net_change Float64,
			
			-- OHLC
			open_price Float64,
			high_price Float64,
			low_price Float64,
			close_price Float64
		)
		ENGINE = MergeTree()
		PARTITION BY toYYYYMM(timestamp)
		ORDER BY (instrument_token, timestamp)
		SETTINGS index_granularity = 8192
	`

	if err := conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Create materialized views if needed
	if err := db.createMaterializedViews(ctx); err != nil {
		return fmt.Errorf("failed to create materialized views: %w", err)
	}

	db.conn = conn
	db.config = cfg

	log.Info("Connected to ClickHouse and initialized tables", map[string]interface{}{
		"host": cfg.Host,
		"port": cfg.Port,
		"db":   cfg.DBName,
	})

	return nil
}

func (db *ClickHouseDB) createMaterializedViews(ctx context.Context) error {
	// Create 1-minute aggregation view
	minuteViewQuery := `
		CREATE MATERIALIZED VIEW IF NOT EXISTS ticks_1m
		ENGINE = SummingMergeTree()
		PARTITION BY toYYYYMM(timestamp)
		ORDER BY (instrument_token, timestamp)
		AS SELECT
			instrument_token,
			toStartOfMinute(timestamp) as timestamp,
			argMax(last_price, timestamp) as last_price,
			max(high_price) as high_price,
			min(low_price) as low_price,
			argMax(close_price, timestamp) as close_price,
			sum(volume_traded) as volume,
			max(oi) as oi
		FROM ticks
		GROUP BY instrument_token, timestamp
	`

	return db.conn.Exec(ctx, minuteViewQuery)
}

// StoreTick writes a single tick to ClickHouse
func (db *ClickHouseDB) StoreTick(tick *proto.TickData) error {
	ctx := context.Background()

	query := `
		INSERT INTO ticks (
			instrument_token, timestamp, is_tradable, is_index, mode,
			last_price, last_trade_time, last_traded_quantity,
			total_buy_quantity, total_sell_quantity, volume_traded,
			total_buy, total_sell, average_trade_price,
			oi, oi_day_high, oi_day_low, net_change,
			open_price, high_price, low_price, close_price
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	// Use batch insert for better performance
	batch, err := db.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	err = batch.Append(
		tick.InstrumentToken,
		time.Unix(tick.Timestamp, 0),
		tick.IsTradable,
		tick.IsIndex,
		tick.Mode,
		tick.LastPrice,
		time.Unix(tick.LastTradeTime, 0),
		tick.LastTradedQuantity,
		tick.TotalBuyQuantity,
		tick.TotalSellQuantity,
		tick.VolumeTraded,
		tick.TotalBuy,
		tick.TotalSell,
		tick.AverageTradePrice,
		tick.Oi,
		tick.OiDayHigh,
		tick.OiDayLow,
		tick.NetChange,
		tick.Ohlc.Open,
		tick.Ohlc.High,
		tick.Ohlc.Low,
		tick.Ohlc.Close,
	)

	if err != nil {
		return fmt.Errorf("failed to append to batch: %w", err)
	}

	if err := batch.Send(); err != nil {
		logger.GetLogger().Error("Failed to store tick", map[string]interface{}{
			"error": err.Error(),
			"token": tick.InstrumentToken,
			"time":  time.Unix(tick.Timestamp, 0),
		})
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// Close closes the database connection
func (db *ClickHouseDB) Close() {
	if db.conn != nil {
		db.conn.Close()
		logger.GetLogger().Info("ClickHouse connection closed", nil)
	}
}

// TokenRecord represents a token entry
type TokenRecord struct {
	TokenID      uint32    `ch:"token_id"`
	AccessToken  string    `ch:"access_token"`
	RefreshToken string    `ch:"refresh_token"`
	CreatedAt    time.Time `ch:"created_at"`
	ExpiresAt    time.Time `ch:"expires_at"`
	IsValid      bool      `ch:"is_valid"`
	APIKey       string    `ch:"api_key"`
	UserID       string    `ch:"user_id"`
	Metadata     string    `ch:"metadata"`
}

// StoreAccessToken stores a new access token
func (db *ClickHouseDB) StoreAccessToken(ctx context.Context, token *TokenRecord) error {
	query := `
		INSERT INTO access_tokens (
			token_id, access_token, refresh_token, 
			created_at, expires_at, is_valid, 
			api_key, user_id, metadata
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	return db.conn.Exec(ctx, query,
		token.TokenID,
		token.AccessToken,
		token.RefreshToken,
		token.CreatedAt,
		token.ExpiresAt,
		token.IsValid,
		token.APIKey,
		token.UserID,
		token.Metadata,
	)
}

// GetLatestValidToken retrieves the latest valid token
func (db *ClickHouseDB) GetLatestValidToken(ctx context.Context, apiKey string) (*TokenRecord, error) {
	query := `
		SELECT *
		FROM access_tokens
		WHERE api_key = ? AND is_valid = true AND expires_at > now()
		ORDER BY created_at DESC
		LIMIT 1
	`

	var token TokenRecord
	row := db.conn.QueryRow(ctx, query, apiKey)
	err := row.Scan(
		&token.TokenID,
		&token.AccessToken,
		&token.RefreshToken,
		&token.CreatedAt,
		&token.ExpiresAt,
		&token.IsValid,
		&token.APIKey,
		&token.UserID,
		&token.Metadata,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	}

	return &token, nil
}

// InvalidateToken marks a token as invalid
func (db *ClickHouseDB) InvalidateToken(ctx context.Context, tokenID uint32) error {
	query := `
		INSERT INTO access_tokens 
		SELECT 
			token_id,
			access_token,
			refresh_token,
			created_at,
			expires_at,
			false as is_valid,
			api_key,
			user_id,
			metadata
		FROM access_tokens
		WHERE token_id = ?
	`

	return db.conn.Exec(ctx, query, tokenID)
}

// GetValidToken gets the latest valid token or returns error if needs refresh
func (db *ClickHouseDB) GetValidToken(ctx context.Context, apiKey string) (string, error) {
	query := `
		SELECT access_token, expires_at
		FROM access_tokens
		WHERE api_key = ? 
		  AND is_valid = true 
		  AND expires_at > now()
		ORDER BY created_at DESC
		LIMIT 1
	`

	var token string
	var expiresAt time.Time

	row := db.conn.QueryRow(ctx, query, apiKey)
	if err := row.Scan(&token, &expiresAt); err != nil {
		return "", fmt.Errorf("no valid token found: %w", err)
	}

	// Check if token expires soon (within next hour)
	if time.Until(expiresAt) < time.Hour {
		return "", fmt.Errorf("token expires soon")
	}

	return token, nil
}

// StoreNewToken stores a new access token with expiry at 6 AM next day
func (db *ClickHouseDB) StoreNewToken(ctx context.Context, token *TokenRecord) error {
	// Set expiry to 6 AM next day
	tomorrow := time.Now().Add(24 * time.Hour)
	expiryTime := time.Date(
		tomorrow.Year(), tomorrow.Month(), tomorrow.Day(),
		6, 0, 0, 0, tomorrow.Location(),
	)

	token.ExpiresAt = expiryTime
	token.CreatedAt = time.Now()

	return db.StoreAccessToken(ctx, token)
}

// InvalidateAllTokens marks all tokens for an API key as invalid
func (db *ClickHouseDB) InvalidateAllTokens(ctx context.Context, apiKey string) error {
	query := `
		INSERT INTO access_tokens 
		SELECT 
			token_id,
			access_token,
			refresh_token,
			created_at,
			expires_at,
			false as is_valid,
			api_key,
			user_id,
			metadata
		FROM access_tokens
		WHERE api_key = ?
	`

	return db.conn.Exec(ctx, query, apiKey)
}
