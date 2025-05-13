package zerodha

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	sync "sync"
	"time"

	"gohustle/cache"
	"gohustle/logger"

	"github.com/zerodha/gokiteconnect/v4/models"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"
)

// ConnectTicker establishes connections to Kite's ticker service
func (k *KiteConnect) ConnectTicker() error {
	log := logger.L()

	if len(k.tokens) == 0 {
		return fmt.Errorf("no tokens provided for subscription")
	}

	// Calculate tokens per connection
	tokensPerConnection := (len(k.tokens) + MaxConnections - 1) / MaxConnections

	log.Info("Starting ticker connections", map[string]interface{}{
		"total_tokens":          len(k.tokens),
		"max_connections":       MaxConnections,
		"tokens_per_connection": tokensPerConnection,
	})

	// Create and connect tickers
	for i := 0; i < MaxConnections; i++ {
		// Calculate token range for this connection
		startIdx := i * tokensPerConnection
		if startIdx >= len(k.tokens) {
			break
		}

		endIdx := startIdx + tokensPerConnection
		if endIdx > len(k.tokens) {
			endIdx = len(k.tokens)
		}

		connectionTokens := k.tokens[startIdx:endIdx]

		log.Info("Creating ticker connection", map[string]interface{}{
			"connection":   i + 1,
			"tokens_count": len(connectionTokens),
			"start_token":  connectionTokens[0],
			"end_token":    connectionTokens[len(connectionTokens)-1],
		})

		if k.Tickers[i] == nil {
			log.Error("Ticker not initialized", map[string]interface{}{
				"connection": i + 1,
			})
			return fmt.Errorf("ticker not initialized for connection %d", i+1)
		}

		ticker := k.Tickers[i]

		// Set callbacks
		ticker.OnConnect(func(connectionID int, tokens []uint32) func() {
			return func() {
				log.Info("Ticker connected, subscribing tokens", map[string]interface{}{
					"connection":   connectionID + 1,
					"tokens_count": len(tokens),
				})

				// Subscribe tokens
				if err := ticker.Subscribe(tokens); err != nil {
					log.Error("Failed to subscribe tokens", map[string]interface{}{
						"connection": connectionID + 1,
						"error":      err.Error(),
					})
					return
				}

				// Set mode to full
				if err := ticker.SetMode(kiteticker.ModeFull, tokens); err != nil {
					log.Error("Failed to set mode", map[string]interface{}{
						"connection": connectionID + 1,
						"error":      err.Error(),
					})
					return
				}

				log.Info("Successfully subscribed tokens", map[string]interface{}{
					"connection":   connectionID + 1,
					"tokens_count": len(tokens),
				})
			}
		}(i, connectionTokens))

		// Set other callbacks
		ticker.OnError(k.onError)
		ticker.OnClose(k.onClose)
		ticker.OnReconnect(k.onReconnect)
		ticker.OnNoReconnect(k.onNoReconnect)
		ticker.OnTick(k.handleTick)

		// Connect in goroutine
		go func(connectionID int, t *kiteticker.Ticker) {
			log.Info("Starting ticker service", map[string]interface{}{
				"connection": connectionID + 1,
			})

			// Serve the ticker
			t.Serve()

			log.Info("Ticker service started", map[string]interface{}{
				"connection": connectionID + 1,
			})
		}(i, ticker)
	}

	return nil
}

func (k *KiteConnect) CloseTicker() error {
	log := logger.L()

	for i, ticker := range k.Tickers {
		if ticker != nil {
			ticker.Close()
			log.Info("Ticker closed", map[string]interface{}{
				"connection": i + 1,
			})
		}
	}

	return nil
}

// Internal handlers
// tokenToIndexCache is an in-memory cache to avoid Redis lookups for every tick
var tokenToIndexCache sync.Map

func (k *KiteConnect) handleTick(tick models.Tick) {
	log := logger.L()
	// cacheMeta, _ := cache.GetCacheMetaInstance()

	// Parallelize Redis storage
	go func(t models.Tick) {
		if err := k.StoreTickInRedis(&t); err != nil {
			log.Error("Failed to store tick in Redis", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}(tick)

	// // Parallelize NATS publishing
	// go func(t models.Tick) {
	// 	natsProducer := nats.GetTickProducer()
	// 	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond) // Increased timeout
	// 	defer cancel()

	// 	// Try to get index name from in-memory cache first
	// 	tokenStr := fmt.Sprintf("%d", t.InstrumentToken)
	// 	indexName, found := tokenToIndexCache.Load(tokenStr)
	// 	if !found {
	// 		// Not in cache, try to get from Redis
	// 		instrumentSymbol, err := cacheMeta.GetSymbolByToken(ctx, tokenStr)
	// 		if err != nil {
	// 			log.Error("Failed to get symbol by token", map[string]interface{}{
	// 				"error": err.Error(),
	// 				"token": t.InstrumentToken,
	// 			})
	// 			return
	// 		}

	// 		// Determine index name from symbol
	// 		indexNameStr, err := cacheMeta.GetIndexNameFromSymbol(ctx, instrumentSymbol)
	// 		if err != nil || indexNameStr == "" {
	// 			log.Error("Failed to get index name from symbol", map[string]interface{}{
	// 				"error":  err,
	// 				"token":  t.InstrumentToken,
	// 				"symbol": instrumentSymbol,
	// 			})
	// 			return
	// 		}

	// 		// Store in cache for future lookups
	// 		tokenToIndexCache.Store(tokenStr, indexNameStr)
	// 		indexName = indexNameStr
	// 	}

	// 	protoTick := &pb.TickData{
	// 		InstrumentToken:       t.InstrumentToken,
	// 		ExchangeUnixTimestamp: t.Timestamp.Unix(),
	// 		LastPrice:             t.LastPrice,
	// 		VolumeTraded:          t.VolumeTraded,
	// 		AverageTradePrice:     t.AverageTradePrice,
	// 		OpenInterest:          t.OI,
	// 	}

	// 	// Use the index name from cache or Redis lookup
	// 	indexNameStr := indexName.(string)
	// 	subject := fmt.Sprintf("ticks.%s", indexNameStr)

	// 	if err := natsProducer.PublishTick(ctx, subject, protoTick); err != nil {
	// 		log.Error("Failed to publish tick", map[string]interface{}{
	// 			"error":      err.Error(),
	// 			"token":      t.InstrumentToken,
	// 			"index":      indexNameStr,
	// 			"last_price": t.LastPrice,
	// 			"oi":         t.OI,
	// 			"volume":     t.VolumeTraded,
	// 			"avg_price":  t.AverageTradePrice,
	// 			"timestamp":  t.Timestamp,
	// 		})
	// 	}
	// }(tick)
}

func (k *KiteConnect) onError(err error) {
	log := logger.L()

	log.Error("Ticker error", map[string]interface{}{
		"error": err.Error(),
	})
}

func (k *KiteConnect) onClose(code int, reason string) {
	log := logger.L()

	log.Info("Ticker closed", map[string]interface{}{
		"code":   code,
		"reason": reason,
	})
}

func (k *KiteConnect) onReconnect(attempt int, delay time.Duration) {
	log := logger.L()

	log.Info("Ticker reconnecting", map[string]interface{}{
		"attempt": attempt,
		"delay":   delay.Seconds(),
	})
}

func (k *KiteConnect) onNoReconnect(attempt int) {
	log := logger.L()

	log.Error("Ticker max reconnect attempts reached", map[string]interface{}{
		"attempts": attempt,
	})
}

// StoreTickInRedis handles storing tick data in Redis
func (k *KiteConnect) StoreTickInRedis(tick *models.Tick) error {
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return fmt.Errorf("failed to get Redis cache: %w", err)
	}

	ltpDB := redisCache.GetLTPDB3()
	if ltpDB == nil {
		return fmt.Errorf("LTP Redis DB is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	instrumentToken := fmt.Sprintf("%d", tick.InstrumentToken)

	requiredKeys := map[string]interface{}{
		fmt.Sprintf("%s_ltp", instrumentToken):                  tick.LastPrice,
		fmt.Sprintf("%s_volume", instrumentToken):               tick.VolumeTraded,
		fmt.Sprintf("%s_oi", instrumentToken):                   tick.OI,
		fmt.Sprintf("%s_average_traded_price", instrumentToken): tick.AverageTradePrice,
	}

	redisData := make(map[string]interface{})
	for key, value := range requiredKeys {
		if !isZeroValue(value) {
			redisData[key] = value
		}
	}

	if len(redisData) > 0 {
		pipe := ltpDB.Pipeline()
		for key, value := range redisData {
			strValue := convertToString(value)
			if strValue != "" {
				pipe.Set(ctx, key, strValue, 12*time.Hour)
			}
		}

		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("failed to execute Redis pipeline for token %s: %w", instrumentToken, err)
		}
	}

	return nil
}

func isZeroValue(v interface{}) bool {
	switch v := v.(type) {
	case int32:
		return v == 0
	case int64:
		return v == 0
	case float64:
		return v == 0
	case string:
		return v == ""
	default:
		return v == nil
	}
}

func convertToString(v interface{}) string {
	switch v := v.(type) {
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case uint32:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%f", v)
	case string:
		return v
	default:
		return ""
	}
}

// Helper function to convert string tokens to uint32
func convertTokensToUint32(tokens []string) ([]uint32, error) {
	result := make([]uint32, 0, len(tokens))

	for _, token := range tokens {
		// Parse string to uint64 first
		val, err := strconv.ParseUint(token, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid token %s: %w", token, err)
		}
		// Convert to uint32
		result = append(result, uint32(val))
	}

	return result, nil
}

func extractStrikeAndType(tradingSymbol string, strike string) string {
	// Split by decimal point and take only the whole number part
	cleanStrike := strings.Split(strike, ".")[0]

	// Find the position of strike in trading symbol
	strikePos := strings.Index(tradingSymbol, cleanStrike)
	if strikePos == -1 {
		return "" // Strike not found in trading symbol
	}

	// Extract strike with option type (last 2 characters are CE or PE)
	result := tradingSymbol[strikePos:]
	if len(result) >= 2 {
		return result
	}

	return ""
}
