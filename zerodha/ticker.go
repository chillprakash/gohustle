package zerodha

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"gohustle/cache"
	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/redis/go-redis/v9"
	"github.com/zerodha/gokiteconnect/v4/models"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"
	googleproto "google.golang.org/protobuf/proto"
)

func (k *KiteConnect) ConnectTicker() error {
	log := logger.GetLogger()
	cfg := config.GetConfig()

	// Get valid token first
	token, err := k.GetValidToken(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get valid token: %w", err)
	}

	// Channel to track connection status
	connectedChan := make(chan bool, len(k.Tickers))
	errorChan := make(chan error, len(k.Tickers))

	// Initialize tickers with token
	for i := range k.Tickers {
		// Create new ticker instance
		k.Tickers[i] = kiteticker.New(cfg.Kite.APIKey, token)
		if k.Tickers[i] == nil {
			log.Error("Ticker not initialized", map[string]interface{}{
				"connection": i + 1,
			})
			return fmt.Errorf("ticker not initialized")
		}

		// Create closure to capture connection ID
		connectionID := i

		// Set up callbacks for each ticker
		k.Tickers[i].OnError(k.onError)
		k.Tickers[i].OnClose(k.onClose)
		k.Tickers[i].OnConnect(func() {
			log.Info("Ticker connected", map[string]interface{}{
				"connection": connectionID + 1,
			})
			connectedChan <- true
		})
		k.Tickers[i].OnReconnect(k.onReconnect)
		k.Tickers[i].OnNoReconnect(k.onNoReconnect)
		k.Tickers[i].OnTick(func(tick models.Tick) {
			k.handleTick(tick, connectionID)
		})

		log.Info("Setting up ticker callbacks", map[string]interface{}{
			"connection": i + 1,
			"api_key":    cfg.Kite.APIKey,
			"token":      token,
		})

		// Start each connection in a goroutine
		go func(ticker *kiteticker.Ticker, connID int) {
			log.Info("Starting ticker connection", map[string]interface{}{
				"connection": connID + 1,
			})
			ticker.Serve()
		}(k.Tickers[i], i)
	}

	// Wait for all tickers to connect or timeout
	timeout := time.After(10 * time.Second)
	connectedCount := 0
	expectedConnections := len(k.Tickers)

	for connectedCount < expectedConnections {
		select {
		case <-connectedChan:
			connectedCount++
			log.Info("Ticker connection established", map[string]interface{}{
				"connected": connectedCount,
				"total":     expectedConnections,
				"remaining": expectedConnections - connectedCount,
			})

		case err := <-errorChan:
			log.Error("Ticker connection failed", map[string]interface{}{
				"error": err.Error(),
			})
			return err

		case <-timeout:
			return fmt.Errorf("timeout waiting for ticker connections, only %d/%d connected",
				connectedCount, expectedConnections)
		}
	}

	log.Info("All tickers connected successfully", map[string]interface{}{
		"connections": len(k.Tickers),
	})

	return nil
}

func (k *KiteConnect) Subscribe(tokens []string) error {
	log := logger.GetLogger()

	// Convert all tokens to uint32
	uint32Tokens := make([]uint32, 0, len(tokens))
	for _, token := range tokens {
		if tokenInt, err := strconv.ParseUint(token, 10, 32); err == nil {
			uint32Tokens = append(uint32Tokens, uint32(tokenInt))
		}
	}

	// Distribute tokens across connections
	tokensPerConnection := (len(uint32Tokens) + len(k.Tickers) - 1) / len(k.Tickers)
	if tokensPerConnection > MaxTokensPerConnection {
		log.Error("Too many tokens to subscribe", map[string]interface{}{
			"tokens":       len(uint32Tokens),
			"max_per_conn": MaxTokensPerConnection,
			"connections":  len(k.Tickers),
		})
		return errors.New("too many tokens to subscribe")
	}

	for i, ticker := range k.Tickers {
		start := i * tokensPerConnection
		if start >= len(uint32Tokens) {
			break
		}

		end := start + tokensPerConnection
		if end > len(uint32Tokens) {
			end = len(uint32Tokens)
		}

		connectionTokens := uint32Tokens[start:end]

		log.Info("Subscribing tokens to connection", map[string]interface{}{
			"connection":   i + 1,
			"tokens_count": len(connectionTokens),
		})

		if err := ticker.Subscribe(connectionTokens); err != nil {
			log.Error("Failed to subscribe on connection", map[string]interface{}{
				"connection": i + 1,
				"error":      err.Error(),
			})
			return err
		}

		if err := ticker.SetMode(kiteticker.ModeFull, connectionTokens); err != nil {
			log.Error("Failed to set mode on connection", map[string]interface{}{
				"connection": i + 1,
				"error":      err.Error(),
			})
			return err
		}
	}

	log.Info("Successfully subscribed all tokens", map[string]interface{}{
		"total_tokens": len(uint32Tokens),
		"connections":  len(k.Tickers),
	})

	return nil
}

func (k *KiteConnect) CloseTicker() error {
	log := logger.GetLogger()

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
func (k *KiteConnect) handleTick(tick models.Tick, connectionID int) {
	log := logger.GetLogger()

	// Get instrument info
	tokenStr := fmt.Sprintf("%d", tick.InstrumentToken)
	instrumentInfo, exists := k.GetInstrumentInfo(tokenStr)
	if !exists {
		return
	}

	// Convert to protobuf and marshal
	protoTick := &proto.TickData{
		// Basic info
		InstrumentToken: tick.InstrumentToken,
		IsTradable:      tick.IsTradable,
		IsIndex:         tick.IsIndex,
		Mode:            tick.Mode,

		// Timestamps
		Timestamp:     tick.Timestamp.Unix(),
		LastTradeTime: tick.LastTradeTime.Unix(),

		// Price and quantity
		LastPrice:          tick.LastPrice,
		LastTradedQuantity: tick.LastTradedQuantity,
		TotalBuyQuantity:   tick.TotalBuyQuantity,
		TotalSellQuantity:  tick.TotalSellQuantity,
		VolumeTraded:       tick.VolumeTraded,
		TotalBuy:           uint32(tick.TotalBuy),
		TotalSell:          uint32(tick.TotalSell),
		AverageTradePrice:  tick.AverageTradePrice,

		// OI related
		Oi:        tick.OI,
		OiDayHigh: tick.OIDayHigh,
		OiDayLow:  tick.OIDayLow,
		NetChange: tick.NetChange,

		// OHLC data
		Ohlc: &proto.TickData_OHLC{
			Open:  tick.OHLC.Open,
			High:  tick.OHLC.High,
			Low:   tick.OHLC.Low,
			Close: tick.OHLC.Close,
		},

		// Market depth
		Depth: &proto.TickData_MarketDepth{
			Buy:  convertDepthItems(tick.Depth.Buy[:]),
			Sell: convertDepthItems(tick.Depth.Sell[:]),
		},

		// Additional metadata
		ChangePercent:       tick.NetChange,
		LastTradePrice:      tick.LastPrice,
		OpenInterest:        tick.OI,
		OpenInterestDayHigh: tick.OIDayHigh,
		OpenInterestDayLow:  tick.OIDayLow,
	}
	data, err := googleproto.Marshal(protoTick)
	if err != nil {
		log.Error("Failed to marshal tick", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Get Redis cache instance
	redisCache, err := cache.NewRedisCache()
	if err != nil {
		log.Error("Failed to get Redis cache", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	ctx := context.Background()
	dataKey := fmt.Sprintf("tick:%d:%d", tick.InstrumentToken, tick.Timestamp.Unix())

	// 1. Store actual data in appropriate DB based on instrument type
	var dataDB *redis.Client
	if instrumentInfo.IsIndex {
		dataDB = redisCache.GetIndexSpotDB1()
	} else if instrumentInfo.Index == "NIFTY" {
		dataDB = redisCache.GetNiftyOptionsDB2()
	} else {
		dataDB = redisCache.GetSensexOptionsDB3()
	}

	// Store data in appropriate DB
	dataPipe := dataDB.Pipeline()
	dataPipe.Set(ctx, dataKey, data, 24*time.Hour)
	if _, err := dataPipe.Exec(ctx); err != nil {
		log.Error("Failed to store tick data", map[string]interface{}{
			"error": err.Error(),
			"db":    dataDB,
		})
		return
	}

	// 2. Store indices in summary DB
	summaryPipe := redisCache.GetSummaryDB5().Pipeline()

	// Add to index-level sorted set
	if instrumentInfo.IsIndex {
		indexSetKey := fmt.Sprintf("index:%s:ticks", instrumentInfo.Index)
		summaryPipe.ZAdd(ctx, indexSetKey, redis.Z{
			Score:  float64(tick.Timestamp.Unix()),
			Member: dataKey,
		})
	}

	// Add to instrument-specific sorted set
	instrumentSetKey := fmt.Sprintf("index:%s:%d", instrumentInfo.Index, tick.InstrumentToken)
	summaryPipe.ZAdd(ctx, instrumentSetKey, redis.Z{
		Score:  float64(tick.Timestamp.Unix()),
		Member: dataKey,
	})

	// Execute summary pipeline
	if _, err := summaryPipe.Exec(ctx); err != nil {
		log.Error("Failed to store tick indices", map[string]interface{}{
			"error": err.Error(),
			"db":    "summary",
		})
		return
	}

	log.Info("Stored tick", map[string]interface{}{
		"token":          tick.InstrumentToken,
		"index":          instrumentInfo.Index,
		"is_index":       instrumentInfo.IsIndex,
		"timestamp":      tick.Timestamp.Format("2006-01-02 15:04:05.000"),
		"data_key":       dataKey,
		"data_db":        getDBName(dataDB),
		"instrument_set": instrumentSetKey,
	})
}

func (k *KiteConnect) onError(err error) {
	log := logger.GetLogger()

	log.Error("Ticker error", map[string]interface{}{
		"error": err.Error(),
	})
}

func (k *KiteConnect) onClose(code int, reason string) {
	log := logger.GetLogger()

	log.Info("Ticker closed", map[string]interface{}{
		"code":   code,
		"reason": reason,
	})
}

func (k *KiteConnect) onReconnect(attempt int, delay time.Duration) {
	log := logger.GetLogger()

	log.Info("Ticker reconnecting", map[string]interface{}{
		"attempt": attempt,
		"delay":   delay.Seconds(),
	})
}

func (k *KiteConnect) onNoReconnect(attempt int) {
	log := logger.GetLogger()

	log.Error("Ticker max reconnect attempts reached", map[string]interface{}{
		"attempts": attempt,
	})
}

// Helper function to convert depth items
func convertDepthItems(items []models.DepthItem) []*proto.TickData_DepthItem {
	result := make([]*proto.TickData_DepthItem, len(items))
	for i, item := range items {
		result[i] = &proto.TickData_DepthItem{
			Price:    item.Price,
			Quantity: uint32(item.Quantity),
			Orders:   uint32(item.Orders),
		}
	}
	return result
}

// Helper function to get DB name for logging
func getDBName(client *redis.Client) string {
	switch client.Options().DB {
	case cache.IndexSpotDB:
		return "index_spot_db"
	case cache.NiftyOptionsDB:
		return "nifty_options_db"
	case cache.SensexOptionsDB:
		return "sensex_options_db"
	case cache.SummaryDB:
		return "summary_db"
	default:
		return fmt.Sprintf("db_%d", client.Options().DB)
	}
}
