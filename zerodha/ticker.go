package zerodha

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"gohustle/cache"
	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/zerodha/gokiteconnect/v4/models"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"
)

func (k *KiteConnect) ConnectTickers() error {
	log := logger.GetLogger()
	cfg := config.GetConfig()

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

		// Create new ticker
		ticker := kiteticker.New(cfg.Kite.APIKey, k.accessToken)
		k.Tickers[i] = ticker

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

		// Set other callbacks...
		ticker.OnError(k.onError)
		ticker.OnClose(k.onClose)
		ticker.OnReconnect(k.onReconnect)
		ticker.OnNoReconnect(k.onNoReconnect)
		ticker.OnTick(k.handleTick)
		// ticker.OnOrderUpdate(k.handleOrder)

		// Connect in goroutine
		go func(connectionID int) {
			log.Info("Starting ticker service", map[string]interface{}{
				"connection": connectionID + 1,
			})

			// Serve the ticker
			ticker.Serve()

			log.Info("Ticker service started", map[string]interface{}{
				"connection": connectionID + 1,
			})
		}(i)
	}

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
func (k *KiteConnect) handleTick(tick models.Tick) {
	log := logger.GetLogger()
	distributor := cache.NewTickDistributor()
	redisCache, _ := cache.NewRedisCache()

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
		Timestamp:        tick.Timestamp.Unix(),
		LastTradeTime:    tick.LastTradeTime.Unix(),
		TickRecievedTime: time.Now().Unix(),

		// Additional metadata

		TargetFile: instrumentInfo.TargetFile,

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
	}

	// Distribute the tick
	if err := distributor.DistributeTick(context.Background(), protoTick); err != nil {
		log.Error("Failed to distribute tick", map[string]interface{}{
			"error": err.Error(),
			"token": tick.InstrumentToken,
		})
		return
	}

	// 3. Store LTP data in LTP DB
	dataKey := fmt.Sprintf("ltp:%d", tick.InstrumentToken)
	ltpDB := redisCache.GetListDB2()
	ctx := context.Background()
	ltpDB.Set(ctx, dataKey, tick.LastPrice, 24*time.Hour)

	log.Info("Stored tick", map[string]interface{}{
		"token":     tick.InstrumentToken,
		"index":     instrumentInfo.Index,
		"is_index":  instrumentInfo.IsIndex,
		"timestamp": tick.Timestamp.Format("2006-01-02 15:04:05.000"),
		"data_key":  dataKey,
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
