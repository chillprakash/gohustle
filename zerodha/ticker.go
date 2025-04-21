package zerodha

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gohustle/logger"
	"gohustle/nats"
	pb "gohustle/proto"

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
func (k *KiteConnect) handleTick(tick models.Tick) {
	log := logger.L()
	natsProducer := nats.GetTickProducer()

	// Create a short-lived context for this tick
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	indexName, err := k.GetIndexNameFromToken(ctx, fmt.Sprintf("%d", tick.InstrumentToken))
	if err != nil {
		log.Error("Failed to get index name", map[string]interface{}{
			"error": err.Error(),
			"token": tick.InstrumentToken,
		})
		return
	}

	// Convert to protobuf with proper type conversions
	protoTick := &pb.TickData{
		// Basic info
		InstrumentToken: tick.InstrumentToken,
		IsTradable:      tick.IsTradable,
		IsIndex:         tick.IsIndex,
		Mode:            tick.Mode,
		IndexName:       indexName,

		// Timestamps - Convert time.Time to Unix timestamp
		Timestamp:        tick.Timestamp.Unix(),
		LastTradeTime:    tick.LastTradeTime.Unix(),
		TickRecievedTime: time.Now().Unix(),

		// Price and quantity - Direct conversions as types match
		LastPrice:          tick.LastPrice,
		LastTradedQuantity: tick.LastTradedQuantity,
		TotalBuyQuantity:   tick.TotalBuyQuantity,
		TotalSellQuantity:  tick.TotalSellQuantity,
		VolumeTraded:       tick.VolumeTraded,
		AverageTradePrice:  tick.AverageTradePrice,

		// Convert integer fields
		TotalBuy:  tick.TotalBuy,
		TotalSell: tick.TotalSell,

		// OI related fields
		Oi:        tick.OI,
		OiDayHigh: tick.OIDayHigh,
		OiDayLow:  tick.OIDayLow,
		NetChange: tick.NetChange,

		// OHLC data
		Ohlc: &pb.TickData_OHLC{
			Open:  tick.OHLC.Open,
			High:  tick.OHLC.High,
			Low:   tick.OHLC.Low,
			Close: tick.OHLC.Close,
		},

		// Market depth
		Depth: &pb.TickData_MarketDepth{
			Buy:  convertDepthItems(tick.Depth.Buy[:]),
			Sell: convertDepthItems(tick.Depth.Sell[:]),
		},

		// Additional metadata for tracking
		TickStoredInDbTime: time.Now().Unix(),
	}

	// Use hierarchical subject pattern for NATS
	subject := fmt.Sprintf("ticks.%s", indexName)

	// Publish asynchronously
	if err := natsProducer.PublishTick(ctx, subject, protoTick); err != nil {
		log.Error("Failed to publish tick", map[string]interface{}{
			"error":        err.Error(),
			"token":        tick.InstrumentToken,
			"index":        indexName,
			"last_price":   tick.LastPrice,
			"total_volume": tick.VolumeTraded,
		})
		return
	}
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

// Helper function to convert depth items with proper type handling
func convertDepthItems(items []models.DepthItem) []*pb.TickData_DepthItem {
	result := make([]*pb.TickData_DepthItem, len(items))
	for i, item := range items {
		result[i] = &pb.TickData_DepthItem{
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
