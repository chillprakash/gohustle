package zerodha

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/zerodha/gokiteconnect/v4/models"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"
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

	// Log incoming tick data
	log.Info("Received tick", map[string]interface{}{
		"connection_id":    connectionID,
		"instrument_token": tick.InstrumentToken,
		"timestamp":        tick.Timestamp.Format("2006-01-02 15:04:05.000"),
		"last_trade_time":  tick.LastTradeTime.Format("2006-01-02 15:04:05.000"),
		"last_price":       tick.LastPrice,
		"last_traded_qty":  tick.LastTradedQuantity,
		"volume":           tick.VolumeTraded,
		"total_buy_qty":    tick.TotalBuyQuantity,
		"total_sell_qty":   tick.TotalSellQuantity,
		"avg_trade_price":  tick.AverageTradePrice,
		"oi":               tick.OI,
		"oi_day_high":      tick.OIDayHigh,
		"oi_day_low":       tick.OIDayLow,
		"net_change":       tick.NetChange,
		"is_index":         tick.IsIndex,
		"mode":             tick.Mode,
		"ohlc": map[string]float64{
			"open":  tick.OHLC.Open,
			"high":  tick.OHLC.High,
			"low":   tick.OHLC.Low,
			"close": tick.OHLC.Close,
		},
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
