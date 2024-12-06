package zerodha

import (
	"errors"
	"strconv"
	"time"

	"gohustle/logger"

	kitemodels "github.com/zerodha/gokiteconnect/v4/models"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"
)

func (k *KiteConnect) ConnectTicker() error {
	log := logger.GetLogger()

	for i, ticker := range k.Tickers {
		if ticker == nil {
			log.Error("Ticker not initialized", map[string]interface{}{
				"connection": i + 1,
			})
			return errors.New("ticker not initialized")
		}

		// Set up callbacks for each ticker
		ticker.OnError(k.onError)
		ticker.OnClose(k.onClose)
		ticker.OnConnect(k.onConnect)
		ticker.OnReconnect(k.onReconnect)
		ticker.OnNoReconnect(k.onNoReconnect)
		ticker.OnTick(k.handleTick)

		log.Info("Setting up ticker callbacks", map[string]interface{}{
			"connection": i + 1,
		})

		// Start each connection
		go ticker.Serve()
	}

	// Wait for connections to establish
	time.Sleep(2 * time.Second)

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
func (k *KiteConnect) handleTick(tick kitemodels.Tick) {
	select {
	case k.tickWorkerPool <- struct{}{}: // Acquire worker
		go func() {
			defer func() {
				<-k.tickWorkerPool // Release worker
			}()
			k.processTickData(tick)
		}()
	default:
		// Worker pool is full, log warning
		log := logger.GetLogger()
		log.Error("Tick worker pool is full, dropping tick", map[string]interface{}{
			"instrument_token": tick.InstrumentToken,
			"workers":          cap(k.tickWorkerPool),
		})
	}
}

func (k *KiteConnect) processTickData(tick kitemodels.Tick) {
	log := logger.GetLogger()

	log.Info("Processing tick", map[string]interface{}{
		"mode":             tick.Mode,
		"instrument_token": tick.InstrumentToken,
		"is_tradable":      tick.IsTradable,
		"is_index":         tick.IsIndex,
		"timestamp":        tick.Timestamp,
		"last_trade_time":  tick.LastTradeTime,
		"last_price":       tick.LastPrice,
		"last_traded_qty":  tick.LastTradedQuantity,
		"total_buy_qty":    tick.TotalBuyQuantity,
		"total_sell_qty":   tick.TotalSellQuantity,
		"volume_traded":    tick.VolumeTraded,
		"total_buy":        tick.TotalBuy,
		"total_sell":       tick.TotalSell,
		"avg_trade_price":  tick.AverageTradePrice,
		"oi":               tick.OI,
		"oi_day_high":      tick.OIDayHigh,
		"oi_day_low":       tick.OIDayLow,
		"net_change":       tick.NetChange,
		"depth":            tick.Depth,
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

func (k *KiteConnect) onConnect() {
	log := logger.GetLogger()
	log.Info("Ticker connected", nil)
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
