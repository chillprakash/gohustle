package zerodha

import (
	"context"
	"errors"
	"strconv"
	"time"

	"gohustle/logger"
	"gohustle/proto"

	asynq "github.com/hibiken/asynq"
	googleproto "google.golang.org/protobuf/proto"

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
	log := logger.GetLogger()

	log.Debug("Received tick", map[string]interface{}{
		"token":     tick.InstrumentToken,
		"price":     tick.LastPrice,
		"quantity":  tick.LastTradedQuantity,
		"timestamp": tick.Timestamp,
	})

	select {
	case k.tickWorkerPool <- struct{}{}: // Acquire worker
		go func() {
			defer func() {
				<-k.tickWorkerPool // Release worker
				log.Info("Released worker", map[string]interface{}{
					"token":          tick.InstrumentToken,
					"workers_in_use": len(k.tickWorkerPool),
					"total_workers":  cap(k.tickWorkerPool),
				})
			}()

			startTime := time.Now()
			k.processTickData(tick)

			log.Debug("Processed tick", map[string]interface{}{
				"token":           tick.InstrumentToken,
				"processing_time": time.Since(startTime).Milliseconds(),
				"workers_in_use":  len(k.tickWorkerPool),
			})
		}()
	default:
		// Worker pool is full, log warning
		log.Error("Tick worker pool is full, dropping tick", map[string]interface{}{
			"instrument_token": tick.InstrumentToken,
			"workers_in_use":   len(k.tickWorkerPool),
			"total_workers":    cap(k.tickWorkerPool),
			"last_price":       tick.LastPrice,
			"timestamp":        tick.Timestamp,
		})
	}
}

func (k *KiteConnect) processTickData(tick kitemodels.Tick) {
	log := logger.GetLogger()

	// Convert to protobuf
	protoTick := &proto.TickData{
		InstrumentToken:    tick.InstrumentToken,
		IsTradable:         tick.IsTradable,
		IsIndex:            tick.IsIndex,
		Mode:               tick.Mode,
		Timestamp:          tick.Timestamp.Unix(),
		LastTradeTime:      tick.LastTradeTime.Unix(),
		LastPrice:          tick.LastPrice,
		LastTradedQuantity: tick.LastTradedQuantity,
		TotalBuyQuantity:   tick.TotalBuyQuantity,
		TotalSellQuantity:  tick.TotalSellQuantity,
		VolumeTraded:       tick.VolumeTraded,
		TotalBuy:           tick.TotalBuy,
		TotalSell:          tick.TotalSell,
		AverageTradePrice:  tick.AverageTradePrice,
		Oi:                 tick.OI,
		OiDayHigh:          tick.OIDayHigh,
		OiDayLow:           tick.OIDayLow,
		NetChange:          tick.NetChange,
		Ohlc: &proto.TickData_OHLC{
			Open:  tick.OHLC.Open,
			High:  tick.OHLC.High,
			Low:   tick.OHLC.Low,
			Close: tick.OHLC.Close,
		},
		Depth: &proto.TickData_Depth{
			Buy:  convertDepthItems(tick.Depth.Buy[:]),
			Sell: convertDepthItems(tick.Depth.Sell[:]),
		},
	}

	// Serialize protobuf
	payload, err := googleproto.Marshal(protoTick)
	if err != nil {
		log.Error("Failed to marshal proto tick", map[string]interface{}{
			"error": err.Error(),
			"token": tick.InstrumentToken,
		})
		return
	}

	fileTask := asynq.NewTask("process_tick_file", payload,
		asynq.Queue("ticks_file"),
		asynq.MaxRetry(3),
	)
	timescaleTask := asynq.NewTask("process_tick_timescale", payload,
		asynq.Queue("ticks_timescale"),
		asynq.MaxRetry(3),
	)

	if err := k.asynqQueue.Enqueue(context.Background(), fileTask); err != nil {
		log.Error("Failed to enqueue tick", map[string]interface{}{
			"error": err.Error(),
			"token": tick.InstrumentToken,
		})
		return
	}

	if err := k.asynqQueue.Enqueue(context.Background(), timescaleTask); err != nil {
		log.Error("Failed to enqueue tick", map[string]interface{}{
			"error": err.Error(),
			"token": tick.InstrumentToken,
		})
		return
	}

	log.Debug("Enqueued proto tick", map[string]interface{}{
		"token": tick.InstrumentToken,
		"price": tick.LastPrice,
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

// Helper function to convert depth items
func convertDepthItems(items []kitemodels.DepthItem) []*proto.TickData_DepthItem {
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
