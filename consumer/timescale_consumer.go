package consumer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gohustle/config"
	"gohustle/db"
	"gohustle/logger"
	proto "gohustle/proto"
	"gohustle/zerodha"

	"github.com/hibiken/asynq"
	googleproto "google.golang.org/protobuf/proto"
)

func StartTimescaleConsumer(cfg *config.Config, kite *zerodha.KiteConnect, timescaleDB *db.TimescaleDB, ready chan<- struct{}) {
	log := logger.GetLogger()
	log.Info("Starting Timescale Consumer", nil)

	// Create wait group for consumers
	var wg sync.WaitGroup

	// Get enabled queues from config
	enabledQueues := make([]string, 0)
	for queueName, queueCfg := range cfg.Asynq.Queues {
		if queueCfg.Enabled {
			enabledQueues = append(enabledQueues, queueName)
			wg.Add(1)
		}
	}

	// Start a consumer for each enabled queue
	for _, queueName := range enabledQueues {
		queueCfg := cfg.Asynq.Queues[queueName]

		// Start consumer for this queue
		go func(qName string, qConfig config.QueueConfig) {
			defer wg.Done()

			log.Info("Starting consumer for queue", map[string]interface{}{
				"queue_name":  qName,
				"priority":    qConfig.Priority,
				"concurrency": qConfig.Concurrency,
			})

			// Use queue-specific pattern
			pattern := fmt.Sprintf("process_tick_%s", qName)

			// Register handler for this queue
			kite.GetAsynqQueue().HandleFunc(pattern, func(ctx context.Context, t *asynq.Task) error {
				receiveTime := time.Now()

				// Unmarshal the tick
				tick := &proto.TickData{}
				if err := googleproto.Unmarshal(t.Payload(), tick); err != nil {
					log.Error("Failed to unmarshal tick", map[string]interface{}{
						"error": err.Error(),
						"queue": qName,
					})
					return fmt.Errorf("failed to unmarshal tick: %w", err)
				}

				processTime := time.Now()
				// Log detailed tick information
				log.Info("Processing tick", map[string]interface{}{
					"queue":           qName,
					"token":           tick.InstrumentToken,
					"timestamp":       time.Unix(tick.Timestamp, 0).Format("2006-01-02 15:04:05.000"),
					"last_trade_time": time.Unix(tick.LastTradeTime, 0).Format("2006-01-02 15:04:05.000"),
					"price":           tick.LastPrice,
					"last_traded_qty": tick.LastTradedQuantity,
					"volume":          tick.VolumeTraded,
					"total_buy_qty":   tick.TotalBuyQuantity,
					"total_sell_qty":  tick.TotalSellQuantity,
					"total_buy":       tick.TotalBuy,
					"total_sell":      tick.TotalSell,
					"avg_trade_price": tick.AverageTradePrice,
					"oi":              tick.Oi,
					"oi_day_high":     tick.OiDayHigh,
					"oi_day_low":      tick.OiDayLow,
					"net_change":      tick.NetChange,
					"is_index":        tick.IsIndex,
					"tradable":        tick.IsTradable,
					"mode":            tick.Mode,
					"ohlc": map[string]float64{
						"open":  tick.Ohlc.Open,
						"high":  tick.Ohlc.High,
						"low":   tick.Ohlc.Low,
						"close": tick.Ohlc.Close,
					},
					"depth": map[string]interface{}{
						"buy":  formatDepth(tick.Depth.Buy),
						"sell": formatDepth(tick.Depth.Sell),
					},
					"receive_latency": processTime.Sub(receiveTime).Milliseconds(),
					"tick_latency":    processTime.Sub(time.Unix(tick.Timestamp, 0)).Milliseconds(),
				})

				return nil
			})

			log.Info("Consumer ready for queue", map[string]interface{}{
				"queue":   qName,
				"pattern": pattern,
			})
		}(queueName, queueCfg)
	}

	// Signal that all consumers are ready
	close(ready)
	log.Info("All Timescale Consumers ready to receive ticks", map[string]interface{}{
		"enabled_queues": enabledQueues,
	})

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("Shutting down timescale consumers")
	wg.Wait() // Wait for all consumers to finish
}

// Helper function to format depth data
func formatDepth(items []*proto.TickData_DepthItem) []map[string]interface{} {
	result := make([]map[string]interface{}, len(items))
	for i, item := range items {
		result[i] = map[string]interface{}{
			"price":    item.Price,
			"quantity": item.Quantity,
			"orders":   item.Orders,
		}
	}
	return result
}
