package zerodha

import (
	"context"
	"errors"
	"fmt"
	"gohustle/logger"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/zerodha/gokiteconnect/v4/models"
	"google.golang.org/protobuf/proto"
)

const (
	TickStream = "zerodha:tick:stream"
	TickGroup  = "zerodha:tick:group"
)

// Add Redis functionality to KiteConnect
func (k *KiteConnect) PublishTickData(ctx context.Context, instrumentToken uint32, tick *models.Tick) error {
	log := logger.GetLogger()

	if k.redisCache == nil {
		return errors.New("redis not initialized")
	}

	// Convert Kite tick to protobuf format
	protoTick := &TickData{
		InstrumentToken:    instrumentToken,
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
		Timestamp:          tick.Timestamp.Unix(),
		LastTradeTime:      tick.LastTradeTime.Unix(),
		Depth: &TickData_Depth{
			Buy:  convertDepth(tick.Depth.Buy[:]),
			Sell: convertDepth(tick.Depth.Sell[:]),
		},
	}

	// Serialize protobuf
	data, err := proto.Marshal(protoTick)
	if err != nil {
		log.Error("Failed to marshal tick data", map[string]interface{}{
			"error": err.Error(),
			"token": instrumentToken,
		})
		return errors.New("failed to marshal tick data")
	}

	// Publish to stream
	streamKey := fmt.Sprintf("%s:%d", TickStream, instrumentToken)
	return k.redisCache.XAdd(ctx, streamKey, map[string]interface{}{
		"data":      data,
		"token":     instrumentToken,
		"timestamp": time.Now().UnixNano(),
	})
}

func (k *KiteConnect) ConsumeTickData(ctx context.Context, instrumentToken uint32, handler func([]byte)) error {
	log := logger.GetLogger()
	streamKey := fmt.Sprintf("%s:%d", TickStream, instrumentToken)
	groupName := fmt.Sprintf("%s:%d", TickGroup, instrumentToken)
	consumerName := fmt.Sprintf("consumer:%d:%s", instrumentToken, uuid.New().String())

	// Ensure consumer group exists
	err := k.redisCache.XGroupCreate(ctx, streamKey, groupName, "0")
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	log.Info("Starting tick consumer", map[string]interface{}{
		"stream":   streamKey,
		"group":    groupName,
		"consumer": consumerName,
	})

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			streams, err := k.redisCache.XReadGroup(ctx, groupName, consumerName, streamKey, ">")
			if err != nil {
				if err == redis.Nil || err == context.Canceled {
					continue
				}
				log.Error("Failed to read from stream", map[string]interface{}{
					"error":  err.Error(),
					"stream": streamKey,
				})
				time.Sleep(time.Second)
				continue
			}

			for _, stream := range streams {
				for _, message := range stream.Messages {
					// Process message
					if data, ok := message.Values["data"].([]byte); ok {
						handler(data)
					}

					// Acknowledge message
					err = k.redisCache.XAck(ctx, streamKey, groupName, message.ID)
					if err != nil {
						log.Error("Failed to acknowledge message", map[string]interface{}{
							"error": err.Error(),
							"msgID": message.ID,
						})
					}
				}
			}
		}
	}
}

func (k *KiteConnect) ConsumeMultipleTickData(ctx context.Context, tokens []uint32, handler func(uint32, []byte)) error {
	log := logger.GetLogger()

	// Test Redis connection first
	client := k.redisCache.GetDefaultRedisDB0()
	if client == nil {
		log.Error("Failed to get Redis client")
		return fmt.Errorf("redis client initialization failed")
	}
	log.Info("Successfully got Redis client")

	// Verify Redis connection with PING
	if err := client.Ping(ctx).Err(); err != nil {
		log.Error("Redis ping failed", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("redis ping failed: %w", err)
	}
	log.Info("Redis ping successful")

	// Start processors with logging
	if err := k.StartProcessing(ctx); err != nil {
		log.Error("Failed to start processors", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to start processors: %w", err)
	}
	log.Info("Successfully started processors")

	streams := make([]string, len(tokens)*2)
	for i, token := range tokens {
		streams[i*2] = fmt.Sprintf("%s:%d", TickStream, token)
		streams[i*2+1] = ">"
	}
	log.Info("Created stream configurations", map[string]interface{}{
		"streams_count": len(streams) / 2,
		"first_stream":  streams[0],
	})

	groupName := fmt.Sprintf("%s:multi", TickGroup)
	consumerName := fmt.Sprintf("consumer:multi:%s", uuid.New().String())

	// Log group creation attempts
	for i := 0; i < len(streams); i += 2 {
		log.Info("Creating consumer group", map[string]interface{}{
			"stream": streams[i],
			"group":  groupName,
		})

		err := k.redisCache.XGroupCreate(ctx, streams[i], groupName, "0")
		if err != nil {
			if strings.Contains(err.Error(), "BUSYGROUP") {
				log.Info("Consumer group already exists", map[string]interface{}{
					"stream": streams[i],
					"group":  groupName,
				})
			} else {
				log.Error("Failed to create consumer group", map[string]interface{}{
					"error":  err.Error(),
					"stream": streams[i],
				})
				return fmt.Errorf("failed to create consumer group: %w", err)
			}
		}
	}

	log.Info("Starting stream reading loop", map[string]interface{}{
		"group":    groupName,
		"consumer": consumerName,
	})

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			result, err := k.redisCache.XReadGroup(ctx, groupName, consumerName, streams...)
			if err != nil {
				if err == redis.Nil || err == context.Canceled {
					continue
				}
				log.Error("Failed to read from streams", map[string]interface{}{
					"error": err.Error(),
				})
				time.Sleep(time.Second)
				continue
			}

			for _, stream := range result {
				parts := strings.Split(stream.Stream, ":")
				if len(parts) != 3 {
					continue
				}
				token, err := strconv.ParseUint(parts[2], 10, 32)
				if err != nil {
					continue
				}

				for _, msg := range stream.Messages {
					if data, ok := msg.Values["data"].([]byte); ok {
						// Call handler with token and raw data
						handler(uint32(token), data)

						// Process for TimeScale and Protobuf
						tick := &TickData{}
						if err := proto.Unmarshal(data, tick); err != nil {
							log.Error("Failed to unmarshal tick", map[string]interface{}{
								"error": err.Error(),
								"token": token,
							})
							continue
						}

						select {
						case k.tickProcessor.timescaleChan <- tick:
						default:
							log.Error("TimeScale channel full", map[string]interface{}{"token": token})
						}

						select {
						case k.tickProcessor.protobufChan <- tick:
						default:
							log.Error("Protobuf channel full", map[string]interface{}{"token": token})
						}
					}

					if err := k.redisCache.XAck(ctx, stream.Stream, groupName, msg.ID); err != nil {
						log.Error("Failed to acknowledge message", map[string]interface{}{
							"error": err.Error(),
							"msgID": msg.ID,
						})
					}
				}
			}
		}
	}
}

// Helper function to convert depth items
func convertDepth(items []models.DepthItem) []*TickData_DepthItem {
	result := make([]*TickData_DepthItem, len(items))
	for i, item := range items {
		result[i] = &TickData_DepthItem{
			Price:    item.Price,
			Quantity: uint32(item.Quantity),
			Orders:   uint32(item.Orders),
		}
	}
	return result
}
