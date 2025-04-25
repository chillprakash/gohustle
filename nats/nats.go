package nats

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/logger"

	"github.com/nats-io/nats.go"
)

const (
	DefaultURL             = "nats://localhost:4222"
	ReconnectWait          = 1 * time.Second
	MaxReconnectAttempts   = 3
	PingInterval           = 20 * time.Second
	MaxPingOutstanding     = 2
	PublishAsyncMaxPending = 10000
)

// NATSHelper manages NATS connections
type NATSHelper struct {
	conn   *nats.Conn
	log    *logger.Logger
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	js     nats.JetStream
}

var (
	instance *NATSHelper
	once     sync.Once
	mu       sync.RWMutex
)

// GetNATSHelper returns a singleton instance of NATSHelper
func GetNATSHelper() *NATSHelper {
	mu.RLock()
	if instance != nil {
		mu.RUnlock()
		return instance
	}
	mu.RUnlock()

	mu.Lock()
	defer mu.Unlock()

	once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		instance = &NATSHelper{
			log:    logger.L(),
			ctx:    ctx,
			cancel: cancel,
		}
	})

	return instance
}

// Initialize initializes the NATS connection
func (n *NATSHelper) Initialize(ctx context.Context) error {
	if err := n.Connect(ctx); err != nil {
		return fmt.Errorf("failed to initialize NATS connection: %w", err)
	}
	return nil
}

// IsConnected checks if NATS is connected
func (n *NATSHelper) IsConnected() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.conn != nil && n.conn.IsConnected()
}

// Shutdown gracefully shuts down the NATS connection
func (n *NATSHelper) Shutdown() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.cancel != nil {
		n.cancel()
	}

	if n.conn != nil {
		n.conn.Close()
		n.conn = nil
	}

	n.log.Info("NATS connection shut down", nil)
}

// Add these methods before Connect
func (n *NATSHelper) handleDisconnect(nc *nats.Conn, err error) {
	if err != nil {
		n.log.Error("NATS disconnected", map[string]interface{}{
			"error": err.Error(),
		})
	}
}

func (n *NATSHelper) handleReconnect(nc *nats.Conn) {
	if nc != nil {
		n.log.Info("NATS reconnected", map[string]interface{}{
			"url": nc.ConnectedUrl(),
		})
	}
}

func (n *NATSHelper) handleError(nc *nats.Conn, sub *nats.Subscription, err error) {
	if err != nil {
		fields := map[string]interface{}{
			"error": err.Error(),
		}
		if sub != nil {
			fields["subject"] = sub.Subject
		}
		n.log.Error("NATS error", fields)
	}
}

// Connect establishes a connection to NATS server
func (n *NATSHelper) Connect(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.conn != nil && n.conn.IsConnected() && n.js != nil {
		return nil
	}

	n.log.Info("Connecting to NATS server", map[string]interface{}{
		"url": DefaultURL,
	})

	opts := []nats.Option{
		nats.Name("gohustle-client"),
		nats.ReconnectWait(ReconnectWait),
		nats.MaxReconnects(MaxReconnectAttempts),
		nats.PingInterval(PingInterval),
		nats.MaxPingsOutstanding(MaxPingOutstanding),
		nats.DisconnectErrHandler(n.handleDisconnect),
		nats.ReconnectHandler(n.handleReconnect),
		nats.ErrorHandler(n.handleError),
		nats.NoEcho(),
		nats.UseOldRequestStyle(),
	}

	// Connect to NATS
	nc, err := nats.Connect(DefaultURL, opts...)
	if err != nil {
		n.log.Error("Failed to connect to NATS", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	n.conn = nc
	n.log.Info("Successfully connected to NATS", map[string]interface{}{
		"url": nc.ConnectedUrl(),
	})

	// Initialize JetStream with higher limits
	js, err := nc.JetStream(
		nats.PublishAsyncMaxPending(PublishAsyncMaxPending),
	)
	if err != nil {
		n.log.Error("Failed to create JetStream context", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// Check if stream exists first
	stream, err := js.StreamInfo("TICKS")
	if err != nil && err != nats.ErrStreamNotFound {
		n.log.Error("Failed to get stream info", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to get stream info: %w", err)
	}

	// Create or update the stream
	streamConfig := &nats.StreamConfig{
		Name:       "TICKS",
		Subjects:   []string{"ticks.>"},
		Storage:    nats.FileStorage,
		MaxAge:     30 * time.Minute,
		MaxBytes:   50 * 1024 * 1024, // 50MB
		Replicas:   1,
		Discard:    nats.DiscardOld,
		MaxMsgs:    -1,
		MaxMsgSize: 10 * 1024, // 10KB max message size
	}

	// If stream exists, preserve its retention policy
	if stream != nil {
		streamConfig.Retention = stream.Config.Retention
	} else {
		// New stream gets WorkQueue policy
		streamConfig.Retention = nats.WorkQueuePolicy
	}

	if stream == nil {
		// Create new stream
		if _, err := js.AddStream(streamConfig); err != nil {
			n.log.Error("Failed to create stream", map[string]interface{}{
				"error": err.Error(),
			})
			return fmt.Errorf("failed to create stream: %w", err)
		}
		n.log.Info("Stream created successfully", map[string]interface{}{
			"name":      streamConfig.Name,
			"retention": streamConfig.Retention,
		})
	} else {
		// Update existing stream
		if _, err := js.UpdateStream(streamConfig); err != nil {
			n.log.Error("Failed to update stream", map[string]interface{}{
				"error": err.Error(),
			})
			return fmt.Errorf("failed to update stream: %w", err)
		}
		n.log.Info("Stream updated successfully", map[string]interface{}{
			"name":      streamConfig.Name,
			"retention": streamConfig.Retention,
		})
	}

	n.js = js
	n.log.Info("JetStream context and stream setup complete", map[string]interface{}{
		"pub_async_max_pending": PublishAsyncMaxPending,
		"stream":                streamConfig.Name,
	})

	return nil
}

// Publish publishes a message to NATS with minimal lock contention and proper acknowledgment
func (n *NATSHelper) Publish(ctx context.Context, subject string, data []byte) error {
	// Quick check for connection with read lock
	n.mu.RLock()
	js := n.js
	n.mu.RUnlock()

	if js == nil {
		return fmt.Errorf("NATS connection not established")
	}

	// Publish with context only
	pa, err := js.PublishAsync(subject, data)
	if err != nil {
		n.log.Error("Failed to publish message", map[string]interface{}{
			"subject": subject,
			"error":   err.Error(),
		})
		return fmt.Errorf("failed to publish message: %w", err)
	}

	// Use a shorter timeout for waiting for acknowledgment
	ackCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	// Wait for publish acknowledgment with shorter timeout
	select {
	case <-ackCtx.Done():
		if ctx.Err() != nil {
			// Parent context cancelled
			return fmt.Errorf("publish cancelled: %w", ctx.Err())
		}
		// Just timed out on ack, but assume it will succeed
		n.log.Debug("Publish ack timed out, continuing", map[string]interface{}{
			"subject": subject,
		})
		return nil
	case ack := <-pa.Ok():
		n.log.Debug("Message published successfully", map[string]interface{}{
			"subject": subject,
			"seq":     ack.Sequence,
			"stream":  ack.Stream,
		})
		return nil
	case err := <-pa.Err():
		return fmt.Errorf("publish error: %w", err)
	}
}

// QueueSubscribe subscribes to a NATS subject with a queue group
func (n *NATSHelper) QueueSubscribe(ctx context.Context, subject string, queue string, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	if n.conn == nil || !n.conn.IsConnected() {
		return nil, fmt.Errorf("NATS connection not established")
	}
	n.log.Info("Subscribing to subject", map[string]interface{}{
		"subject": subject,
		"queue":   queue,
	})
	return n.conn.QueueSubscribe(subject, queue, handler)
}

// Subscribe subscribes to a NATS subject
func (n *NATSHelper) Subscribe(ctx context.Context, subject string, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.conn == nil || !n.conn.IsConnected() {
		return nil, fmt.Errorf("NATS connection not established")
	}

	return n.conn.Subscribe(subject, handler)
}
