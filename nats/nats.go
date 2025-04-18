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
	DefaultURL           = "nats://localhost:4222"
	ReconnectWait        = 2 * time.Second
	MaxReconnectAttempts = 5
	PingInterval         = 30 * time.Second
	MaxPingOutstanding   = 2

	// Stream configuration
	StreamName   = "TICKS"
	TicksSubject = "ticks"
	MaxAge       = 24 * time.Hour    // Keep data for 24 hours
	MaxBytes     = 1024 * 1024 * 512 // 512MB
)

// NATSHelper manages NATS connections and operations
type NATSHelper struct {
	conn     *nats.Conn
	js       nats.JetStreamContext
	log      *logger.Logger
	mu       sync.RWMutex
	subjects map[string]struct{}
	ctx      context.Context
	cancel   context.CancelFunc
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
			log:      logger.L(),
			subjects: make(map[string]struct{}),
			ctx:      ctx,
			cancel:   cancel,
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
		n.js = nil
	}

	n.log.Info("NATS connection shut down", nil)
}

// Connect establishes a connection to NATS server
func (n *NATSHelper) Connect(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.conn != nil && n.conn.IsConnected() {
		return nil
	}

	n.log.Info("Connecting to NATS server", map[string]interface{}{
		"url": DefaultURL,
	})

	// Configure connection options
	opts := []nats.Option{
		nats.Name("gohustle-client"),
		nats.ReconnectWait(ReconnectWait),
		nats.MaxReconnects(MaxReconnectAttempts),
		nats.PingInterval(PingInterval),
		nats.MaxPingsOutstanding(MaxPingOutstanding),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			n.log.Error("NATS disconnected", map[string]interface{}{
				"error": err.Error(),
			})
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			n.log.Info("NATS reconnected", map[string]interface{}{
				"url": nc.ConnectedUrl(),
			})
		}),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			delivered, _ := sub.Delivered()
			n.log.Error("NATS error", map[string]interface{}{
				"error":     err.Error(),
				"subject":   sub.Subject,
				"queue":     sub.Queue,
				"delivered": delivered,
			})
		}),
	}

	// Connect to NATS
	nc, err := nats.Connect(DefaultURL, opts...)
	if err != nil {
		n.log.Error("Failed to connect to NATS", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		n.log.Error("Failed to create JetStream context", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}

	n.conn = nc
	n.js = js

	// Initialize stream
	if err := n.setupStream(); err != nil {
		n.log.Error("Failed to setup stream", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to setup stream: %w", err)
	}

	n.log.Info("Successfully connected to NATS", map[string]interface{}{
		"url":    nc.ConnectedUrl(),
		"stream": StreamName,
	})

	return nil
}

// setupStream creates or updates the JetStream stream
func (n *NATSHelper) setupStream() error {
	// Delete existing stream if it exists
	n.js.DeleteStream(StreamName)

	// Create new stream with proper configuration
	config := &nats.StreamConfig{
		Name:      StreamName,
		Subjects:  []string{TicksSubject},
		Retention: nats.LimitsPolicy,
		MaxAge:    MaxAge,
		MaxBytes:  MaxBytes,
		Storage:   nats.FileStorage,
		Discard:   nats.DiscardOld,
	}

	// Create new stream
	if _, err := n.js.AddStream(config); err != nil {
		n.log.Error("Failed to create stream", map[string]interface{}{
			"error":  err.Error(),
			"stream": StreamName,
			"config": config,
		})
		return fmt.Errorf("failed to create stream: %w", err)
	}

	n.log.Info("Created new stream", map[string]interface{}{
		"stream": StreamName,
		"config": config,
	})

	return nil
}

// PublishTick publishes a tick to NATS
func (n *NATSHelper) PublishTick(ctx context.Context, subject string, data []byte) error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.conn == nil || !n.conn.IsConnected() {
		return fmt.Errorf("not connected to NATS")
	}

	// Publish with JetStream
	_, err := n.js.Publish(subject, data)
	if err != nil {
		n.log.Error("Failed to publish tick", map[string]interface{}{
			"error":   err.Error(),
			"subject": subject,
		})
		return fmt.Errorf("failed to publish tick: %w", err)
	}
	n.log.Info("Published tick", map[string]interface{}{
		"subject": subject,
	})

	return nil
}

// SubscribeTicks subscribes to tick updates
func (n *NATSHelper) SubscribeTicks(ctx context.Context, subject string, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.conn == nil || !n.conn.IsConnected() {
		return nil, fmt.Errorf("not connected to NATS")
	}

	// Subscribe with JetStream
	sub, err := n.js.Subscribe(subject, handler)
	if err != nil {
		n.log.Error("Failed to subscribe to ticks", map[string]interface{}{
			"error":   err.Error(),
			"subject": subject,
		})
		return nil, fmt.Errorf("failed to subscribe to ticks: %w", err)
	}

	n.log.Info("Successfully subscribed to ticks", map[string]interface{}{
		"subject": subject,
	})

	return sub, nil
}
