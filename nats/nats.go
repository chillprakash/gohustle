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

	// JetStream configuration
	StreamName             = "TICKS"
	TicksSubject           = "ticks"
	MaxMsgAge              = 30 * time.Minute
	MaxBytes               = 1 * 1024 * 1024 * 1024 // 1GB
	PublishAsyncMaxPending = 16384
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
			n.log.Error("NATS error", map[string]interface{}{
				"error":   err.Error(),
				"subject": sub.Subject,
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

	n.conn = nc
	n.log.Info("Successfully connected to NATS", map[string]interface{}{
		"url": nc.ConnectedUrl(),
	})

	// Initialize JetStream
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(PublishAsyncMaxPending))
	if err != nil {
		n.log.Error("Failed to create JetStream context", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}

	n.js = js
	n.log.Info("JetStream context created successfully", nil)

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

	// Wait for publish acknowledgment with context
	select {
	case <-ctx.Done():
		return fmt.Errorf("publish cancelled: %w", ctx.Err())
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
