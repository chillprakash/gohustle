package jetstream

import (
	"time"

	"github.com/nats-io/nats.go"
)

// Stream configuration
const (
	StreamName = "TICKS"
	Subject    = "ticks"
)

// Stream limits
const (
	MaxAge     = 30 * time.Minute
	MaxBytes   = 8 * 1024 * 1024 * 1024 // 8GB
	MaxMsgSize = 8 * 1024 * 1024        // 8MB
)

// GetStreamConfig returns the default stream configuration
func GetStreamConfig() *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:       StreamName,
		Subjects:   []string{Subject + ".>"},
		Storage:    nats.FileStorage,
		MaxAge:     MaxAge,
		MaxBytes:   MaxBytes,
		Replicas:   1,
		Retention:  nats.WorkQueuePolicy,
		Discard:    nats.DiscardOld,
		MaxMsgs:    -1,
		MaxMsgSize: MaxMsgSize,
	}
}
