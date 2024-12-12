package zerodha

import (
	"fmt"
	"os"
	sync "sync"
	"time"

	proto "gohustle/proto"

	googleproto "google.golang.org/protobuf/proto"
)

type TickWriter struct {
	baseDir string
	mu      sync.Mutex
}

func NewTickWriter(baseDir string) *TickWriter {
	return &TickWriter{
		baseDir: baseDir,
	}
}

func (w *TickWriter) Write(tick *proto.TickData, filename string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Create directory if not exists
	if err := os.MkdirAll(w.baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Read existing batch if file exists
	var batch proto.TickBatch
	if data, err := os.ReadFile(filename); err == nil {
		if err := googleproto.Unmarshal(data, &batch); err == nil {
			batch.Ticks = append(batch.Ticks, tick)
		}
	} else {
		// Create new batch if file doesn't exist
		batch.Ticks = []*proto.TickData{tick}
	}

	// Update metadata
	batch.Metadata = &proto.BatchMetadata{
		Timestamp:  time.Now().Unix(),
		BatchSize:  int32(len(batch.Ticks)),
		RetryCount: 0,
	}

	// Marshal and write
	data, err := googleproto.Marshal(&batch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %v", err)
	}

	return os.WriteFile(filename, data, 0644)
}
