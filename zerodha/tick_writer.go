package zerodha

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	sync "sync"
	"time"

	"gohustle/logger"
	proto "gohustle/proto"

	googleproto "google.golang.org/protobuf/proto"
)

type TickWriter struct {
	writeChannel chan *WriteRequest
	batchSize    int
	fileBuffers  map[string]*FileBuffer
	mutex        sync.RWMutex
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	errorChannel chan error
	baseDir      string
}

type FileBuffer struct {
	buffer    []*proto.TickData
	lastFlush time.Time
	retries   int
}

type WriteRequest struct {
	tick     *proto.TickData
	filename string
}

func NewTickWriter(ctx context.Context, batchSize int, baseDir string) *TickWriter {
	// Ensure base directory exists
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		panic(fmt.Sprintf("failed to create base directory: %v", err))
	}

	ctx, cancel := context.WithCancel(ctx)
	tw := &TickWriter{
		writeChannel: make(chan *WriteRequest, 100000), // Large buffer for high-frequency ticks
		batchSize:    batchSize,
		fileBuffers:  make(map[string]*FileBuffer),
		ctx:          ctx,
		cancel:       cancel,
		errorChannel: make(chan error, 1000),
		baseDir:      baseDir,
	}

	// Start multiple writer goroutines for parallel processing
	for i := 0; i < 4; i++ {
		tw.wg.Add(1)
		go tw.startWriter()
	}

	// Start error monitor
	go tw.monitorErrors()

	// Start periodic flush
	go tw.periodicFlush()

	return tw
}

func (tw *TickWriter) Write(tick *proto.TickData, filename string) error {
	select {
	case tw.writeChannel <- &WriteRequest{
		tick:     tick,
		filename: filename,
	}:
		return nil
	case <-tw.ctx.Done():
		return fmt.Errorf("writer is shutting down")
	default:
		// If channel is full, write to emergency backup
		return tw.writeToEmergencyFile(tick, filename)
	}
}

func (tw *TickWriter) writeToEmergencyFile(tick *proto.TickData, filename string) error {
	batch := &proto.TickBatch{
		Ticks: []*proto.TickData{tick},
		Metadata: &proto.BatchMetadata{
			Timestamp:  time.Now().Unix(),
			BatchSize:  1,
			RetryCount: 0,
		},
	}

	data, err := googleproto.Marshal(batch)
	if err != nil {
		return err
	}

	return tw.writeToFileAtomic(filename, data)
}

func (tw *TickWriter) startWriter() {
	defer tw.wg.Done()

	for {
		select {
		case <-tw.ctx.Done():
			tw.flushAllBuffers() // Ensure all data is written before shutdown
			return
		case req := <-tw.writeChannel:
			if err := tw.addToBatch(req.tick, req.filename); err != nil {
				tw.errorChannel <- err
			}
		}
	}
}

func (tw *TickWriter) addToBatch(tick *proto.TickData, filename string) error {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	if _, exists := tw.fileBuffers[filename]; !exists {
		tw.fileBuffers[filename] = &FileBuffer{
			buffer:    make([]*proto.TickData, 0, tw.batchSize),
			lastFlush: time.Now(),
		}
	}

	buffer := tw.fileBuffers[filename]
	buffer.buffer = append(buffer.buffer, tick)

	if len(buffer.buffer) >= tw.batchSize {
		return tw.flushBufferWithRetry(filename, buffer)
	}

	return nil
}

func (tw *TickWriter) flushBufferWithRetry(filename string, buffer *FileBuffer) error {
	const maxRetries = 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := tw.flushBuffer(filename, buffer); err != nil {
			lastErr = err
			buffer.retries++
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
			continue
		}
		buffer.retries = 0
		return nil
	}

	// Return the last error if all retries failed
	return fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

func (tw *TickWriter) flushBuffer(filename string, buffer *FileBuffer) error {
	if len(buffer.buffer) == 0 {
		return nil
	}

	batch := &proto.TickBatch{
		Ticks: buffer.buffer,
		Metadata: &proto.BatchMetadata{
			Timestamp:  time.Now().Unix(),
			BatchSize:  int32(len(buffer.buffer)),
			RetryCount: int32(buffer.retries),
		},
	}

	data, err := googleproto.Marshal(batch)
	if err != nil {
		return err
	}

	// Use full path with base directory
	fullPath := filepath.Join(tw.baseDir, filename)
	tempFile := fullPath + ".tmp"

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write to temp file
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return err
	}

	// Atomic rename
	if err := os.Rename(tempFile, fullPath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	// Reset buffer
	buffer.buffer = buffer.buffer[:0]
	buffer.lastFlush = time.Now()

	return nil
}

func (tw *TickWriter) writeToFileAtomic(filename string, data []byte) error {
	// Create full path including base directory
	fullPath := filepath.Join(tw.baseDir, filename)
	dir := filepath.Dir(fullPath)

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write to temp file
	tempFile := fullPath + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return err
	}

	// Atomic rename
	return os.Rename(tempFile, fullPath)
}

func (tw *TickWriter) monitorErrors() {
	log := logger.GetLogger()
	for err := range tw.errorChannel {
		log.Error("Tick writing error", map[string]interface{}{
			"error": err.Error(),
		})
		// Could add metrics/alerting here
	}
}

func (tw *TickWriter) Shutdown() error {
	tw.cancel()  // Signal all goroutines to stop
	tw.wg.Wait() // Wait for all writers to finish

	// Process any remaining emergency files
	return tw.processEmergencyFiles()
}

func (tw *TickWriter) processEmergencyFiles() error {
	log := logger.GetLogger()
	files, err := filepath.Glob("emergency_*.pb")
	if err != nil {
		return err
	}

	for _, file := range files {
		// Process emergency file
		if err := tw.processEmergencyFile(file); err != nil {
			log.Error("Failed to process emergency file", map[string]interface{}{
				"file":  file,
				"error": err.Error(),
			})
		}
	}
	return nil
}

func generateFileName(tick *proto.TickData) string {
	// Get token info from cache
	tokenStr := fmt.Sprintf("%d", tick.InstrumentToken)
	tokenInfo, exists := reverseLookupCache[tokenStr]
	if !exists {
		return fmt.Sprintf("unknown_%s.pb", tokenStr)
	}

	// Extract index name from symbol
	symbolParts := strings.Split(tokenInfo.Symbol, "23") // Split at year
	index := symbolParts[0]                              // Get the index name

	currentDate := time.Unix(tick.Timestamp, 0)
	return fmt.Sprintf("%s_%s_%s.pb",
		index,
		tokenInfo.Expiry.Format("20060102"),
		currentDate.Format("20060102"))
}

func (tw *TickWriter) periodicFlush() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tw.ctx.Done():
			return
		case <-ticker.C:
			tw.flushStaleBuffers()
		}
	}
}

func (tw *TickWriter) processEmergencyFile(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	batch := &proto.TickBatch{}
	if err := googleproto.Unmarshal(data, batch); err != nil {
		return err
	}

	// Process each tick in the batch
	for _, tick := range batch.Ticks {
		if err := tw.Write(tick, generateFileName(tick)); err != nil {
			return err
		}
	}

	// Delete processed emergency file
	return os.Remove(filename)
}

func (tw *TickWriter) flushStaleBuffers() {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	now := time.Now()
	for filename, buffer := range tw.fileBuffers {
		if now.Sub(buffer.lastFlush) > 5*time.Second {
			if err := tw.flushBuffer(filename, buffer); err != nil {
				tw.errorChannel <- fmt.Errorf("failed to flush stale buffer for %s: %w", filename, err)
			}
		}
	}
}

func (tw *TickWriter) flushAllBuffers() {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	for filename, buffer := range tw.fileBuffers {
		if err := tw.flushBuffer(filename, buffer); err != nil {
			tw.errorChannel <- fmt.Errorf("failed to flush buffer for %s: %w", filename, err)
		}
	}
}
