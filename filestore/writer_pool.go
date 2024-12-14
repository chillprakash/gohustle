package filestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"gohustle/logger"
	"gohustle/proto"
)

const (
	defaultNumWorkers = 10
	defaultBufferSize = 1000
	defaultQueueSize  = 10000
	ticksDir          = "data/ticks" // Base directory for tick data
)

type WriteRequest struct {
	TargetFile string
	Tick       *proto.TickData
	ResultChan chan error
}

type WriterWorker struct {
	id         int
	workChan   chan *WriteRequest
	buffer     map[string][]*proto.TickData
	bufferSize int
	ctx        context.Context
	log        *logger.Logger
	baseDir    string // Base directory for writing files
}

type WriterPool struct {
	workers    []*WriterWorker
	workChan   chan *WriteRequest
	numWorkers int
	bufferSize int
	ctx        context.Context
	cancel     context.CancelFunc
	log        *logger.Logger
	wg         sync.WaitGroup // Added for worker synchronization
}

// Start starts all workers
func (p *WriterPool) Start() {
	for _, worker := range p.workers {
		p.wg.Add(1)
		go p.runWorker(worker)
	}
}

// runWorker runs the main worker loop
func (p *WriterPool) runWorker(w *WriterWorker) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			w.flushAll() // Flush before exit
			return
		case req := <-w.workChan:
			w.processRequest(req)
		}
	}
}

func (w *WriterWorker) processRequest(req *WriteRequest) {
	// Add single tick to appropriate buffer
	w.buffer[req.TargetFile] = append(w.buffer[req.TargetFile], req.Tick)

	// Check buffer size
	if len(w.buffer[req.TargetFile]) >= w.bufferSize {
		if err := w.flushFile(req.TargetFile); err != nil {
			req.ResultChan <- err
			return
		}
	}

	// Send success response
	req.ResultChan <- nil
}

func (w *WriterWorker) flushFile(targetFile string) error {
	ticks := w.buffer[targetFile]
	if len(ticks) == 0 {
		return nil
	}

	// Construct full file path
	fullPath := filepath.Join(w.baseDir, targetFile)

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write to parquet file
	if err := writeTicksToParquet(fullPath, ticks); err != nil {
		return fmt.Errorf("failed to write parquet file %s: %w", fullPath, err)
	}

	w.log.Debug("Flushed ticks to file", map[string]interface{}{
		"worker_id":   w.id,
		"file":        targetFile,
		"ticks_count": len(ticks),
		"full_path":   fullPath,
	})

	// Clear buffer after successful write
	w.buffer[targetFile] = w.buffer[targetFile][:0]
	return nil
}

func (w *WriterWorker) flushAll() {
	for targetFile := range w.buffer {
		if err := w.flushFile(targetFile); err != nil {
			w.log.Error("Failed to flush file", map[string]interface{}{
				"error":       err.Error(),
				"target_file": targetFile,
				"worker_id":   w.id,
			})
		}
	}
}

func NewWriterPool() *WriterPool {
	ctx, cancel := context.WithCancel(context.Background())

	// Using hardcoded values
	numWorkers := defaultNumWorkers // 10 workers
	bufferSize := defaultBufferSize // 1000 ticks per buffer

	// Get project root directory
	rootDir, err := os.Getwd()
	if err != nil {
		panic(fmt.Sprintf("failed to get working directory: %v", err))
	}

	// Construct base directory for ticks
	baseDir := filepath.Join(rootDir, ticksDir)

	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		panic(fmt.Sprintf("failed to create ticks directory: %v", err))
	}

	pool := &WriterPool{
		workers:    make([]*WriterWorker, numWorkers),
		workChan:   make(chan *WriteRequest, defaultQueueSize),
		numWorkers: numWorkers,
		bufferSize: bufferSize,
		ctx:        ctx,
		cancel:     cancel,
		log:        logger.GetLogger(),
	}

	// Initialize workers with base directory
	for i := 0; i < numWorkers; i++ {
		pool.workers[i] = &WriterWorker{
			id:         i,
			workChan:   pool.workChan,
			buffer:     make(map[string][]*proto.TickData),
			bufferSize: bufferSize,
			ctx:        ctx,
			log:        logger.GetLogger(),
			baseDir:    baseDir,
		}
	}

	return pool
}

func (p *WriterPool) Write(req *WriteRequest) error {
	select {
	case p.workChan <- req:
		return nil
	case <-p.ctx.Done():
		return fmt.Errorf("writer pool is shutting down")
	}
}

func (p *WriterPool) Stop() {
	p.cancel()
	p.wg.Wait()
}

// Helper function to write ticks to parquet file
func writeTicksToParquet(filePath string, ticks []*proto.TickData) error {
	// TODO: Implement actual parquet writing
	// For now, just log the operation
	return nil
}

// ... rest of the implementation (getOrCreateWriter, Flush methods etc.)
