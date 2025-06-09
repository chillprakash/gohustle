package nats

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gohustle/backend/logger"
	pb "gohustle/backend/proto"

	"google.golang.org/protobuf/proto"
)

const (
	PublishChannelSize = 50000                 // Large buffer
	DefaultWorkers     = 10                    // Many workers
	MaxBatchSize       = 100                   // Large batch size
	BatchTimeout       = 25 * time.Millisecond // Quick batching
)

var (
	producerInstance *TickProducer
	producerOnce     sync.Once
	producerMu       sync.RWMutex
)

// TickProducer handles parallel publishing of ticks
type TickProducer struct {
	nats         *NATSHelper
	log          *logger.Logger
	publishChan  chan *publishJob
	workerCount  int
	wg           sync.WaitGroup
	metrics      *producerMetrics
	workerStatus []bool // Track worker initialization status
}

type publishJob struct {
	subject string
	tick    *pb.TickData
}

type batchJob struct {
	ticks    []*pb.TickData
	subjects []string
}

type producerMetrics struct {
	publishedCount uint64
	errorCount     uint64
	batchCount     uint64
	avgBatchSize   uint64
}

// GetTickProducer returns a singleton instance of TickProducer
func GetTickProducer() *TickProducer {
	producerMu.RLock()
	if producerInstance != nil {
		producerMu.RUnlock()
		return producerInstance
	}
	producerMu.RUnlock()

	producerMu.Lock()
	defer producerMu.Unlock()

	producerOnce.Do(func() {
		producerInstance = &TickProducer{
			nats:         GetNATSHelper(),
			log:          logger.L(),
			publishChan:  make(chan *publishJob, PublishChannelSize),
			workerCount:  DefaultWorkers,
			metrics:      &producerMetrics{},
			workerStatus: make([]bool, DefaultWorkers),
		}
	})

	producerInstance.log.Info("Tick producer instance created", map[string]interface{}{
		"worker_count": producerInstance.workerCount,
		"channel_size": PublishChannelSize,
	})

	producerInstance.Initialize(context.Background())

	return producerInstance
}

// Initialize initializes the producer and starts the workers
func (p *TickProducer) Initialize(ctx context.Context) error {
	p.log.Info("Initializing tick producer", map[string]interface{}{
		"worker_count": p.workerCount,
		"channel_size": PublishChannelSize,
	})

	if err := p.nats.Initialize(ctx); err != nil {
		p.log.Error("Failed to initialize NATS connection", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to initialize NATS connection: %w", err)
	}

	p.log.Info("NATS connection initialized successfully", nil)

	// Start the producer workers
	go func() {
		if err := p.Start(ctx); err != nil {
			p.log.Error("Failed to start producer", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}()

	return nil
}

// Start begins the producer workers
func (p *TickProducer) Start(ctx context.Context) error {
	// Start metrics logging
	go p.logMetrics(ctx)

	p.log.Info("Starting producer workers", map[string]interface{}{
		"worker_count": p.workerCount,
	})

	// Create initialization channel
	initChan := make(chan int, p.workerCount)

	// Start worker pool
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go func(workerId int) {
			// Mark worker as initialized before starting main loop
			p.workerStatus[workerId] = true
			initChan <- workerId

			p.log.Info("Worker initialized", map[string]interface{}{
				"worker_id": workerId,
				"status":    "initialized",
			})

			// Start worker main loop
			p.worker(ctx, workerId)
		}(i)
	}

	// Wait for all workers to initialize
	initializedWorkers := 0
	timeout := time.After(5 * time.Second)

	for initializedWorkers < p.workerCount {
		select {
		case <-timeout:
			activeWorkers := 0
			for _, status := range p.workerStatus {
				if status {
					activeWorkers++
				}
			}
			p.log.Error("Worker initialization timed out", map[string]interface{}{
				"active_workers": activeWorkers,
				"total_workers":  p.workerCount,
			})
			return fmt.Errorf("worker initialization timed out, only %d/%d workers started", activeWorkers, p.workerCount)

		case workerId := <-initChan:
			initializedWorkers++
			p.log.Debug("Worker reported initialization", map[string]interface{}{
				"worker_id":         workerId,
				"initialized_count": initializedWorkers,
				"total_workers":     p.workerCount,
			})
		}
	}

	p.log.Info("All workers initialized successfully", map[string]interface{}{
		"active_workers": initializedWorkers,
		"total_workers":  p.workerCount,
	})

	// Wait for context cancellation
	<-ctx.Done()

	// Wait for all workers to finish with timeout
	shutdownComplete := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		p.log.Info("All workers shut down gracefully", nil)
	case <-time.After(5 * time.Second):
		p.log.Error("Worker shutdown timed out", map[string]interface{}{
			"timeout": "5s",
		})
	}

	return nil
}

// PublishTick publishes a tick asynchronously
func (p *TickProducer) PublishTick(ctx context.Context, subject string, tick *pb.TickData) error {
	// Try to send to publish channel with minimal timeout
	select {
	case p.publishChan <- &publishJob{subject: subject, tick: tick}:
		p.log.Debug("Successfully queued tick for publishing", map[string]interface{}{
			"subject": subject,
			"token":   tick.InstrumentToken,
		})
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while publishing tick")
	case <-time.After(50 * time.Millisecond):
		return fmt.Errorf("channel full - dropping message")
	}
}

// worker processes publish jobs in batches
func (p *TickProducer) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	defer func() {
		p.workerStatus[id] = false
		p.log.Info("Worker shutting down", map[string]interface{}{
			"worker_id": id,
			"status":    "stopped",
		})
	}()

	// Create batch with pre-allocated capacity
	batch := &batchJob{
		ticks:    make([]*pb.TickData, 0, MaxBatchSize),
		subjects: make([]string, 0, MaxBatchSize),
	}

	ticker := time.NewTicker(BatchTimeout)
	defer ticker.Stop()

	p.log.Info("Worker started processing", map[string]interface{}{
		"worker_id": id,
		"status":    "running",
	})

	for {
		select {
		case <-ctx.Done():
			// Flush any remaining items before shutting down
			if len(batch.ticks) > 0 {
				p.publishBatch(ctx, batch)
			}
			return

		case job := <-p.publishChan:
			// Add to batch
			batch.ticks = append(batch.ticks, job.tick)
			batch.subjects = append(batch.subjects, job.subject)

			// Publish if batch is full
			if len(batch.ticks) >= MaxBatchSize {
				p.publishBatch(ctx, batch)
				batch.ticks = batch.ticks[:0]
				batch.subjects = batch.subjects[:0]
			}

			// Drain channel while we're at it (up to batch size)
			drainCount := 0
			for len(batch.ticks) < MaxBatchSize && drainCount < 50 {
				select {
				case job := <-p.publishChan:
					batch.ticks = append(batch.ticks, job.tick)
					batch.subjects = append(batch.subjects, job.subject)
					drainCount++
				default:
					// No more messages
					break
				}
			}

			// If we accumulated enough messages, publish
			if len(batch.ticks) >= MaxBatchSize/2 {
				p.publishBatch(ctx, batch)
				batch.ticks = batch.ticks[:0]
				batch.subjects = batch.subjects[:0]
			}

		case <-ticker.C:
			if len(batch.ticks) > 0 {
				p.publishBatch(ctx, batch)
				batch.ticks = batch.ticks[:0]
				batch.subjects = batch.subjects[:0]
			}
		}
	}
}

// publishBatch publishes a batch of ticks to NATS
func (p *TickProducer) publishBatch(ctx context.Context, batch *batchJob) {
	batchSize := len(batch.ticks)
	if batchSize == 0 {
		return
	}

	startTime := time.Now()
	p.log.Debug("Publishing batch", map[string]interface{}{
		"size": batchSize,
	})

	atomic.AddUint64(&p.metrics.batchCount, 1)
	atomic.AddUint64(&p.metrics.avgBatchSize, uint64(batchSize))

	// Process each message in the batch
	successCount := 0
	for i := 0; i < batchSize; i++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			p.log.Error("Batch publishing cancelled", map[string]interface{}{
				"processed": i,
				"total":     batchSize,
				"error":     ctx.Err().Error(),
			})
			return
		default:
		}

		data, err := proto.Marshal(batch.ticks[i])
		if err != nil {
			atomic.AddUint64(&p.metrics.errorCount, 1)
			p.log.Error("Failed to marshal tick", map[string]interface{}{
				"error": err.Error(),
				"index": i,
			})
			continue
		}

		if err := p.nats.Publish(ctx, batch.subjects[i], data); err != nil {
			atomic.AddUint64(&p.metrics.errorCount, 1)
			p.log.Error("Failed to publish message", map[string]interface{}{
				"error": err.Error(),
				"index": i,
			})
			continue
		}

		successCount++
		atomic.AddUint64(&p.metrics.publishedCount, 1)
	}

	elapsedMs := time.Since(startTime).Milliseconds()
	if elapsedMs > 100 {
		// Log slow batches
		p.log.Debug("Batch publishing completed", map[string]interface{}{
			"size":         batchSize,
			"success":      successCount,
			"failed":       batchSize - successCount,
			"duration_ms":  elapsedMs,
			"rate_per_sec": int64(float64(successCount) / (float64(elapsedMs) / 1000.0)),
		})
	}
}

// logMetrics periodically logs producer metrics
func (p *TickProducer) logMetrics(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			published := atomic.LoadUint64(&p.metrics.publishedCount)
			errors := atomic.LoadUint64(&p.metrics.errorCount)
			batches := atomic.LoadUint64(&p.metrics.batchCount)
			avgSize := atomic.LoadUint64(&p.metrics.avgBatchSize)
			if batches > 0 {
				avgSize = avgSize / batches
			}

			p.log.Info("Producer metrics", map[string]interface{}{
				"published_count": published,
				"error_count":     errors,
				"batch_count":     batches,
				"avg_batch_size":  avgSize,
				"worker_count":    p.workerCount,
				"channel_pending": len(p.publishChan),
			})
		}
	}
}

// GetMetrics returns current producer metrics
func (p *TickProducer) GetMetrics() (uint64, uint64) {
	return atomic.LoadUint64(&p.metrics.publishedCount),
		atomic.LoadUint64(&p.metrics.errorCount)
}
