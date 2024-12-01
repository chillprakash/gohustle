package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"gohustle/config"
	"gohustle/db"
	"gohustle/logger"
)

// SensorData represents a sample data structure
type SensorData struct {
	DeviceID    string
	Temperature float64
	Humidity    float64
	Timestamp   time.Time
}

type SensorDataService struct {
	db     *db.TimescaleDB
	logger *logger.Logger
}

func NewSensorDataService(db *db.TimescaleDB) *SensorDataService {
	return &SensorDataService{
		db:     db,
		logger: logger.GetLogger(),
	}
}

func main() {
	log := logger.GetLogger()

	// Load config
	cfg, err := config.GetConfig()
	if err != nil {
		log.Error("Failed to load config", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Initialize TimescaleDB
	database, err := db.NewTimescaleDB(cfg.Timescale)
	if err != nil {
		log.Error("Failed to initialize TimescaleDB", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}
	defer database.Close()

	service := NewSensorDataService(database)

	// Generate sample data
	data := service.generateSampleData(10000000)
	log.Info("Generated sample data", map[string]interface{}{
		"count": len(data),
	})

	// Perform batch insert
	startTime := time.Now()
	err = service.BatchInsertSensorData(data, 1000, 5)
	if err != nil {
		log.Error("Failed to insert sensor data", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	log.Info("Successfully inserted sensor data", map[string]interface{}{
		"duration_ms": time.Since(startTime).Milliseconds(),
		"records":     len(data),
	})
}

func (s *SensorDataService) generateSampleData(count int) []SensorData {
	data := make([]SensorData, count)
	for i := range data {
		data[i] = SensorData{
			DeviceID:    fmt.Sprintf("device_%d", i%10),
			Temperature: 20.5 + float64(i%10),
			Humidity:    50.0 + float64(i%20),
			Timestamp:   time.Now().Add(-time.Duration(i) * time.Minute),
		}
	}
	return data
}

func (s *SensorDataService) BatchInsertSensorData(data []SensorData, batchSize int, workers int) error {
	if len(data) == 0 {
		return nil
	}

	s.logger.Info("Starting batch insert", map[string]interface{}{
		"total_records": len(data),
		"batch_size":    batchSize,
		"workers":       workers,
	})

	// Create work chunks
	chunks := make([][]SensorData, 0, (len(data)+batchSize-1)/batchSize)
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}
		chunks = append(chunks, data[i:end])
	}

	// Error channel to collect errors from goroutines
	errChan := make(chan error, len(chunks))
	var wg sync.WaitGroup

	// Process chunks with worker pool
	for i := 0; i < len(chunks); i++ {
		wg.Add(1)
		go func(chunkID int, chunk []SensorData) {
			defer wg.Done()

			startTime := time.Now()
			ctx := context.Background()
			tx, err := s.db.Begin(ctx)
			if err != nil {
				s.logger.Error("Failed to begin transaction", map[string]interface{}{
					"chunk_id": chunkID,
					"error":    err.Error(),
				})
				errChan <- fmt.Errorf("failed to begin transaction: %w", err)
				return
			}
			defer tx.Rollback(ctx)

			// Prepare the batch insert statement
			batch := &pgx.Batch{}
			const query = `
                INSERT INTO sensor_data (device_id, temperature, humidity, timestamp)
                VALUES ($1, $2, $3, $4)`

			for _, record := range chunk {
				batch.Queue(query,
					record.DeviceID,
					record.Temperature,
					record.Humidity,
					record.Timestamp,
				)
			}

			// Execute batch
			results := tx.SendBatch(ctx, batch)
			defer results.Close()

			if err := results.Close(); err != nil {
				s.logger.Error("Failed to execute batch", map[string]interface{}{
					"chunk_id": chunkID,
					"error":    err.Error(),
				})
				errChan <- fmt.Errorf("failed to execute batch: %w", err)
				return
			}

			if err := tx.Commit(ctx); err != nil {
				s.logger.Error("Failed to commit transaction", map[string]interface{}{
					"chunk_id": chunkID,
					"error":    err.Error(),
				})
				errChan <- fmt.Errorf("failed to commit transaction: %w", err)
				return
			}

			s.logger.Info("Chunk processed successfully", map[string]interface{}{
				"chunk_id":    chunkID,
				"records":     len(chunk),
				"duration_ms": time.Since(startTime).Milliseconds(),
			})
		}(i, chunks[i])
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(errChan)

	// Collect any errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("batch insert errors: %v", errors)
	}

	return nil
}
