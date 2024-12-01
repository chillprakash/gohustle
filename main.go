package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"hustleapp/go/config"
)

func main() {
	// Load config
	cfg, err := config.Load("development")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize DB
	database, err := NewTimescaleDB(&cfg.Timescale)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	ctx := context.Background()

	// Create test table
	createTableSQL := `
        CREATE TABLE IF NOT EXISTS concurrent_test (
            id SERIAL PRIMARY KEY,
            value TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
    `

	if _, err := database.Exec(ctx, createTableSQL); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Configuration
	numWorkers := 5
	insertsPerWorker := 10

	var wg sync.WaitGroup
	startTime := time.Now()

	// Launch workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < insertsPerWorker; j++ {
				value := fmt.Sprintf("worker-%d-value-%d", workerID, j)

				_, err := database.Exec(ctx,
					"INSERT INTO concurrent_test (value) VALUES ($1)",
					value,
				)

				if err != nil {
					log.Printf("Worker %d insert %d failed: %v", workerID, j, err)
					continue
				}

				log.Printf("Worker %d inserted value %d", workerID, j)
			}
		}(i)
	}

	// Wait for completion
	wg.Wait()
	duration := time.Since(startTime)

	// Verify results
	var count int
	err = database.QueryRow(ctx, "SELECT COUNT(*) FROM concurrent_test").Scan(&count)
	if err != nil {
		log.Fatalf("Failed to count records: %v", err)
	}

	log.Printf("Total records: %d", count)
	log.Printf("Time taken: %v", duration)
	log.Printf("Average insert rate: %.2f records/second",
		float64(count)/duration.Seconds())

	// Display some records
	rows, err := database.Query(ctx,
		"SELECT id, value, created_at FROM concurrent_test LIMIT 5")
	if err != nil {
		log.Fatalf("Failed to query records: %v", err)
	}
	defer rows.Close()

	log.Println("\nSample records:")
	for rows.Next() {
		var (
			id        int
			value     string
			createdAt time.Time
		)
		if err := rows.Scan(&id, &value, &createdAt); err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}
		log.Printf("ID: %d, Value: %s, Created: %v", id, value, createdAt)
	}

	// Optional: Clean up
	_, err = database.Exec(ctx, "DROP TABLE concurrent_test")
	if err != nil {
		log.Printf("Failed to clean up table: %v", err)
	}
}
