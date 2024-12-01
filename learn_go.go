package go

import (
	"context"
	"fmt"
	"time"
)

func ExampleTimescale() {
	// Initialize database
	tsdb, err := NewTimescaleDB(nil) // Use default config
	if err != nil {
		fmt.Printf("Failed to initialize database: %v\n", err)
		return
	}
	defer tsdb.Close()

	ctx := context.Background()

	// Example query
	rows, err := tsdb.Query(ctx, `
		SELECT timestamp, symbol, price 
		FROM market_data 
		WHERE symbol = $1 
		LIMIT 5
	`, "NIFTY")
	
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		return
	}
	defer rows.Close()

	// Process results
	for rows.Next() {
		var (
			ts     time.Time
			symbol string
			price  float64
		)
		
		if err := rows.Scan(&ts, &symbol, &price); err != nil {
			fmt.Printf("Error scanning row: %v\n", err)
			continue
		}
		
		fmt.Printf("Data: %s %s %.2f\n", ts, symbol, price)
	}
}
