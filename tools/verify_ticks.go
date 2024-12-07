package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gohustle/proto"

	googleproto "google.golang.org/protobuf/proto"
)

const DefaultDataPath = "data/ticks"

func main() {
	// Read all .pb files from data directory
	pattern := filepath.Join(DefaultDataPath, "*.pb")
	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Printf("Error reading directory: %v\n", err)
		return
	}

	if len(files) == 0 {
		fmt.Printf("No .pb files found in %s\n", DefaultDataPath)
		// List all files in the directory for debugging
		if entries, err := os.ReadDir(DefaultDataPath); err == nil {
			fmt.Println("\nContents of data directory:")
			for _, entry := range entries {
				fmt.Printf("- %s\n", entry.Name())
			}
		}
		return
	}

	fmt.Printf("Found %d files to process\n", len(files))
	for _, file := range files {
		fmt.Printf("\nProcessing file: %s\n", file)

		// Read file content
		data, err := os.ReadFile(file)
		if err != nil {
			fmt.Printf("Error reading file %s: %v\n", file, err)
			continue
		}

		fmt.Printf("File size: %d bytes\n", len(data))

		// Unmarshal protobuf
		batch := &proto.TickBatch{}
		if err := googleproto.Unmarshal(data, batch); err != nil {
			fmt.Printf("Error unmarshaling file %s: %v\n", file, err)
			continue
		}

		fmt.Printf("Successfully unmarshaled batch\n")
		fmt.Printf("Batch size: %d\n", len(batch.Ticks))
		if batch.Metadata != nil {
			fmt.Printf("Metadata: timestamp=%v, size=%d, retries=%d\n",
				time.Unix(batch.Metadata.Timestamp, 0),
				batch.Metadata.BatchSize,
				batch.Metadata.RetryCount)
		}

		// Print first 5 ticks as sample
		for i, tick := range batch.Ticks {
			if i >= 5 {
				break
			}
			fmt.Printf("\nTick %d:\n", i+1)
			fmt.Printf("  Token: %d\n", tick.InstrumentToken)
			fmt.Printf("  Timestamp: %v\n", time.Unix(tick.Timestamp, 0))
			fmt.Printf("  LastPrice: %.2f\n", tick.LastPrice)
			fmt.Printf("  Volume: %d\n", tick.VolumeTraded)
			fmt.Printf("  OI: %d\n", tick.Oi)
			fmt.Printf("  Change: %.2f%%\n", tick.NetChange)
			fmt.Printf("  LastQuantity: %d\n", tick.LastTradedQuantity)
			fmt.Printf("  AveragePrice: %.2f\n", tick.AverageTradePrice)
			fmt.Printf("  TotalBuyQuantity: %d\n", tick.TotalBuyQuantity)
			fmt.Printf("  TotalSellQuantity: %d\n", tick.TotalSellQuantity)

			if tick.Ohlc != nil {
				fmt.Printf("  OHLC:\n")
				fmt.Printf("    Open: %.2f\n", tick.Ohlc.Open)
				fmt.Printf("    High: %.2f\n", tick.Ohlc.High)
				fmt.Printf("    Low: %.2f\n", tick.Ohlc.Low)
				fmt.Printf("    Close: %.2f\n", tick.Ohlc.Close)
			}

			if tick.Depth != nil {
				fmt.Printf("  Depth:\n")
				fmt.Printf("    Buy:\n")
				for _, buy := range tick.Depth.Buy {
					fmt.Printf("      Price: %.2f, Quantity: %d, Orders: %d\n",
						buy.Price, buy.Quantity, buy.Orders)
				}
				fmt.Printf("    Sell:\n")
				for _, sell := range tick.Depth.Sell {
					fmt.Printf("      Price: %.2f, Quantity: %d, Orders: %d\n",
						sell.Price, sell.Quantity, sell.Orders)
				}
			}
			fmt.Println()
		}
	}
}
