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
	pattern := filepath.Join(DefaultDataPath, "NIFTY_20241212_09122024.pb")
	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Printf("Error reading directory: %v\n", err)
		return
	}

	if len(files) == 0 {
		fmt.Printf("No .pb files found in %s\n", DefaultDataPath)
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

		fmt.Printf("Number of ticks: %d\n", len(batch.Ticks))

		// Add timestamp analysis
		timestamps := make(map[time.Time]bool)
		var lastEventTime time.Time
		var firstEventTime time.Time

		for _, tick := range batch.Ticks {
			ts := time.Unix(tick.Timestamp, 0)
			fmt.Printf("Tick timestamp: %v\n", ts)
			timestamps[ts] = true

			if firstEventTime.IsZero() {
				firstEventTime = ts
			}
			if ts.After(lastEventTime) {
				lastEventTime = ts
			}
		}

		fmt.Printf("\nTimestamp Analysis:\n")
		fmt.Printf("First Event: %v\n", firstEventTime)
		fmt.Printf("Last Event: %v\n", lastEventTime)
		fmt.Printf("Duration: %v\n", lastEventTime.Sub(firstEventTime))

		// Check for missing seconds
		missingCount := 0
		for t := firstEventTime; !t.After(lastEventTime); t = t.Add(time.Second) {
			if !timestamps[t] {
				missingCount++
				if missingCount <= 5 { // Show only first 5 missing timestamps
					fmt.Printf("Missing data at: %v\n", t)
				}
			}
		}
		if missingCount > 0 {
			fmt.Printf("Total missing seconds: %d\n", missingCount)
		} else {
			fmt.Printf("No missing seconds in the data\n")
		}

		fmt.Printf("Successfully unmarshaled batch\n")
		fmt.Printf("Batch size: %d\n", len(batch.Ticks))
		if batch.Metadata != nil {
			fmt.Printf("Metadata: timestamp=%v, size=%d, retries=%d\n",
				time.Unix(batch.Metadata.Timestamp, 0),
				batch.Metadata.BatchSize,
				batch.Metadata.RetryCount)
		}

		// Print all ticks with complete details
		for i, tick := range batch.Ticks {
			if i >= 5 {
				break
			}
			fmt.Printf("\nTick %d:\n", i+1)
			fmt.Printf("Basic Info:\n")
			fmt.Printf("  InstrumentToken: %d\n", tick.InstrumentToken)
			fmt.Printf("  IsTradable: %v\n", tick.IsTradable)
			fmt.Printf("  IsIndex: %v\n", tick.IsIndex)
			fmt.Printf("  Mode: %s\n", tick.Mode)

			fmt.Printf("\nTimestamps:\n")
			fmt.Printf("  Timestamp: %v\n", time.Unix(tick.Timestamp, 0))
			fmt.Printf("  LastTradeTime: %v\n", time.Unix(tick.LastTradeTime, 0))

			fmt.Printf("\nPrice and Quantity:\n")
			fmt.Printf("  LastPrice: %.2f\n", tick.LastPrice)
			fmt.Printf("  LastTradedQuantity: %d\n", tick.LastTradedQuantity)
			fmt.Printf("  TotalBuyQuantity: %d\n", tick.TotalBuyQuantity)
			fmt.Printf("  TotalSellQuantity: %d\n", tick.TotalSellQuantity)
			fmt.Printf("  VolumeTraded: %d\n", tick.VolumeTraded)
			fmt.Printf("  TotalBuy: %d\n", tick.TotalBuy)
			fmt.Printf("  TotalSell: %d\n", tick.TotalSell)
			fmt.Printf("  AverageTradePrice: %.2f\n", tick.AverageTradePrice)

			fmt.Printf("\nOI Information:\n")
			fmt.Printf("  OI: %d\n", tick.Oi)
			fmt.Printf("  OI Day High: %d\n", tick.OiDayHigh)
			fmt.Printf("  OI Day Low: %d\n", tick.OiDayLow)
			fmt.Printf("  Net Change: %.2f%%\n", tick.NetChange)

			if tick.Ohlc != nil {
				fmt.Printf("\nOHLC Data:\n")
				fmt.Printf("  Open: %.2f\n", tick.Ohlc.Open)
				fmt.Printf("  High: %.2f\n", tick.Ohlc.High)
				fmt.Printf("  Low: %.2f\n", tick.Ohlc.Low)
				fmt.Printf("  Close: %.2f\n", tick.Ohlc.Close)
			}

			if tick.Depth != nil {
				fmt.Printf("\nMarket Depth:\n")
				fmt.Printf("  Buy Orders:\n")
				for j, buy := range tick.Depth.Buy {
					fmt.Printf("    %d. Price: %.2f, Quantity: %d, Orders: %d\n",
						j+1, buy.Price, buy.Quantity, buy.Orders)
				}
				fmt.Printf("  Sell Orders:\n")
				for j, sell := range tick.Depth.Sell {
					fmt.Printf("    %d. Price: %.2f, Quantity: %d, Orders: %d\n",
						j+1, sell.Price, sell.Quantity, sell.Orders)
				}
			}
			fmt.Println("----------------------------------------")
		}
	}
}
