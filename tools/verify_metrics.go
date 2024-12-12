package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"gohustle/proto"
	"gohustle/zerodha"

	googleproto "google.golang.org/protobuf/proto"
)

type TickMetrics struct {
	TotalTicks        int
	UniqueTokens      map[uint32]bool
	FirstTickTime     time.Time
	LastTickTime      time.Time
	MaxPrice          float64
	MinPrice          float64
	MaxVolume         uint32
	TotalVolume       uint64
	MaxOI             float64
	TicksPerSecond    map[int64]int                      // timestamp -> count
	TokenDistribution map[uint32]int                     // token -> count
	QueueLatencies    []time.Duration                    // Processing latencies
	InstrumentInfo    map[uint32]*zerodha.InstrumentInfo // Token -> Instrument Info
}

func main() {
	// Directory containing tick files
	dataDir := "data/ticks"

	// Get all .pb files
	files, err := filepath.Glob(filepath.Join(dataDir, "*.pb"))
	if err != nil {
		fmt.Printf("Error finding files: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Found %d tick files\n", len(files))

	// Process each file
	totalMetrics := &TickMetrics{
		UniqueTokens:      make(map[uint32]bool),
		TicksPerSecond:    make(map[int64]int),
		TokenDistribution: make(map[uint32]int),
		InstrumentInfo:    make(map[uint32]*zerodha.InstrumentInfo),
	}

	// Get instrument info
	kite := zerodha.NewKiteConnect(nil, nil, nil) // Just for instrument operations
	if err := kite.DownloadInstrumentData(context.Background()); err != nil {
		fmt.Printf("Error downloading instrument data: %v\n", err)
		os.Exit(1)
	}

	for _, file := range files {
		processFile(file, totalMetrics, kite)
	}

	// Print analysis
	printMetrics(totalMetrics)
}

func processFile(filepath string, metrics *TickMetrics, kite *zerodha.KiteConnect) {
	// Read the file
	data, err := os.ReadFile(filepath)
	if err != nil {
		fmt.Printf("Error reading file %s: %v\n", filepath, err)
		return
	}

	// Unmarshal the batch
	batch := &proto.TickBatch{}
	if err := googleproto.Unmarshal(data, batch); err != nil {
		fmt.Printf("Error unmarshaling file %s: %v\n", filepath, err)
		return
	}

	// Process each tick
	for _, tick := range batch.Ticks {
		metrics.TotalTicks++
		metrics.UniqueTokens[tick.InstrumentToken] = true

		// Get instrument info if not already cached
		if _, exists := metrics.InstrumentInfo[tick.InstrumentToken]; !exists {
			if info, exists := kite.GetInstrumentInfo(fmt.Sprintf("%d", tick.InstrumentToken)); exists {
				metrics.InstrumentInfo[tick.InstrumentToken] = info
			}
		}

		tickTime := time.Unix(tick.Timestamp, 0)
		if metrics.FirstTickTime.IsZero() || tickTime.Before(metrics.FirstTickTime) {
			metrics.FirstTickTime = tickTime
		}
		if tickTime.After(metrics.LastTickTime) {
			metrics.LastTickTime = tickTime
		}

		// Price metrics
		if tick.LastPrice > metrics.MaxPrice {
			metrics.MaxPrice = tick.LastPrice
		}
		if metrics.MinPrice == 0 || tick.LastPrice < metrics.MinPrice {
			metrics.MinPrice = tick.LastPrice
		}

		// Volume metrics
		if tick.VolumeTraded > metrics.MaxVolume {
			metrics.MaxVolume = tick.VolumeTraded
		}
		metrics.TotalVolume += uint64(tick.VolumeTraded)

		// OI metrics
		if tick.Oi > metrics.MaxOI {
			metrics.MaxOI = tick.Oi
		}

		// Ticks per second
		metrics.TicksPerSecond[tick.Timestamp]++

		// Token distribution
		metrics.TokenDistribution[tick.InstrumentToken]++
	}
}

func printMetrics(metrics *TickMetrics) {
	fmt.Printf("\nTick Processing Metrics:\n")
	fmt.Printf("=======================\n")
	fmt.Printf("Total Ticks: %d\n", metrics.TotalTicks)
	fmt.Printf("Unique Tokens: %d\n", len(metrics.UniqueTokens))
	fmt.Printf("Time Range: %s to %s (Duration: %s)\n",
		metrics.FirstTickTime.Format("2006-01-02 15:04:05"),
		metrics.LastTickTime.Format("2006-01-02 15:04:05"),
		metrics.LastTickTime.Sub(metrics.FirstTickTime))

	// Calculate ticks per second stats
	var tps []int
	for _, count := range metrics.TicksPerSecond {
		tps = append(tps, count)
	}
	sort.Ints(tps)

	if len(tps) > 0 {
		fmt.Printf("\nTicks Per Second:\n")
		fmt.Printf("Max: %d\n", tps[len(tps)-1])
		fmt.Printf("Min: %d\n", tps[0])
		fmt.Printf("Avg: %.2f\n", float64(metrics.TotalTicks)/float64(len(metrics.TicksPerSecond)))
	}

	// Print top tokens by volume with instrument info
	fmt.Printf("\nTop 10 Most Active Instruments:\n")
	type tokenCount struct {
		token uint32
		count int
		info  *zerodha.InstrumentInfo
	}
	var tokenCounts []tokenCount
	for token, count := range metrics.TokenDistribution {
		tc := tokenCount{token: token, count: count}
		if info, exists := metrics.InstrumentInfo[token]; exists {
			tc.info = info
		}
		tokenCounts = append(tokenCounts, tc)
	}
	sort.Slice(tokenCounts, func(i, j int) bool {
		return tokenCounts[i].count > tokenCounts[j].count
	})

	fmt.Printf("\n%-12s %-20s %-10s %-15s %-10s %-10s\n",
		"Token", "Index", "Type", "Expiry", "Ticks", "% of Total")
	fmt.Printf("------------------------------------------------------------------------\n")
	for i := 0; i < min(10, len(tokenCounts)); i++ {
		tc := tokenCounts[i]
		instrumentType := "Unknown"
		index := "Unknown"
		expiry := "-"

		if tc.info != nil {
			index = tc.info.Index
			if tc.info.IsIndex {
				instrumentType = "Index"
			} else if tc.info.IsOptions {
				instrumentType = "Option"
			} else {
				instrumentType = "Future"
			}
			if !tc.info.IsIndex {
				expiry = tc.info.Expiry.Format("02-Jan-2006")
			}
		}

		fmt.Printf("%-12d %-20s %-10s %-15s %-10d %.2f%%\n",
			tc.token,
			index,
			instrumentType,
			expiry,
			tc.count,
			float64(tc.count)*100/float64(metrics.TotalTicks))
	}

	// Print price and volume metrics
	fmt.Printf("\nPrice and Volume Metrics:\n")
	fmt.Printf("------------------------\n")
	fmt.Printf("Max Price: %.2f\n", metrics.MaxPrice)
	fmt.Printf("Min Price: %.2f\n", metrics.MinPrice)
	fmt.Printf("Max Volume: %d\n", metrics.MaxVolume)
	fmt.Printf("Total Volume: %d\n", metrics.TotalVolume)
	fmt.Printf("Max OI: %.0f\n", metrics.MaxOI)

	// Print instrument type distribution
	fmt.Printf("\nInstrument Type Distribution:\n")
	fmt.Printf("---------------------------\n")
	typeCount := make(map[string]int)
	for token := range metrics.UniqueTokens {
		if info, exists := metrics.InstrumentInfo[token]; exists {
			if info.IsIndex {
				typeCount["Index"]++
			} else if info.IsOptions {
				typeCount["Option"]++
			} else {
				typeCount["Future"]++
			}
		} else {
			typeCount["Unknown"]++
		}
	}
	for iType, count := range typeCount {
		fmt.Printf("%-10s: %d\n", iType, count)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
