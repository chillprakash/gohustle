package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"gohustle/proto"
	"gohustle/zerodha"

	googleproto "google.golang.org/protobuf/proto"
)

func main() {
	// Hardcoded file path
	dataPath := "data/instruments_12-12-2024.pb"

	// Initialize KiteConnect for instrument info
	kite := zerodha.NewKiteConnect(nil, nil, nil)
	if err := kite.DownloadInstrumentData(context.Background()); err != nil {
		fmt.Printf("Error downloading instrument data: %v\n", err)
		os.Exit(1)
	}

	// Read the entire file
	data, err := os.ReadFile(dataPath)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	// Try to unmarshal as a batch
	batch := &proto.TickBatch{}
	if err := googleproto.Unmarshal(data, batch); err != nil {
		fmt.Printf("Error unmarshaling data: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nFile size: %d bytes", len(data))
	fmt.Printf("\nTotal ticks in batch: %d\n", len(batch.Ticks))

	if len(batch.Ticks) > 0 {
		firstTick := batch.Ticks[0]
		lastTick := batch.Ticks[len(batch.Ticks)-1]

		// Get instrument info for first tick
		tokenStr := fmt.Sprintf("%d", firstTick.InstrumentToken)
		instrumentInfo, exists := kite.GetInstrumentInfo(tokenStr)

		fmt.Printf("\nInstrument Information:")
		if exists {
			fmt.Printf("\nToken: %s", tokenStr)
			fmt.Printf("\nIndex: %s", instrumentInfo.Index)
			fmt.Printf("\nIs Index: %v", instrumentInfo.IsIndex)
			fmt.Printf("\nIs Options: %v", instrumentInfo.IsOptions)
			if !instrumentInfo.IsIndex {
				fmt.Printf("\nExpiry: %s", instrumentInfo.Expiry.Format("02-Jan-2006"))
			}
		} else {
			fmt.Printf("\nNo instrument info found for token: %s", tokenStr)
		}

		fmt.Printf("\n\nTime range:")
		fmt.Printf("\nFirst tick: %s", time.Unix(firstTick.Timestamp, 0).Format("2006-01-02 15:04:05"))
		fmt.Printf("\nLast tick:  %s", time.Unix(lastTick.Timestamp, 0).Format("2006-01-02 15:04:05"))
		fmt.Printf("\nDuration:   %s\n", time.Unix(lastTick.Timestamp, 0).Sub(time.Unix(firstTick.Timestamp, 0)))

		// Print sample tick details
		fmt.Printf("\nSample tick details (first tick):")
		fmt.Printf("\nInstrument Token: %d", firstTick.InstrumentToken)
		fmt.Printf("\nIs Index: %v", firstTick.IsIndex)
		fmt.Printf("\nOHLC:")
		fmt.Printf("\n  Open:  %.2f", firstTick.Ohlc.Open)
		fmt.Printf("\n  High:  %.2f", firstTick.Ohlc.High)
		fmt.Printf("\n  Low:   %.2f", firstTick.Ohlc.Low)
		fmt.Printf("\n  Close: %.2f", firstTick.Ohlc.Close)

		if len(firstTick.Depth.Buy) > 0 {
			fmt.Printf("\nMarket Depth (Top Buy):")
			fmt.Printf("\n  Price: %.2f", firstTick.Depth.Buy[0].Price)
			fmt.Printf("\n  Quantity: %d", firstTick.Depth.Buy[0].Quantity)
			fmt.Printf("\n  Orders: %d", firstTick.Depth.Buy[0].Orders)
		}
	}

	// Print all ticks with instrument info
	fmt.Printf("\n\nTimestamp\t\t\tToken\t\tInstrument\t\tLast Price\tVolume\tOI\n")
	fmt.Printf("--------------------------------------------------------------------------------------\n")
	for _, tick := range batch.Ticks {
		timestamp := time.Unix(tick.Timestamp, 0).Format("2006-01-02 15:04:05")
		tokenStr := fmt.Sprintf("%d", tick.InstrumentToken)
		instrumentInfo, exists := kite.GetInstrumentInfo(tokenStr)

		instrumentDesc := "Unknown"
		if exists {
			if instrumentInfo.IsIndex {
				instrumentDesc = fmt.Sprintf("%s-IDX", instrumentInfo.Index)
			} else if instrumentInfo.IsOptions {
				instrumentDesc = fmt.Sprintf("%s-OPT", instrumentInfo.Index)
			} else {
				instrumentDesc = fmt.Sprintf("%s-FUT", instrumentInfo.Index)
			}
		}

		fmt.Printf("%s\t%d\t%-20s\t%.2f\t\t%d\t%.0f\n",
			timestamp,
			tick.InstrumentToken,
			instrumentDesc,
			tick.LastPrice,
			tick.VolumeTraded,
			tick.Oi)
	}
}
