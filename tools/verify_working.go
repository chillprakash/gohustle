package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gohustle/zerodha"

	googleproto "google.golang.org/protobuf/proto"
)

const DefaultDataPath = "data"

func main() {
	// Read all .pb.gz files from data directory
	pattern := filepath.Join(DefaultDataPath, "*.pb.gz")
	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Printf("Error reading directory: %v\n", err)
		return
	}

	if len(files) == 0 {
		fmt.Printf("No .pb.gz files found in %s\n", DefaultDataPath)
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

		// Open and decompress gzipped file
		f, err := os.Open(file)
		if err != nil {
			fmt.Printf("Error opening file: %v\n", err)
			continue
		}
		defer f.Close()

		gz, err := gzip.NewReader(f)
		if err != nil {
			fmt.Printf("Error creating gzip reader: %v\n", err)
			continue
		}
		defer gz.Close()

		data, err := io.ReadAll(gz)
		if err != nil {
			fmt.Printf("Error reading gzipped data: %v\n", err)
			continue
		}

		// Unmarshal protobuf
		instrumentList := &zerodha.InstrumentList{}
		if err := googleproto.Unmarshal(data, instrumentList); err != nil {
			fmt.Printf("Error unmarshaling data: %v\n", err)
			continue
		}

		fmt.Printf("\nTotal instruments: %d\n", len(instrumentList.Instruments))

		// Print all indices
		fmt.Printf("\nAll INDICES instruments:\n")
		for _, inst := range instrumentList.Instruments {
			if inst.Segment == "INDICES" {
				fmt.Printf("Index: %s\n", inst.Tradingsymbol)
			}
		}

		fmt.Printf("\nDetailed NIFTY 50 instruments:\n")
		for _, inst := range instrumentList.Instruments {
			if inst.Name == "SENSEX" {
				fmt.Printf("\nNIFTY 50 Details:\n")
				fmt.Printf("  Symbol: %s\n", inst.Tradingsymbol)
				fmt.Printf("  Token: %s\n", inst.InstrumentToken)
				fmt.Printf("  Exchange: %s\n", inst.Exchange)
				fmt.Printf("  Segment: %s\n", inst.Segment)
				fmt.Printf("  Type: %s\n", inst.InstrumentType)
				fmt.Printf("  Name: %s\n", inst.Name)
				fmt.Printf("  Expiry: %s\n", inst.Expiry)
				fmt.Printf("  Strike: %f\n", inst.StrikePrice)
				fmt.Printf("  Tick Size: %f\n", inst.TickSize)
				fmt.Printf("  Lot Size: %d\n", inst.LotSize)
			}
		}

	}
}
