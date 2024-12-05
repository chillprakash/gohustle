package zerodha

import (
	"context"
	"fmt"
	"sort"
	"time"

	"gohustle/logger"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
	"google.golang.org/protobuf/proto"
)

// DownloadInstrumentData downloads and saves instrument data
func (k *KiteConnect) DownloadInstrumentData(ctx context.Context) error {
	log := logger.GetLogger()

	// Define instrument names we're interested in
	instrumentNames := []string{"NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "SENSEX", "BANKEX"}

	// Get all instruments
	allInstruments, err := k.Kite.GetInstruments()
	if err != nil {
		log.Error("Failed to download instruments", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	log.Info("Downloaded raw data", map[string]interface{}{
		"total_count": len(allInstruments),
	})

	// Filter instruments
	filteredInstruments := filterInstruments(allInstruments, instrumentNames)

	log.Info("Filtered instruments", map[string]interface{}{
		"total_count":    len(allInstruments),
		"filtered_count": len(filteredInstruments),
		"instruments":    instrumentNames,
	})

	// Save filtered instruments
	return k.saveInstrumentsToFile(ctx, filteredInstruments)
}

func (k *KiteConnect) SyncInstrumentExpiriesFromFileToDB(ctx context.Context) error {
	log := logger.GetLogger()

	// Read expiries from file
	expiries, err := k.readInstrumentExpiriesFromFile(ctx)
	if err != nil {
		log.Error("Failed to read expiries from file", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to read expiries: %w", err)
	}

	// Save expiries to DB
	if err := k.saveInstrumentExpiriesToDB(ctx, expiries); err != nil {
		log.Error("Failed to save expiries to DB", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to save expiries: %w", err)
	}

	log.Info("Successfully synced expiries from file to DB", map[string]interface{}{
		"instruments_count": len(expiries),
	})

	return nil
}

// GetInstrumentExpiries reads the gzipped instrument data and returns expiry dates
func (k *KiteConnect) readInstrumentExpiriesFromFile(ctx context.Context) (map[string][]time.Time, error) {
	log := logger.GetLogger()
	currentDate := time.Now().Format("02-01-2006")

	// Read the gzipped data
	data, err := k.fileStore.ReadGzippedProto("instruments", currentDate)
	if err != nil {
		log.Error("Failed to read instrument data", map[string]interface{}{
			"error": err.Error(),
			"date":  currentDate,
		})
		return nil, err
	}

	// Unmarshal the protobuf data
	instrumentList := &InstrumentList{}
	if err := proto.Unmarshal(data, instrumentList); err != nil {
		log.Error("Failed to unmarshal instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	// Map to store unique expiries for each instrument
	expiryMap := make(map[string]map[time.Time]bool)

	// Process each instrument
	for _, inst := range instrumentList.Instruments {
		// Parse expiry date
		expiry, err := time.Parse("02-01-2006", inst.Expiry)
		if err != nil {
			log.Error("Failed to parse expiry date", map[string]interface{}{
				"error":         err.Error(),
				"instrument":    inst.Name,
				"tradingsymbol": inst.Tradingsymbol,
				"expiry":        inst.Expiry,
			})
			continue
		}

		// Add expiry to map
		if expiryMap[inst.Name] == nil {
			expiryMap[inst.Name] = make(map[time.Time]bool)
		}
		expiryMap[inst.Name][expiry] = true
	}

	return convertExpiryMapToSortedSlices(expiryMap), nil
}

// SaveInstrumentExpiries saves the expiry dates to the database
func (k *KiteConnect) saveInstrumentExpiriesToDB(ctx context.Context, expiries map[string][]time.Time) error {
	log := logger.GetLogger()

	// First, mark all existing records as inactive
	deactivateQuery := `
		UPDATE instrument_expiries 
		SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP 
		WHERE is_active = TRUE
	`
	if _, err := k.db.Exec(ctx, deactivateQuery); err != nil {
		log.Error("Failed to deactivate existing expiries", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Insert new records
	insertQuery := `
		INSERT INTO instrument_expiries (instrument_name, expiry_date)
		VALUES ($1, $2)
		ON CONFLICT (instrument_name, expiry_date)
		DO UPDATE SET 
			is_active = TRUE,
			updated_at = CURRENT_TIMESTAMP
	`

	for instrument, dates := range expiries {
		for _, date := range dates {
			if _, err := k.db.Exec(ctx, insertQuery, instrument, date); err != nil {
				log.Error("Failed to insert expiry", map[string]interface{}{
					"error":      err.Error(),
					"instrument": instrument,
					"expiry":     date,
				})
				return err
			}
		}

		log.Info("Saved expiries for instrument", map[string]interface{}{
			"instrument": instrument,
			"count":      len(dates),
		})
	}

	return nil
}

func convertExpiryMapToSortedSlices(expiryMap map[string]map[time.Time]bool) map[string][]time.Time {
	result := make(map[string][]time.Time)

	for name, uniqueExpiries := range expiryMap {
		expiries := make([]time.Time, 0, len(uniqueExpiries))
		for expiry := range uniqueExpiries {
			expiries = append(expiries, expiry)
		}

		// Sort expiries
		sort.Slice(expiries, func(i, j int) bool {
			return expiries[i].Before(expiries[j])
		})

		result[name] = expiries
	}

	return result
}

// formatExpiries formats expiry dates for logging
func formatExpiries(expiries []time.Time) []string {
	formatted := make([]string, len(expiries))
	for i, expiry := range expiries {
		formatted[i] = expiry.Format("02-Jan-2006")
	}
	return formatted
}

func filterInstruments(allInstruments []kiteconnect.Instrument, targetNames []string) []kiteconnect.Instrument {
	filtered := make([]kiteconnect.Instrument, 0)
	for _, inst := range allInstruments {
		for _, name := range targetNames {
			if inst.Name == name {
				filtered = append(filtered, inst)
				break
			}
		}
	}
	return filtered
}

func (k *KiteConnect) saveInstrumentsToFile(ctx context.Context, instruments []kiteconnect.Instrument) error {
	log := logger.GetLogger()
	currentDate := time.Now().Format("02-01-2006")

	// Convert to proto message
	data, err := proto.Marshal(convertToProtoInstruments(instruments))
	if err != nil {
		log.Error("Failed to marshal instruments", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Save using filestore
	if err := k.fileStore.SaveGzippedProto("instruments", currentDate, data); err != nil {
		log.Error("Failed to save instruments", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	return nil
}

func convertToProtoInstruments(instruments []kiteconnect.Instrument) *InstrumentList {
	protoInstruments := make([]*Instrument, 0, len(instruments))

	for _, inst := range instruments {
		protoInst := &Instrument{
			InstrumentToken: fmt.Sprintf("%d", inst.InstrumentToken),
			ExchangeToken:   fmt.Sprintf("%d", inst.ExchangeToken),
			Tradingsymbol:   inst.Tradingsymbol,
			Name:            inst.Name,
			LastPrice:       fmt.Sprintf("%.2f", inst.LastPrice),
			Expiry:          inst.Expiry.Time.Format("02-01-2006"),
			StrikePrice:     fmt.Sprintf("%.2f", inst.StrikePrice),
			TickSize:        fmt.Sprintf("%.2f", inst.TickSize),
			LotSize:         fmt.Sprintf("%d", int(inst.LotSize)),
			InstrumentType:  inst.InstrumentType,
			Segment:         inst.Segment,
			Exchange:        inst.Exchange,
		}
		protoInstruments = append(protoInstruments, protoInst)
	}

	return &InstrumentList{
		Instruments: protoInstruments,
	}
}
