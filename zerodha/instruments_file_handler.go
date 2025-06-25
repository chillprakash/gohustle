package zerodha

import (
	"context"
	"fmt"
	"gohustle/core"
	"gohustle/filestore"
	"gohustle/logger"
	"gohustle/utils"
	"sync"
	"time"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
	"google.golang.org/protobuf/proto"
)

// InstrumentsFileHandler handles operations related to instrument data files
type InstrumentsFileHandler struct {
	kite       *kiteconnect.Client
	fileStore  *filestore.DiskFileStore
	filePrefix string
}

var (
	instrumentsFileHandlerInstance *InstrumentsFileHandler
	instrumentsFileHandlerOnce     sync.Once
	instrumentsFileHandlerMu       sync.RWMutex
)

// GetInstrumentsFileHandler returns a singleton instance of InstrumentsFileHandler
func GetInstrumentsFileHandler(kite *kiteconnect.Client) *InstrumentsFileHandler {
	instrumentsFileHandlerMu.RLock()
	if instrumentsFileHandlerInstance != nil {
		defer instrumentsFileHandlerMu.RUnlock()
		return instrumentsFileHandlerInstance
	}
	instrumentsFileHandlerMu.RUnlock()

	instrumentsFileHandlerMu.Lock()
	defer instrumentsFileHandlerMu.Unlock()

	instrumentsFileHandlerOnce.Do(func() {
		instrumentsFileHandlerInstance = &InstrumentsFileHandler{
			kite:       kite,
			fileStore:  filestore.NewDiskFileStore(),
			filePrefix: "instruments",
		}
	})

	return instrumentsFileHandlerInstance
}

// NewInstrumentsFileHandler creates a new InstrumentsFileHandler (for backward compatibility)
func NewInstrumentsFileHandler(kite *kiteconnect.Client) *InstrumentsFileHandler {
	return GetInstrumentsFileHandler(kite)
}

// DownloadInstrumentData downloads and saves instrument data
func (h *InstrumentsFileHandler) DownloadInstrumentDataFromZerodha(ctx context.Context, instrumentNames []core.Index) error {
	log := logger.L()
	currentDate := utils.GetCurrentKiteDate()

	// Check if file already exists for today
	exists := h.fileStore.FileExists(h.filePrefix, currentDate)
	if exists {
		log.Info("Instruments file already exists for today, skipping download", map[string]interface{}{
			"date": currentDate,
		})
		return nil
	}

	// Get all instruments
	allInstruments, err := h.kite.GetInstruments()
	if err != nil {
		log.Error("Failed to download instruments", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Filter instruments
	filteredInstruments := filterInstruments(allInstruments, instrumentNames)

	log.Info("Filtered instruments", map[string]interface{}{
		"total_count":    len(allInstruments),
		"filtered_count": len(filteredInstruments),
		"instruments":    instrumentNames,
	})

	// Save filtered data
	return h.saveInstrumentsToFile(filteredInstruments)
}

// saveInstrumentsToFile saves instruments to a file
func (h *InstrumentsFileHandler) saveInstrumentsToFile(instruments []kiteconnect.Instrument) error {
	log := logger.L()
	currentDate := utils.GetCurrentKiteDate()

	// Convert to proto message
	data, err := proto.Marshal(convertToProtoInstruments(instruments))
	if err != nil {
		log.Error("Failed to marshal instruments", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Save using filestore
	if err := h.fileStore.SaveGzippedProto(h.filePrefix, currentDate, data); err != nil {
		log.Error("Failed to save instruments", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	return nil
}

// FetchInstrumentsFromFilteredList retrieves complete instrument data for a list of filtered instrument symbols
func (h *InstrumentsFileHandler) FetchInstrumentsFromFilteredList(ctx context.Context, filteredInstruments []string) ([]*Instrument, error) {
	log := logger.L()
	currentDate := utils.GetCurrentKiteDate()

	// Read the gzipped data
	data, err := h.fileStore.ReadGzippedProto(h.filePrefix, currentDate)
	if err != nil {
		log.Error("Failed to read instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to read instrument data: %w", err)
	}

	// Unmarshal the proto message
	var instruments InstrumentList
	if err := proto.Unmarshal(data, &instruments); err != nil {
		log.Error("Failed to unmarshal instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to unmarshal instrument data: %w", err)
	}

	// Create a map for quick lookup of filtered instruments
	filteredMap := make(map[string]bool)
	for _, symbol := range filteredInstruments {
		filteredMap[symbol] = true
	}

	// Filter instruments based on trading symbol
	result := make([]*Instrument, 0)
	for _, inst := range instruments.Instruments {
		if filteredMap[inst.Tradingsymbol] {
			result = append(result, inst)
		}
	}

	log.Info("Fetched instruments from filtered list", map[string]interface{}{
		"input_count":  len(filteredInstruments),
		"output_count": len(result),
	})

	return result, nil
}

// ReadInstrumentExpiriesFromFile reads instrument expiries from file
func (h *InstrumentsFileHandler) ReadInstrumentExpiriesFromFile() (map[string][]time.Time, error) {
	log := logger.L()
	currentDate := utils.GetCurrentKiteDate()

	// Read the gzipped data
	data, err := h.fileStore.ReadGzippedProto(h.filePrefix, currentDate)
	if err != nil {
		log.Error("Failed to read instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to read instrument data: %w", err)
	}

	// Unmarshal the proto message
	var instruments InstrumentList
	if err := proto.Unmarshal(data, &instruments); err != nil {
		log.Error("Failed to unmarshal instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to unmarshal instrument data: %w", err)
	}

	// Extract expiry dates
	expiryMap := make(map[string]map[time.Time]bool)
	for _, inst := range instruments.Instruments {
		// Skip if no expiry
		if inst.Expiry == "" {
			continue
		}

		// Parse expiry date
		expiry, err := time.Parse("2006-01-02", inst.Expiry)
		if err != nil {
			log.Error("Failed to parse expiry date", map[string]interface{}{
				"error":         err.Error(),
				"instrument":    inst.Name,
				"tradingsymbol": inst.Tradingsymbol,
				"expiry":        inst.Expiry,
			})
			continue
		}

		// Skip invalid dates (year before 2024)
		if expiry.Year() < 2024 {
			continue
		}

		// Add expiry to map
		if expiryMap[inst.Name] == nil {
			expiryMap[inst.Name] = make(map[time.Time]bool)
		}
		expiryMap[inst.Name][expiry] = true
	}

	result := convertExpiryMapToSortedSlices(expiryMap)

	return result, nil
}
