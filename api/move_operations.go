package api

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gohustle/cache"
	"gohustle/db"
	"gohustle/logger"
	"gohustle/zerodha"
)

// StrikeInfo contains information about an option strike
type StrikeInfo struct {
	Strike         float64
	InstrumentType string // CE or PE
	Expiry         string
	TradingSymbol  string
}

// handleMoveOperation processes move_away, move_closer, and exit operations for options
func (s *Server) handleMoveOperation(w http.ResponseWriter, r *http.Request, req *PlaceOrderAPIRequest) {
	// Get the KiteConnect instance
	kc := zerodha.GetKiteConnect()
	if kc == nil {
		sendErrorResponse(w, "Zerodha connection not available", http.StatusInternalServerError)
		return
	}

	cacheMetaInstance, err := cache.GetCacheMetaInstance()
	if err != nil {
		s.log.Error("Failed to get cache meta instance for move", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, fmt.Sprintf("Failed to get cache meta instance: %v", err), http.StatusInternalServerError)
		return
	}

	// 1. Extract strike, expiry, and instrument type from the trading symbol or token
	tokenMetaData, err := cacheMetaInstance.GetMetadataOfToken(r.Context(), req.InstrumentToken)
	if err != nil {
		s.log.Error("Failed to extract strike info", map[string]interface{}{
			"error":            err.Error(),
			"instrument_token": req.InstrumentToken,
			"trading_symbol":   req.TradingSymbol,
		})
		sendErrorResponse(w, fmt.Sprintf("Failed to extract strike info: %v", err), http.StatusBadRequest)
		return
	}

	s.log.Info("Extracted strike info", map[string]interface{}{
		"strike":          tokenMetaData.StrikePrice,
		"instrument_type": tokenMetaData.InstrumentType,
		"expiry":          tokenMetaData.Expiry,
		"trading_symbol":  tokenMetaData.TradingSymbol,
	})

	// 2. Find the current position for this instrument
	currentPosition, err := findCurrentPosition(r.Context(), tokenMetaData.TradingSymbol)
	if err != nil {
		s.log.Error("Failed to find current position", map[string]interface{}{
			"error":          err.Error(),
			"trading_symbol": tokenMetaData.TradingSymbol,
		})
		sendErrorResponse(w, fmt.Sprintf("Failed to find current position: %v", err), http.StatusBadRequest)
		return
	}

	if currentPosition == nil {
		sendErrorResponse(w, "Position not found", http.StatusNotFound)
		return
	}

	s.log.Info("Found current position", map[string]interface{}{
		"position_id":    currentPosition.ID,
		"trading_symbol": currentPosition.TradingSymbol,
		"quantity":       currentPosition.Quantity,
		"paper_trading":  currentPosition.PaperTrading,
	})

	// Fetch indexName earlier and pass to calculateQuantityToProcess
	// 1. Get Instrument Token from Trading Symbol
	instrumentTokenValue, tokenFound := cacheMetaInstance.GetInstrumentTokenForSymbol(r.Context(), currentPosition.TradingSymbol)
	if !tokenFound {
		s.log.Error("Could not find instrument token for position in cache", map[string]interface{}{ // Log token not found
			"trading_symbol": currentPosition.TradingSymbol,
		})
		// Cannot proceed without token to determine lot size reliably
		sendErrorResponse(w, "Failed to retrieve instrument details for quantity calculation", http.StatusInternalServerError)
		return
	}
	instrumentTokenStr, ok := instrumentTokenValue.(string)
	if !ok {
		s.log.Error("Instrument token retrieved from cache is not a string", map[string]interface{}{ // Log invalid token type
			"trading_symbol": currentPosition.TradingSymbol,
			"token_value":    instrumentTokenValue,
		})
		sendErrorResponse(w, "Internal error processing instrument details", http.StatusInternalServerError)
		return
	}

	// 2. Get Index Name from Instrument Token
	indexName, indexFound := zerodha.GetIndexFromInstrumentToken(r.Context(), instrumentTokenStr)

	if !indexFound {
		// Attempt to guess from trading symbol as a fallback
		if strings.Contains(currentPosition.TradingSymbol, "NIFTY") {
			indexName = "NIFTY"
		} else if strings.Contains(currentPosition.TradingSymbol, "BANKNIFTY") {
			indexName = "BANKNIFTY"
		} else if strings.Contains(currentPosition.TradingSymbol, "SENSEX") {
			indexName = "SENSEX"
		} else {
			// Log error and potentially default or return error if index cannot be determined
			s.log.Error("Could not determine index name for lot size calculation", map[string]interface{}{ // Log index not found/guessed
				"instrument_token": instrumentTokenStr,
				"trading_symbol":   currentPosition.TradingSymbol,
			})
			// Defaulting to NIFTY as a last resort, but this might be inaccurate
			indexName = "NIFTY"
		}
		indexFound = true // Proceed with the guessed/default name
	}

	// 3. Calculate the quantity to process based on the fraction
	toProcessQuantity, err := calculateQuantityToProcess(currentPosition.Quantity, req.QuantityFrac, tokenMetaData.InstrumentType, indexName)
	if err != nil {
		s.log.Error("Failed to calculate quantity to process", map[string]interface{}{ // Log quantity calculation failure
			"error":            err.Error(),
			"instrument_token": req.InstrumentToken,
			"trading_symbol":   req.TradingSymbol,
		})
		sendErrorResponse(w, fmt.Sprintf("Failed to calculate quantity to process: %v", err), http.StatusBadRequest)
		return
	}

	if toProcessQuantity == 0 {
		sendErrorResponse(w, "Calculated quantity to process is zero, cannot proceed", http.StatusBadRequest)
		return
	}

	s.log.Info("Calculated quantity to process", map[string]interface{}{
		"original_quantity": currentPosition.Quantity,
		"fraction":          req.QuantityFrac,
		"to_process":        toProcessQuantity,
	})

	// 4. Handle the requested operation
	switch req.MoveType {
	case "exit":
		// Exit operations handle their own response
		s.handleExitOperation(w, r, req)
		return // Exit early as handleExitOperation handles its own response
	case "move_away", "move_closer":
		err = s.processMoveOperation(w, r, req, currentPosition, toProcessQuantity, tokenMetaData)
	default:
		sendErrorResponse(w, "Invalid move type", http.StatusBadRequest)
		return
	}

	if err != nil {
		s.log.Error("Failed to handle move operation", map[string]interface{}{
			"error":       err.Error(),
			"move_type":   req.MoveType,
			"position_id": currentPosition.ID,
		})
		sendErrorResponse(w, fmt.Sprintf("Failed to handle move operation: %v", err), http.StatusInternalServerError)
		return
	}

	// Send a success response
	// Lot size is already determined by the indexName fetched earlier
	lotSize := getLotSize(indexName)
	finalQuantity := int(math.Abs(float64(toProcessQuantity))) // toProcessQuantity is already lot-adjusted
	finalLots := 0
	if lotSize > 0 {
		finalLots = finalQuantity / lotSize
	} else {
		s.log.Error("Lot size is zero or negative, cannot calculate lots for response", map[string]interface{}{
			"index_name": indexName,
			"lot_size":   lotSize,
		})
	}

	s.log.Info("Final response values", map[string]interface{}{ // Adjusted log
		"final_quantity": finalQuantity,
		"lot_size":       lotSize,
		"final_lots":     finalLots,
	})

	// Create a success response
	response := map[string]interface{}{
		"status":  "success",
		"message": fmt.Sprintf("%s operation completed successfully", req.MoveType),
		"data": map[string]interface{}{ // Ensure this map uses the correct values
			"move_type":      string(req.MoveType),
			"trading_symbol": currentPosition.TradingSymbol,
			"quantity":       finalQuantity, // Use absolute quantity
			"position_id":    currentPosition.ID,
			"lots":           finalLots,
			"paper_trading":  currentPosition.PaperTrading,
		},
	}

	// Send the response as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// extractStrikeInfo extracts the strike price, instrument type, and expiry from the trading symbol or token
func extractStrikeInfo(ctx context.Context, req *PlaceOrderAPIRequest) (*StrikeInfo, error) {
	log := logger.L()
	log.Info("Starting extractStrikeInfo", map[string]interface{}{
		"instrument_token": req.InstrumentToken,
		"trading_symbol":   req.TradingSymbol,
	})

	cacheMetaInstance, err := cache.GetCacheMetaInstance()
	if err != nil {
		log.Error("Failed to get cache meta instance", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to get cache meta instance: %w", err)
	}

	// We need an instrument token to fetch the information
	if req.InstrumentToken == "" {
		// If we only have a trading symbol, try to get the instrument token
		if req.TradingSymbol != "" {
			log.Info("Getting instrument token from trading symbol", map[string]interface{}{
				"trading_symbol": req.TradingSymbol,
			})
			token, found := cacheMetaInstance.GetInstrumentTokenForSymbol(ctx, req.TradingSymbol)
			if !found {
				log.Error("Failed to get instrument token for trading symbol", map[string]interface{}{
					"trading_symbol": req.TradingSymbol,
				})
				return nil, fmt.Errorf("failed to get instrument token for trading symbol: %s", req.TradingSymbol)
			}
			req.InstrumentToken = token.(string)
		} else {
			log.Error("Neither instrument token nor trading symbol provided", nil)
			return nil, fmt.Errorf("neither instrument token nor trading symbol provided")
		}
	}

	instrumentMetadata, err := cacheMetaInstance.GetMetadataOfToken(ctx, req.InstrumentToken)
	if err != nil {
		log.Error("Failed to get instrument metadata for token", map[string]interface{}{
			"instrument_token": req.InstrumentToken,
		})
		return nil, fmt.Errorf("failed to get instrument metadata for token: %s", req.InstrumentToken)
	}

	log.Info("Got instrument metadata for token", map[string]interface{}{
		"instrument_token": req.InstrumentToken,
		"instrument_type":  instrumentMetadata.InstrumentType,
	})

	// Parse the strike price
	strikePrice, err := strconv.ParseFloat(instrumentMetadata.StrikePrice, 64)
	if err != nil {
		log.Error("Failed to parse strike price", map[string]interface{}{
			"strike_str": instrumentMetadata.StrikePrice,
			"error":      err.Error(),
		})
		return nil, fmt.Errorf("failed to parse strike price: %w", err)
	}

	strikeInfo := &StrikeInfo{
		Strike:         strikePrice,
		InstrumentType: instrumentMetadata.InstrumentType,
		Expiry:         instrumentMetadata.Expiry,
		TradingSymbol:  instrumentMetadata.TradingSymbol,
	}

	log.Info("Successfully created StrikeInfo", map[string]interface{}{
		"strike":          strikeInfo.Strike,
		"instrument_type": strikeInfo.InstrumentType,
		"expiry":          strikeInfo.Expiry,
		"trading_symbol":  strikeInfo.TradingSymbol,
	})

	return strikeInfo, nil
}

// Note: We've removed the extractStrikeInfoFromSymbol function as we now use the market_data.go methods
// to get the instrument type, expiry, and strike price directly from the cache

// findCurrentPosition finds the current position for the given instrument
func findCurrentPosition(ctx context.Context, tradingSymbol string) (*db.PositionRecord, error) {
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return nil, fmt.Errorf("database connection not available")
	}

	// First try to find by trading symbol (most efficient)
	if tradingSymbol != "" {
		position, err := timescaleDB.GetPositionByTradingSymbol(ctx, tradingSymbol)
		if err != nil {
			return nil, fmt.Errorf("failed to get position by trading symbol: %w", err)
		}
		if position != nil {
			return position, nil
		}
	}

	// If still not found, return an error
	return nil, fmt.Errorf("no position found for trading symbol '%s'", tradingSymbol)
}

// calculateQuantityToProcess calculates the quantity to move/exit based on fraction and ensures it's a multiple of lot size.
// It returns the quantity with the correct sign (negative for SELL, positive for BUY).
func calculateQuantityToProcess(currentQuantity int, fraction QuantityFraction, instrumentType string, indexName string) (int, error) {
	log := logger.L()

	if currentQuantity == 0 {
		log.Error("Current position quantity is zero", map[string]interface{}{
			"instrument_type": instrumentType,
			"index_name":      indexName,
		})
		return 0, fmt.Errorf("current position quantity is zero")
	}

	// Get the absolute quantity
	absQuantity := int(math.Abs(float64(currentQuantity)))
	log.Info("Calculating quantity to process", map[string]interface{}{
		"current_quantity": currentQuantity,
		"abs_quantity":     absQuantity,
		"fraction":         fraction,
		"instrument_type":  instrumentType,
		"index_name":       indexName,
	})

	// Get the lot size for this instrument
	lotSize := getLotSize(indexName)
	if lotSize <= 0 {
		log.Error("Invalid lot size calculated", map[string]interface{}{ // Log error
			"instrument_type": instrumentType,
			"index_name":      indexName,
			"lot_size":        lotSize,
		})
		return 0, fmt.Errorf("invalid lot size %d for instrument type %s", lotSize, instrumentType)
	}
	log.Info("Got lot size for instrument", map[string]interface{}{
		"instrument_type": instrumentType,
		"index_name":      indexName,
		"lot_size":        lotSize,
	})

	// Calculate in terms of lots rather than individual contracts
	// This is the key change to ensure proper lot-based splitting
	totalLots := absQuantity / lotSize
	if absQuantity%lotSize != 0 {
		log.Error("Current position quantity is not a multiple of lot size", map[string]interface{}{
			"current_quantity": currentQuantity,
			"lot_size":         lotSize,
			"remainder":        absQuantity % lotSize,
		})

		// Round to the nearest number of lots
		totalLots = int(math.Round(float64(absQuantity) / float64(lotSize)))
		log.Info("Rounded to nearest number of lots", map[string]interface{}{
			"original_quantity": absQuantity,
			"total_lots":        totalLots,
			"lot_size":          lotSize,
		})
	}

	// Determine the fraction value based on the QuantityFraction
	var fractionValue float64
	switch fraction {
	case "0.5", "half":
		fractionValue = 0.5
	case "0.25", "quarter":
		fractionValue = 0.25
	case "1":
		fractionValue = 1.0
	default:
		log.Error("Invalid fraction specified", map[string]interface{}{
			"fraction": fraction,
		})
		return 0, fmt.Errorf("invalid quantity fraction: %s", fraction)
	}

	// Calculate based on lots first
	lotsToProcess := int(math.Round(float64(totalLots) * fractionValue))
	log.Info("Calculated lots to process based on fraction", map[string]interface{}{ // Log lots to process
		"total_lots":    totalLots,
		"fraction":      fraction,
		"fractionValue": fractionValue,
		"lotsToProcess": lotsToProcess,
	})

	// Ensure we process at least one lot
	if lotsToProcess == 0 {
		lotsToProcess = 1
		log.Info("Adjusted lots to process to minimum 1 lot", map[string]interface{}{}) // Log adjustment
	}

	// Convert calculated lots back to quantity
	toProcess := lotsToProcess * lotSize
	log.Info("Converted lots to contracts", map[string]interface{}{ // Log conversion
		"lots_to_process": lotsToProcess,
		"lot_size":        lotSize,
		"to_process":      toProcess,
	})

	// Check if the quantity is below the minimum lot size
	if toProcess < lotSize {
		log.Info("Quantity below minimum lot size, cannot process", map[string]interface{}{
			"calculated_quantity": toProcess,
			"minimum_lot_size":    lotSize,
		})
		return 0, fmt.Errorf("quantity below minimum lot size")
	}

	// Preserve the sign of the original position
	// This is important for move operations where we want to maintain direction
	// For exit operations, the sign will be flipped later in handleExitOperation
	if currentQuantity < 0 {
		toProcess = -toProcess
	}

	log.Info("Final quantity to process", map[string]interface{}{
		"quantity":        toProcess,
		"original":        currentQuantity,
		"lots":            lotsToProcess,
		"fraction":        fraction,
		"instrument_type": instrumentType,
		"index_name":      indexName,
	})

	return toProcess, nil
}

// calculateExitQuantity calculates the quantity required to exit the entire position.
func calculateExitQuantity(currentQuantity int, instrumentType string, indexName string) (int, error) {
	log := logger.L()
	lotSize := getLotSize(indexName)
	if lotSize <= 0 {
		log.Error("Invalid lot size calculated for exit", map[string]interface{}{ // Log invalid lot size
			"instrument_type": instrumentType,
			"index_name":      indexName,
			"lot_size":        lotSize,
		})
		return 0, fmt.Errorf("invalid lot size %d for instrument type %s", lotSize, instrumentType)
	}
	log.Info("Got lot size for exit", map[string]interface{}{
		"instrument_type": instrumentType,
		"index_name":      indexName,
		"lot_size":        lotSize,
	})

	// Calculate the quantity to exit - must be the opposite of current position
	absQuantity := int(math.Abs(float64(currentQuantity)))

	// Check if the quantity is a multiple of lot size
	if absQuantity%lotSize != 0 {
		log.Error("Current position quantity is not a multiple of lot size for exit", map[string]interface{}{
			"current_quantity": currentQuantity,
			"lot_size":         lotSize,
			"remainder":        absQuantity % lotSize,
		})

		// Round to the nearest number of lots
		absQuantity = int(math.Round(float64(absQuantity)/float64(lotSize))) * lotSize
		log.Info("Rounded to nearest number of lots for exit", map[string]interface{}{
			"original_quantity": absQuantity,
			"adjusted_quantity": absQuantity,
			"lot_size":          lotSize,
		})
	}

	// For exit, we need the OPPOSITE sign of the current position
	exitQuantity := absQuantity
	if currentQuantity > 0 {
		// If position is positive (long), we need to SELL, so quantity should be negative
		exitQuantity = -exitQuantity
	} else if currentQuantity < 0 {
		// If position is negative (short), we need to BUY, so quantity should be positive
		// exitQuantity is already positive here
	}

	log.Info("Final exit quantity", map[string]interface{}{
		"exit_quantity":   exitQuantity,
		"original":        currentQuantity,
		"instrument_type": instrumentType,
		"index_name":      indexName,
	})

	return exitQuantity, nil
}

// getLotSize returns the lot size for the given instrument type
func getLotSize(indexName string) int {
	// This is a simplified implementation - it should ideally fetch from a config or cache
	log := logger.L()
	log.Info("Getting lot size", map[string]interface{}{ // Log getting lot size
		"index_name": indexName,
	})
	if strings.Contains(indexName, "BANK") {
		return 20 // Bank Nifty lot size
	} else if strings.Contains(indexName, "SENSEX") {
		return 20 // Sensex lot size
	} else if strings.Contains(indexName, "NIFTY") {
		return 75 // Nifty lot size
	}

	// Default lot size
	return 1
}

// handleExitOperation handles the exit operation for a position
func (s *Server) handleExitOperation(w http.ResponseWriter, r *http.Request, req *PlaceOrderAPIRequest) {
	// Get the KiteConnect instance
	kc := zerodha.GetKiteConnect()
	if kc == nil {
		sendErrorResponse(w, "Zerodha connection not available", http.StatusInternalServerError)
		return
	}

	cacheMetaInstance, err := cache.GetCacheMetaInstance()
	if err != nil {
		s.log.Error("Failed to get cache meta instance for exit", map[string]interface{}{
			"error": err.Error(),
		})
		sendErrorResponse(w, fmt.Sprintf("Failed to get cache meta instance: %v", err), http.StatusInternalServerError)
		return
	}

	tokenMetaData, _ := cacheMetaInstance.GetMetadataOfToken(r.Context(), req.InstrumentToken)

	// 2. Find the current position for the instrument
	currentPosition, err := findCurrentPosition(r.Context(), tokenMetaData.TradingSymbol)
	if err != nil {
		s.log.Error("Failed to find current position for exit", map[string]interface{}{
			"error":          err.Error(),
			"trading_symbol": tokenMetaData.TradingSymbol,
		})
		sendErrorResponse(w, fmt.Sprintf("Failed to find current position: %v", err), http.StatusBadRequest)
		return
	}

	if currentPosition == nil {
		sendErrorResponse(w, "Position not found", http.StatusNotFound)
		return
	}

	s.log.Info("Found position to exit", map[string]interface{}{
		"position_id":      currentPosition.ID,
		"trading_symbol":   currentPosition.TradingSymbol,
		"current_quantity": currentPosition.Quantity,
		"paper_trading":    currentPosition.PaperTrading,
	})

	// 3. Get Instrument Token from Trading Symbol
	instrumentTokenValue, tokenFound := cacheMetaInstance.GetInstrumentTokenForSymbol(r.Context(), currentPosition.TradingSymbol)
	if !tokenFound {
		s.log.Error("Could not find instrument token for position in cache for exit", map[string]interface{}{
			"trading_symbol": currentPosition.TradingSymbol,
		})
		sendErrorResponse(w, "Failed to retrieve instrument details for exit", http.StatusInternalServerError)
		return
	}
	instrumentTokenStr, ok := instrumentTokenValue.(string)
	if !ok {
		s.log.Error("Instrument token retrieved from cache is not a string for exit", map[string]interface{}{
			"trading_symbol": currentPosition.TradingSymbol,
			"token_value":    instrumentTokenValue,
		})
		sendErrorResponse(w, "Internal error processing instrument details for exit", http.StatusInternalServerError)
		return
	}

	indexName := tokenMetaData.InstrumentType

	// 5. Calculate exit quantity based on fraction
	var exitQuantity int

	// Check if a specific fraction was requested
	if req.QuantityFrac == "0.5" || req.QuantityFrac == "0.25" {
		// Calculate the absolute quantity to exit based on fraction
		absQuantity := int(math.Abs(float64(currentPosition.Quantity)))
		lotSize := getLotSize(indexName)

		// Convert to lots
		totalLots := absQuantity / lotSize
		if absQuantity%lotSize != 0 {
			totalLots = int(math.Round(float64(absQuantity) / float64(lotSize)))
		}

		// Apply fraction to lots
		var fractionValue float64
		if req.QuantityFrac == "0.5" {
			fractionValue = 0.5
		} else { // "0.25"
			fractionValue = 0.25
		}

		lotsToExit := int(math.Round(float64(totalLots) * fractionValue))
		if lotsToExit == 0 {
			lotsToExit = 1 // Minimum 1 lot
		}

		// Convert back to quantity
		exitQuantity = lotsToExit * lotSize

		s.log.Info("Calculated partial exit quantity", map[string]interface{}{
			"fraction":      req.QuantityFrac,
			"total_lots":    totalLots,
			"lots_to_exit":  lotsToExit,
			"exit_quantity": exitQuantity,
		})
	} else {
		// For full exit, use the absolute value of current position
		// (rounded to nearest lot if needed)
		absQuantity := int(math.Abs(float64(currentPosition.Quantity)))
		lotSize := getLotSize(indexName)

		// Ensure it's a multiple of lot size
		if absQuantity%lotSize != 0 {
			lotsToExit := int(math.Round(float64(absQuantity) / float64(lotSize)))
			exitQuantity = lotsToExit * lotSize
		} else {
			exitQuantity = absQuantity
		}

		s.log.Info("Calculated full exit quantity", map[string]interface{}{
			"original_quantity": currentPosition.Quantity,
			"exit_quantity":     exitQuantity,
		})
	}
	if err != nil {
		s.log.Error("Failed to calculate exit quantity", map[string]interface{}{
			"error":            err.Error(),
			"instrument_token": instrumentTokenStr,
			"trading_symbol":   currentPosition.TradingSymbol,
			"current_quantity": currentPosition.Quantity,
			"index_name":       indexName,
		})
		sendErrorResponse(w, "Failed to calculate exit quantity", http.StatusInternalServerError)
		return
	}

	// 6. Determine transaction type (opposite of current position)
	transactionType := "SELL" // Default for long positions (positive quantity)
	if currentPosition.Quantity < 0 {
		transactionType = "BUY" // For short positions (negative quantity)
	}

	// The exitQuantity should always be positive when sending to the order system
	// The direction (buy/sell) is determined by the transactionType
	positiveExitQuantity := int(math.Abs(float64(exitQuantity)))

	s.log.Info("Exit order details", map[string]interface{}{
		"position_quantity": currentPosition.Quantity,
		"exit_quantity":     positiveExitQuantity,
		"transaction_type":  transactionType,
	})

	// 7. Prepare the order details
	exitOrderReq := &PlaceOrderAPIRequest{
		InstrumentToken: instrumentTokenStr,
		TradingSymbol:   currentPosition.TradingSymbol,
		Exchange:        currentPosition.Exchange,
		OrderType:       "MARKET",
		Side:            transactionType,
		Quantity:        positiveExitQuantity, // Always positive
		Product:         currentPosition.Product,
		Validity:        "DAY",
		Tag:             "exit_position",
		PaperTrading:    currentPosition.PaperTrading,
	}

	// 8. Place the exit order
	err = s.placeOrderInternal(r.Context(), exitOrderReq)
	if err != nil {
		s.log.Error("Failed to place exit order", map[string]interface{}{
			"error":            err.Error(),
			"instrument_token": instrumentTokenStr,
			"trading_symbol":   currentPosition.TradingSymbol,
			"quantity":         exitQuantity,
		})
		sendErrorResponse(w, "Failed to place exit order", http.StatusInternalServerError)
		return
	}

	// 9. Calculate lots for the response message
	lotSize := getLotSize(indexName)
	lotsExited := 0
	if lotSize > 0 {
		lotsExited = exitQuantity / lotSize
	}

	// 10. Send success response
	successResponse := map[string]interface{}{
		"order_id": "EX" + instrumentTokenStr[len(instrumentTokenStr)-6:],
		"status":   "SUCCESS",
		"message":  fmt.Sprintf("Exited %d lots (%d quantity) of %s", lotsExited, exitQuantity, currentPosition.TradingSymbol),
		"lots":     lotsExited,
		"quantity": exitQuantity,
	}
	sendJSONResponse(w, successResponse)
}

// processMoveOperation handles the move_away and move_closer operations
func (s *Server) processMoveOperation(w http.ResponseWriter, r *http.Request, req *PlaceOrderAPIRequest,
	currentPosition *db.PositionRecord, toProcessQuantity int, tokenMetaData cache.InstrumentData) error {
	cacheMeta, err := cache.GetCacheMetaInstance()
	if err != nil {
		return fmt.Errorf("failed to get cache meta instance: %w", err)
	}

	// Calculate the new strike price based on the move type and steps
	newStrike, _ := strconv.ParseFloat(tokenMetaData.StrikePrice, 64)
	newStrike = calculateNewStrike(newStrike, tokenMetaData.InstrumentType, req.MoveType, req.Steps)

	// Get the instrument token for the new strike
	newInstrumentToken, err := cacheMeta.GetInstrumentTokenForStrike(r.Context(), newStrike, tokenMetaData.InstrumentType, tokenMetaData.Expiry)
	if err != nil {
		return fmt.Errorf("failed to get instrument token for new strike: %w", err)
	}

	// Get the trading symbol for the new strike
	newTradingSymbol, newExchange, err := zerodha.GetKiteConnect().GetInstrumentDetailsByToken(r.Context(), newInstrumentToken)
	if err != nil {
		return fmt.Errorf("failed to get trading symbol for new strike: %w", err)
	}

	// First, place an order to close the current position
	exitSide := "BUY"
	if currentPosition.Quantity > 0 {
		exitSide = "SELL"
	}

	exitOrderReq := &PlaceOrderAPIRequest{
		InstrumentToken: req.InstrumentToken,
		TradingSymbol:   currentPosition.TradingSymbol,
		Exchange:        currentPosition.Exchange,
		OrderType:       "MARKET",
		Side:            exitSide,
		Quantity:        int(math.Abs(float64(toProcessQuantity))),
		Product:         currentPosition.Product,
		Validity:        "DAY",
		Tag:             fmt.Sprintf("move_%s_exit", req.MoveType),
		PaperTrading:    currentPosition.PaperTrading,
	}

	// Place the exit order
	err = s.placeOrderInternal(r.Context(), exitOrderReq)
	if err != nil {
		return fmt.Errorf("failed to place exit order: %w", err)
	}

	// Then, place an order to enter the new position
	// The side for the new position should be the same as the original position
	entrySide := "BUY"
	if currentPosition.Quantity < 0 {
		entrySide = "SELL"
	}

	entryOrderReq := &PlaceOrderAPIRequest{
		InstrumentToken: newInstrumentToken,
		TradingSymbol:   newTradingSymbol,
		Exchange:        newExchange,
		OrderType:       "MARKET",
		Side:            entrySide,
		Quantity:        int(math.Abs(float64(toProcessQuantity))),
		Product:         currentPosition.Product,
		Validity:        "DAY",
		Tag:             fmt.Sprintf("move_%s_entry", req.MoveType),
		PaperTrading:    currentPosition.PaperTrading,
	}

	// Place the entry order
	return s.placeOrderInternal(r.Context(), entryOrderReq)
}

// calculateNewStrike calculates the new strike price based on the move type and steps
func calculateNewStrike(currentStrike float64, instrumentType string, moveType MoveType, steps int) float64 {
	// Determine the step size based on the index
	stepSize := 50.0 // Default for NIFTY
	if strings.Contains(instrumentType, "SENSEX") {
		stepSize = 100.0
	}

	// Calculate the strike price change
	strikeChange := float64(steps) * stepSize

	// Apply the change based on the move type and option type
	if instrumentType == "CE" {
		if moveType == MoveAway {
			return currentStrike + strikeChange
		} else { // MoveCloser
			return currentStrike - strikeChange
		}
	} else { // PE
		if moveType == MoveAway {
			return currentStrike - strikeChange
		} else { // MoveCloser
			return currentStrike + strikeChange
		}
	}
}

// placeOrderInternal places an order using the internal order placement logic
func (s *Server) placeOrderInternal(ctx context.Context, req *PlaceOrderAPIRequest) error {
	// Convert the API request to a Zerodha order request
	orderReq := zerodha.PlaceOrderRequest{
		TradingSymbol: req.TradingSymbol,
		Exchange:      req.Exchange,
		OrderType:     zerodha.OrderType(req.OrderType),
		Side:          zerodha.OrderSide(req.Side),
		Quantity:      req.Quantity,
		Price:         req.Price,
		TriggerPrice:  req.TriggerPrice,
		Product:       zerodha.ProductType(req.Product),
		Validity:      req.Validity,
		DisclosedQty:  req.DisclosedQty,
		Tag:           req.Tag,
	}

	var resp *zerodha.OrderResponse
	var kiteResp interface{}

	// Check if this is a paper trading order
	if req.PaperTrading {
		// Generate a simulated response for paper trading
		s.log.Info("Processing paper trading order", map[string]interface{}{
			"symbol": req.TradingSymbol,
			"side":   req.Side,
			"qty":    req.Quantity,
		})

		// Try to get the latest price for the instrument from Redis
		var executionPrice float64 = req.Price // Default to the requested price

		if req.InstrumentToken != "" {
			// Get Redis cache for LTP data
			redisCache, err := cache.GetRedisCache()
			if err == nil {
				ltpDB := redisCache.GetLTPDB3()
				if ltpDB != nil {
					// Create a context with timeout for Redis operations
					ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
					defer cancel()

					// Format the key as expected in Redis
					ltpKey := fmt.Sprintf("%s_ltp", req.InstrumentToken)

					// Try to get the LTP from Redis
					ltpStr, err := ltpDB.Get(ctx, ltpKey).Result()
					if err == nil {
						ltp, err := strconv.ParseFloat(ltpStr, 64)
						if err == nil && ltp > 0 {
							executionPrice = ltp
							s.log.Info("Using Redis LTP for paper trading", map[string]interface{}{
								"instrument_token": req.InstrumentToken,
								"ltp":              ltp,
							})
						}
					}
				}
			}
		}

		// Generate a unique order ID for paper trading
		paperOrderID := fmt.Sprintf("paper-%s-%d", strings.ToLower(req.TradingSymbol), time.Now().UnixNano())

		// Create a simulated response
		resp = &zerodha.OrderResponse{
			OrderID: paperOrderID,
			Status:  "PAPER",
			Message: "Paper trading order simulated successfully",
		}

		// For paper trades, KiteResponse should be nil as there's no actual Zerodha interaction
		kiteResp = nil

		// Store the execution price in the request for tracking purposes
		orderReq.Price = executionPrice
	} else {
		// Place the actual order with Zerodha using the token from KiteConnect
		var err error
		resp, err = zerodha.PlaceOrder(orderReq)
		if err != nil {
			return fmt.Errorf("order placement failed: %w", err)
		}

		// Store the Kite response for persistence
		kiteResp = resp
	}

	// Persist the order to the database (both real and paper) - using "system" as userID
	zerodha.SaveOrderAsync(orderReq, resp, "system", kiteResp)

	return nil
}
