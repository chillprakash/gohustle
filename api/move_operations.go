package api

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
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

	// 1. Extract strike, expiry, and instrument type from the trading symbol or token
	strikeInfo, err := extractStrikeInfo(r.Context(), req)
	if err != nil {
		s.log.Error("Failed to extract strike info", map[string]interface{}{
			"error":            err.Error(),
			"instrument_token": req.InstrumentToken,
			"trading_symbol":   req.TradingSymbol,
		})
		s.log.Error("Failed to extract strike info", map[string]interface{}{
			"strike_info": strikeInfo,
		})
		sendErrorResponse(w, fmt.Sprintf("Failed to extract strike info: %v", err), http.StatusBadRequest)
		return
	}

	s.log.Info("Extracted strike info", map[string]interface{}{
		"strike":          strikeInfo.Strike,
		"instrument_type": strikeInfo.InstrumentType,
		"expiry":          strikeInfo.Expiry,
	})

	// 2. Find the current position for this instrument
	currentPosition, err := findCurrentPosition(r.Context(), req.InstrumentToken, req.TradingSymbol)
	if err != nil {
		s.log.Error("Failed to find current position", map[string]interface{}{
			"error":            err.Error(),
			"instrument_token": req.InstrumentToken,
			"trading_symbol":   req.TradingSymbol,
		})
		sendErrorResponse(w, fmt.Sprintf("Failed to find current position: %v", err), http.StatusBadRequest)
		return
	}

	if currentPosition == nil {
		sendErrorResponse(w, "No position found for the specified instrument", http.StatusBadRequest)
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
	instrumentTokenValue, tokenFound := zerodha.GetInstrumentToken(r.Context(), currentPosition.TradingSymbol)
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
	toProcessQuantity, err := calculateQuantityToProcess(currentPosition.Quantity, req.QuantityFrac, strikeInfo.InstrumentType, indexName)
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
		sendErrorResponse(w, "Calculated quantity to process is zero", http.StatusBadRequest)
		return
	}

	s.log.Info("Calculated quantity to process", map[string]interface{}{
		"original_quantity": currentPosition.Quantity,
		"fraction":          req.QuantityFrac,
		"to_process":        toProcessQuantity,
	})

	// 4. Handle the operation based on the move type
	switch req.MoveType {
	case Exit:
		// For exit, we just need to close the position
		err = s.handleExitOperation(w, r, req, currentPosition, toProcessQuantity)
	case MoveAway, MoveCloser:
		// For move operations, we need to calculate the new strike and place orders
		err = s.processMoveOperation(w, r, req, currentPosition, toProcessQuantity, strikeInfo)
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

	// We need an instrument token to fetch the information
	if req.InstrumentToken == "" {
		// If we only have a trading symbol, try to get the instrument token
		if req.TradingSymbol != "" {
			log.Info("Getting instrument token from trading symbol", map[string]interface{}{
				"trading_symbol": req.TradingSymbol,
			})
			token, found := zerodha.GetInstrumentToken(ctx, req.TradingSymbol)
			if !found {
				log.Error("Failed to get instrument token for trading symbol", map[string]interface{}{
					"trading_symbol": req.TradingSymbol,
				})
				return nil, fmt.Errorf("failed to get instrument token for trading symbol: %s", req.TradingSymbol)
			}
			req.InstrumentToken = fmt.Sprintf("%v", token)
			log.Info("Got instrument token from trading symbol", map[string]interface{}{
				"trading_symbol":   req.TradingSymbol,
				"instrument_token": req.InstrumentToken,
			})
		} else {
			log.Error("Neither instrument token nor trading symbol provided", nil)
			return nil, fmt.Errorf("neither instrument token nor trading symbol provided")
		}
	}

	// Make sure we have the trading symbol
	if req.TradingSymbol == "" {
		log.Info("Getting trading symbol from instrument token", map[string]interface{}{
			"instrument_token": req.InstrumentToken,
		})
		// Get the trading symbol from the instrument token
		tradingSymbol, _, err := zerodha.GetKiteConnect().GetInstrumentDetailsByToken(ctx, req.InstrumentToken)
		if err != nil {
			log.Error("Failed to get trading symbol from instrument token", map[string]interface{}{
				"instrument_token": req.InstrumentToken,
				"error":            err.Error(),
			})
			return nil, fmt.Errorf("failed to get trading symbol for token: %s, error: %w", req.InstrumentToken, err)
		}
		req.TradingSymbol = tradingSymbol
		log.Info("Got trading symbol from instrument token", map[string]interface{}{
			"instrument_token": req.InstrumentToken,
			"trading_symbol":   req.TradingSymbol,
		})
	}

	// Get the instrument type (CE/PE) using the new market_data.go method
	log.Info("Getting instrument type from cache", map[string]interface{}{
		"instrument_token": req.InstrumentToken,
	})
	instrumentType, found := zerodha.GetInstrumentType(ctx, req.InstrumentToken)
	if !found {
		// Try to extract from trading symbol as fallback
		log.Error("Failed to get instrument type from cache, attempting to extract from trading symbol", map[string]interface{}{
			"instrument_token": req.InstrumentToken,
			"trading_symbol":   req.TradingSymbol,
		})
	} else {
		log.Info("Got instrument type from cache", map[string]interface{}{
			"instrument_token": req.InstrumentToken,
			"instrument_type":  instrumentType,
		})
	}

	// Get the expiry date using the new market_data.go method
	log.Info("Getting expiry from cache", map[string]interface{}{
		"instrument_token": req.InstrumentToken,
	})
	expiry, found := zerodha.GetExpiry(ctx, req.InstrumentToken)
	if !found {
		// Try to extract from trading symbol as fallback
		log.Error("Failed to get expiry from cache, attempting to extract from trading symbol", map[string]interface{}{
			"instrument_token": req.InstrumentToken,
			"trading_symbol":   req.TradingSymbol,
		})

		// For now, use a placeholder expiry
		expiry = "CURRENT"
		log.Info("Using placeholder expiry", map[string]interface{}{
			"expiry": expiry,
		})
	} else {
		log.Info("Got expiry from cache", map[string]interface{}{
			"instrument_token": req.InstrumentToken,
			"expiry":           expiry,
		})
	}

	// Get the strike price from cache
	log.Info("Getting strike price from cache", map[string]interface{}{
		"instrument_token": req.InstrumentToken,
	})
	strike := 0.0
	inMemoryCache := cache.GetInMemoryCacheInstance()
	if inMemoryCache != nil {
		strikeKey := fmt.Sprintf("strike:%s", req.InstrumentToken)
		log.Info("Looking up strike price with key", map[string]interface{}{
			"strike_key": strikeKey,
		})
		strikeVal, found := inMemoryCache.Get(strikeKey)
		if found {
			strikeStr := fmt.Sprintf("%v", strikeVal)
			log.Info("Found strike value in cache", map[string]interface{}{
				"strike_val": strikeStr,
			})
			strikeFloat, err := strconv.ParseFloat(strikeStr, 64)
			if err == nil {
				strike = strikeFloat
				log.Info("Parsed strike price", map[string]interface{}{
					"strike": strike,
				})
			} else {
				log.Error("Failed to parse strike price", map[string]interface{}{
					"strike_str": strikeStr,
					"error":      err.Error(),
				})
			}
		} else {
			log.Error("Strike price not found in cache", map[string]interface{}{
				"strike_key": strikeKey,
			})

			// Try to extract from trading symbol as fallback
			log.Info("Attempting to extract strike from trading symbol", map[string]interface{}{
				"trading_symbol": req.TradingSymbol,
			})

			// Extract numeric part from trading symbol (e.g., NIFTY2550824450PE -> 24450)
			re := regexp.MustCompile(`(\d+)(CE|PE)$`)
			matches := re.FindStringSubmatch(req.TradingSymbol)
			if len(matches) == 3 {
				strikeStr := matches[1]
				strikeFloat, err := strconv.ParseFloat(strikeStr, 64)
				if err == nil {
					strike = strikeFloat
					log.Info("Extracted strike from trading symbol", map[string]interface{}{
						"trading_symbol": req.TradingSymbol,
						"strike":         strike,
					})
				}
			}
		}
	} else {
		log.Error("In-memory cache instance is nil", nil)
	}

	if strike == 0.0 {
		log.Error("Failed to get strike price", map[string]interface{}{
			"instrument_token": req.InstrumentToken,
			"trading_symbol":   req.TradingSymbol,
		})
		return nil, fmt.Errorf("failed to get strike price for token: %s", req.InstrumentToken)
	}

	strikeInfo := &StrikeInfo{
		Strike:         strike,
		InstrumentType: instrumentType,
		Expiry:         expiry,
		TradingSymbol:  req.TradingSymbol,
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
func findCurrentPosition(ctx context.Context, instrumentToken, tradingSymbol string) (*db.PositionRecord, error) {
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return nil, fmt.Errorf("database connection not available")
	}

	// Get all positions
	positions, err := timescaleDB.ListPositions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list positions: %w", err)
	}

	// Find the position with the matching trading symbol
	for _, pos := range positions {
		if pos.TradingSymbol == tradingSymbol {
			return pos, nil
		}
	}

	// If we couldn't find by trading symbol but have an instrument token, try that
	if instrumentToken != "" {
		for _, pos := range positions {
			// Check if the position has a KiteResponse with matching instrument token
			if pos.KiteResponse != nil {
				if kitePos, ok := pos.KiteResponse.(map[string]interface{}); ok {
					if token, hasToken := kitePos["instrument_token"]; hasToken {
						tokenStr := fmt.Sprintf("%v", token)
						if tokenStr == instrumentToken {
							return pos, nil
						}
					}
				}
			}
		}
	}

	return nil, nil // No position found
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

	log.Info("Position in lots", map[string]interface{}{
		"total_lots": totalLots,
		"lot_size":   lotSize,
	})

	// Calculate the target quantity based on the fraction
	var fractionValue float64
	switch fraction {
	case HalfPosition:
		fractionValue = 0.5
	case QuarterPosition:
		fractionValue = 0.25
	default:
		log.Error("Invalid fraction specified", map[string]interface{}{ // Log invalid fraction
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

	// If the current position is negative (short), make the result negative too
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

	// Calculate the quantity to exit
	exitQuantity := int(math.Abs(float64(currentQuantity)))
	if exitQuantity%lotSize != 0 {
		log.Error("Current position quantity is not a multiple of lot size for exit", map[string]interface{}{
			"current_quantity": currentQuantity,
			"lot_size":         lotSize,
			"remainder":        exitQuantity % lotSize,
		})

		// Round to the nearest number of lots
		exitQuantity = int(math.Round(float64(exitQuantity)/float64(lotSize))) * lotSize
		log.Info("Rounded to nearest number of lots for exit", map[string]interface{}{
			"original_quantity": exitQuantity,
			"exit_quantity":     exitQuantity,
			"lot_size":          lotSize,
		})
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

// handleExitOperation handles the exit operation
func (s *Server) handleExitOperation(w http.ResponseWriter, r *http.Request, req *PlaceOrderAPIRequest, currentPosition *db.PositionRecord, toProcessQuantity int) error {
	// For exit, we just need to place an order to close the position
	// The side should be the opposite of the current position
	side := "BUY"
	if currentPosition.Quantity > 0 {
		side = "SELL"
	}

	// Extract instrument type from the trading symbol to get the lot size
	var instrumentType string
	if strings.Contains(currentPosition.TradingSymbol, "CE") {
		instrumentType = "CE"
	} else if strings.Contains(currentPosition.TradingSymbol, "PE") {
		instrumentType = "PE"
	} else {
		// Default to NIFTY if we can't determine
		instrumentType = "NIFTY"
	}

	// Get the lot size for the instrument type
	lotSize := getLotSize(instrumentType)

	// Ensure quantity is a multiple of lot size
	absQuantity := int(math.Abs(float64(toProcessQuantity)))
	quantityInLots := int(math.Round(float64(absQuantity) / float64(lotSize)))

	// Ensure it's at least 1 lot
	if quantityInLots < 1 {
		quantityInLots = 1
		s.log.Info("Adjusted lots to process to minimum 1 lot", map[string]interface{}{}) // Log adjustment
	}

	// Convert calculated lots back to quantity
	quantityInContracts := quantityInLots * lotSize

	s.log.Info("Calculated exit quantity", map[string]interface{}{
		"original_quantity":     toProcessQuantity,
		"lot_size":              lotSize,
		"quantity_in_lots":      quantityInLots,
		"quantity_in_contracts": quantityInContracts,
	})

	// Create a new order request
	exitOrderReq := &PlaceOrderAPIRequest{
		InstrumentToken: req.InstrumentToken,
		TradingSymbol:   currentPosition.TradingSymbol,
		Exchange:        currentPosition.Exchange,
		OrderType:       "MARKET", // Use market order for exit
		Side:            side,
		Quantity:        quantityInContracts,
		Product:         currentPosition.Product,
		Validity:        "DAY",
		Tag:             "exit_position",
		PaperTrading:    currentPosition.PaperTrading,
	}

	// Place the exit order
	return s.placeOrderInternal(w, r, exitOrderReq)
}

// processMoveOperation handles the move_away and move_closer operations
func (s *Server) processMoveOperation(w http.ResponseWriter, r *http.Request, req *PlaceOrderAPIRequest,
	currentPosition *db.PositionRecord, toProcessQuantity int, strikeInfo *StrikeInfo) error {

	// Calculate the new strike price based on the move type and steps
	newStrike := calculateNewStrike(strikeInfo.Strike, strikeInfo.InstrumentType, req.MoveType, req.Steps)

	// Get the instrument token for the new strike
	newInstrumentToken, err := getInstrumentTokenForStrike(r.Context(), newStrike, strikeInfo.InstrumentType, strikeInfo.Expiry)
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
	err = s.placeOrderInternal(w, r, exitOrderReq)
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
	return s.placeOrderInternal(w, r, entryOrderReq)
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

// getInstrumentTokenForStrike gets the instrument token for a given strike price, instrument type, and expiry
func getInstrumentTokenForStrike(ctx context.Context, strike float64, instrumentType, expiry string) (string, error) {
	// Format the lookup key
	lookupKey := fmt.Sprintf("next_move:%v:%s:%s", strike, instrumentType, expiry)

	// Try to get the instrument token from the cache
	inMemoryCache := cache.GetInMemoryCacheInstance()
	if inMemoryCache == nil {
		return "", fmt.Errorf("in-memory cache not available")
	}

	token, exists := inMemoryCache.Get(lookupKey)
	if !exists {
		return "", fmt.Errorf("instrument token not found for strike %v %s %s", strike, instrumentType, expiry)
	}

	return fmt.Sprintf("%v", token), nil
}

// placeOrderInternal places an order using the internal order placement logic
func (s *Server) placeOrderInternal(w http.ResponseWriter, r *http.Request, req *PlaceOrderAPIRequest) error {
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
					ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
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
