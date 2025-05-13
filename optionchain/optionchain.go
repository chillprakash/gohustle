package optionchain

import (
	"context"
	"fmt"
	"gohustle/cache"
	"gohustle/core"
	"gohustle/db"
	"gohustle/logger"
	"gohustle/zerodha"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Singleton instance
var (
	_instance *OptionChainManager
	_once     sync.Once
)

// GetOptionChainManager returns the singleton instance of OptionChainManager
func GetOptionChainManager() *OptionChainManager {
	_once.Do(func() {
		_instance = NewOptionChainManager(context.Background())
	})
	return _instance
}

type OptionChainManager struct {
	log *logger.Logger

	// Channel for broadcasting updates
	broadcastChan chan *OptionChainResponse

	// Latest data storage with RWMutex for thread-safe access
	latestData map[string]*OptionChainResponse // key: "index:expiry"
	mu         sync.RWMutex

	// Subscribers management
	subscribers map[string][]chan *OptionChainResponse // key: "index:expiry"
	subMutex    sync.RWMutex

	// Metrics manager for time series data
	metricsManager *MetricsManager

	// Context for cleanup
	ctx    context.Context
	cancel context.CancelFunc
}

type OptionData struct {
	InstrumentToken string  `json:"instrument_token"`
	LTP             float64 `json:"ltp"`
	OI              uint32  `json:"oi"`
	Volume          uint32  `json:"volume"`
	VWAP            float64 `json:"vwap"`
	Change          float64 `json:"change"`
	PositionQty     int64   `json:"position_qty"`
}

type StrikeData struct {
	Strike    float64     `json:"strike"`
	CE        *OptionData `json:"CE"`
	PE        *OptionData `json:"PE"`
	CEPETotal float64     `json:"ce_pe_total"`
	IsATM     bool        `json:"is_atm"`
}

type OptionChainResponse struct {
	Index            string        `json:"index"`
	Expiry           string        `json:"expiry"`
	UnderlyingPrice  float64       `json:"underlying_price"`
	Chain            []*StrikeData `json:"chain"`
	ATMStrike        float64       `json:"atm_strike"`
	Timestamp        int64         `json:"timestamp"`
	RequestedStrikes int           `json:"requested_strikes"`
}

func NewOptionChainManager(ctx context.Context) *OptionChainManager {
	ctx, cancel := context.WithCancel(ctx)
	mgr := &OptionChainManager{
		log:            logger.L(),
		broadcastChan:  make(chan *OptionChainResponse, 100),
		latestData:     make(map[string]*OptionChainResponse),
		subscribers:    make(map[string][]chan *OptionChainResponse),
		metricsManager: NewMetricsManager(),
		ctx:            ctx,
		cancel:         cancel,
	}

	// Start broadcast handler
	go mgr.handleBroadcasts()

	return mgr
}

// Subscribe returns a channel that receives option chain updates for the given index and expiry
func (m *OptionChainManager) Subscribe(index, expiry string) chan *OptionChainResponse {
	key := fmt.Sprintf("%s:%s", index, expiry)
	ch := make(chan *OptionChainResponse, 10)

	m.subMutex.Lock()
	m.subscribers[key] = append(m.subscribers[key], ch)
	m.subMutex.Unlock()

	// Send latest data immediately if available
	m.mu.RLock()
	if data, exists := m.latestData[key]; exists {
		select {
		case ch <- data:
		default:
		}
	}
	m.mu.RUnlock()

	return ch
}

// Unsubscribe removes a subscriber channel
func (m *OptionChainManager) Unsubscribe(index, expiry string, ch chan *OptionChainResponse) {
	key := fmt.Sprintf("%s:%s", index, expiry)

	m.subMutex.Lock()
	defer m.subMutex.Unlock()

	subs := m.subscribers[key]
	for i, sub := range subs {
		if sub == ch {
			m.subscribers[key] = append(subs[:i], subs[i+1:]...)
			close(ch)
			break
		}
	}
}

// handleBroadcasts processes updates from broadcastChan and distributes to subscribers
func (m *OptionChainManager) handleBroadcasts() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case update := <-m.broadcastChan:
			// Include requested strikes in the cache key
			key := fmt.Sprintf("%s:%s:%d", update.Index, update.Expiry, update.RequestedStrikes)

			// Update latest data
			m.mu.Lock()
			m.latestData[key] = update
			m.mu.Unlock()

			// Broadcast to subscribers
			m.subMutex.RLock()
			subs := m.subscribers[key]
			m.subMutex.RUnlock()

			for _, ch := range subs {
				select {
				case ch <- update:
				default:
					// Skip if channel is full
				}
			}
		}
	}
}

// GetLatestChain returns the most recent option chain data for the given index, expiry, and strikes count
func (m *OptionChainManager) GetLatestChain(index, expiry string, strikesCount int) *OptionChainResponse {
	// Include strikes count in the cache key
	key := fmt.Sprintf("%s:%s:%d", index, expiry, strikesCount)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latestData[key]
}

// storeTimeSeriesMetrics stores the calculated metrics for different intervals
func (m *OptionChainManager) storeTimeSeriesMetrics(ctx context.Context, index string, chain []*StrikeData, underlyingPrice float64) error {
	// Create a context with timeout for database operations
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Calculate metrics once
	metrics := m.metricsManager.calculateMetrics(chain, underlyingPrice)
	now := time.Unix(0, metrics.Timestamp*int64(time.Millisecond))

	// Create error channel to collect errors from goroutines
	errChan := make(chan error, len(m.metricsManager.intervals))
	var wg sync.WaitGroup

	// Store metrics for each configured interval
	for _, interval := range m.metricsManager.intervals {
		if m.metricsManager.shouldStoreForInterval(now, interval.Duration) {
			wg.Add(1)
			go func(interval IntervalConfig) {
				defer wg.Done()

				// Get metrics store instance

				// Store metrics in TimescaleDB
				indexMetrics := &db.IndexMetrics{
					IndexName:     index,
					SpotPrice:     metrics.UnderlyingPrice,
					FairPrice:     metrics.SyntheticFuture,
					StraddlePrice: metrics.LowestStraddle,
					ATMStrike:     metrics.ATMStrike,
					Timestamp:     now,
				}
				if err := db.GetTimescaleDB().StoreIndexMetrics(index, indexMetrics); err != nil {
					errChan <- fmt.Errorf("failed to store metrics for interval %s: %w", interval.Name, err)
					return
				}

				// Run cleanup in a separate goroutine
				// go func() {
				// 	if err := db.GetTimescaleDB().CleanupOldMetrics(interval.TTL); err != nil {
				// 		m.log.Error("Failed to cleanup old metrics", map[string]interface{}{
				// 			"error":    err.Error(),
				// 			"interval": interval.Name,
				// 		})
				// 	}
				// }()
			}(interval)
		}
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Collect any errors
	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}

	// If there were any errors, return them combined
	if len(errors) > 0 {
		return fmt.Errorf("errors storing metrics: %s", strings.Join(errors, "; "))
	}

	m.log.Debug("Stored time series metrics", map[string]interface{}{
		"index":     index,
		"timestamp": metrics.Timestamp,
		"spot":      metrics.UnderlyingPrice,
		"fair":      metrics.SyntheticFuture,
		"straddle":  metrics.LowestStraddle,
	})

	return nil
}

// CalculateOptionChain calculates the option chain for given parameters
func (m *OptionChainManager) CalculateOptionChain(ctx context.Context, index, expiry string, strikesCount int) (*OptionChainResponse, error) {
	// Create a context with longer timeout for the entire operation
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second) // Increased timeout for entire calculation
	defer cancel()

	// Get Redis cache instance with initialization check
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		m.log.Error("Failed to get Redis cache", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("redis cache not initialized: %w", err)
	}

	// Verify Redis DBs are initialized
	ltpDB := redisCache.GetLTPDB3()
	positionsDB := redisCache.GetPositionsDB2()
	if ltpDB == nil || positionsDB == nil {
		m.log.Error("Redis DBs not initialized", map[string]interface{}{
			"ltpDB_nil":       ltpDB == nil,
			"positionsDB_nil": positionsDB == nil,
		})
		return nil, fmt.Errorf("redis databases not properly initialized")
	}

	// Configure Redis client timeouts
	ltpDB.Options().WriteTimeout = 10 * time.Second
	ltpDB.Options().ReadTimeout = 10 * time.Second
	positionsDB.Options().WriteTimeout = 10 * time.Second
	positionsDB.Options().ReadTimeout = 10 * time.Second

	// Get the Redis cache instance
	cacheMeta, err := cache.GetCacheMetaInstance()
	if err != nil {
		m.log.Error("Failed to get cache instance", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("redis cache not initialized: %w", err)
	}
	strikes, err := cacheMeta.GetExpiryStrikes(ctx, index, expiry)
	if err != nil {
		m.log.Error("Failed to get strikes", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to get strikes: %w", err)
	}

	allStrikes := strikes
	if len(allStrikes) == 0 {
		m.log.Error("Invalid strikes data type", map[string]interface{}{
			"index":  index,
			"expiry": expiry,
			"type":   fmt.Sprintf("%T", strikes),
		})
		return nil, fmt.Errorf("invalid strikes data type")
	}

	if len(allStrikes) == 0 {
		return nil, fmt.Errorf("empty strikes list")
	}

	// Get tentative ATM strike
	kc := zerodha.GetKiteConnect()
	indices := core.GetIndices()
	indexObj := indices.GetIndexByName(index)
	if indexObj == nil {
		return nil, fmt.Errorf("index %s is not enabled or does not exist", index)
	}
	atmStrike_tentative := kc.GetTentativeATMBasedonLTP(*indexObj, allStrikes)

	// Find the middle strike and calculate range
	middleIndex := -1
	for i, strike := range allStrikes {
		if strike == atmStrike_tentative {
			middleIndex = i
			break
		}
	}
	if middleIndex == -1 {
		return nil, fmt.Errorf("ATM strike not found in strikes list")
	}

	// Original logic: select strikesCount strikes on each side of the ATM
	startIndex := max(0, middleIndex-strikesCount)
	endIndex := min(len(allStrikes), middleIndex+strikesCount+1)
	selectedStrikes := allStrikes[startIndex:endIndex]

	// Log the number of strikes selected for debugging
	m.log.Info("Selected strikes for option chain", map[string]interface{}{
		"requested_count": strikesCount,
		"selected_count":  len(selectedStrikes),
		"atm_strike":      atmStrike_tentative,
		"start_index":     startIndex,
		"end_index":       endIndex,
		"middle_index":    middleIndex,
		"all_strikes_len": len(allStrikes),
	})

	// Log the actual selected strikes for verification
	strikeValues := make([]string, 0, len(selectedStrikes))
	strikeValues = append(strikeValues, selectedStrikes...)
	m.log.Info("Selected strike values", map[string]interface{}{
		"strikes": strings.Join(strikeValues, ","),
	})

	strikeVsOptionChainStrikeStructList, err := cacheMeta.GetOptionChainStrikeStructList(ctx, index, expiry, selectedStrikes)
	if err != nil {
		m.log.Error("Failed to get option chain strike struct list", map[string]interface{}{
			"index":       index,
			"expiry":      expiry,
			"token_count": len(selectedStrikes),
			"error":       err.Error(),
		})
		return nil, err
	}

	// Build option chain
	chain := make([]*StrikeData, 0, len(selectedStrikes))
	lowestTotal := float64(999999999)
	var atmStrike float64

	// First pass: Calculate CE+PE totals
	for _, strike := range selectedStrikes {
		strikeInt, _ := strconv.ParseInt(strike, 10, 64)
		details := strikeVsOptionChainStrikeStructList[strikeInt]

		strikeFloat, _ := strconv.ParseFloat(strike, 64)
		strikeData := &StrikeData{
			Strike: strikeFloat,
			PE: &OptionData{
				InstrumentToken: details.PEToken,
				LTP:             details.PELTP,
				OI:              details.PEOI,
				Volume:          details.PEVolume,
			},
			CE: &OptionData{
				InstrumentToken: details.CEToken,
				LTP:             details.CELTP,
				OI:              details.CEOI,
				Volume:          details.CEVolume,
			},
		}

		// Calculate CE+PE total
		if strikeData.CE != nil && strikeData.PE != nil {
			total := strikeData.CE.LTP + strikeData.PE.LTP
			strikeData.CEPETotal = total

			// Only consider for ATM strike if both CE and PE have valid prices
			if strikeData.CE.LTP > 0 && strikeData.PE.LTP > 0 && total < lowestTotal {
				lowestTotal = total
				atmStrike = strikeFloat
			}
		}

		chain = append(chain, strikeData)
	}

	// Second pass: Set ATM flags
	for _, strike := range chain {
		strike.IsATM = strike.Strike == atmStrike
	}

	response := &OptionChainResponse{
		Index:            index,
		Expiry:           expiry,
		UnderlyingPrice:  0.0,
		Chain:            chain,
		ATMStrike:        atmStrike,
		Timestamp:        time.Now().UnixNano(),
		RequestedStrikes: strikesCount,
	}

	// Log the final chain size for debugging
	m.log.Info("Final option chain size", map[string]interface{}{
		"requested_strikes": strikesCount,
		"selected_strikes":  len(selectedStrikes),
		"final_chain_size":  len(chain),
	})

	// // Store time series metrics
	// if err := m.storeTimeSeriesMetrics(ctx, index, chain, underlyingPrice); err != nil {
	// 	m.log.Error("Failed to store time series metrics", map[string]interface{}{
	// 		"error": err.Error(),
	// 		"index": index,
	// 	})
	// 	// Don't return error as option chain calculation is still successful
	// }

	// Broadcast the update
	select {
	case m.broadcastChan <- response:
	default:
		m.log.Error("Broadcast channel full, skipping update", map[string]interface{}{
			"index":  index,
			"expiry": expiry,
		})
	}
	return response, nil
}

func (m *OptionChainManager) Close() {
	m.cancel()

	// Close all subscriber channels
	m.subMutex.Lock()
	for _, subs := range m.subscribers {
		for _, ch := range subs {
			close(ch)
		}
	}
	m.subMutex.Unlock()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
