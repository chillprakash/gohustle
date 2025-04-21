package optionchain

import (
	"context"
	"fmt"
	"gohustle/cache"
	"gohustle/core"
	"gohustle/logger"
	"gohustle/zerodha"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
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

	// Context for cleanup
	ctx    context.Context
	cancel context.CancelFunc
}

type OptionData struct {
	InstrumentToken string  `json:"instrument_token"`
	LTP             float64 `json:"ltp"`
	OI              int64   `json:"oi"`
	Volume          int64   `json:"volume"`
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
	Index           string        `json:"index"`
	Expiry          string        `json:"expiry"`
	UnderlyingPrice float64       `json:"underlying_price"`
	Chain           []*StrikeData `json:"chain"`
	ATMStrike       float64       `json:"atm_strike"`
	Timestamp       int64         `json:"timestamp"`
}

func NewOptionChainManager(ctx context.Context) *OptionChainManager {
	ctx, cancel := context.WithCancel(ctx)
	mgr := &OptionChainManager{
		log:           logger.L(),
		broadcastChan: make(chan *OptionChainResponse, 100),
		latestData:    make(map[string]*OptionChainResponse),
		subscribers:   make(map[string][]chan *OptionChainResponse),
		ctx:           ctx,
		cancel:        cancel,
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
			key := fmt.Sprintf("%s:%s", update.Index, update.Expiry)

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

// GetLatestChain returns the most recent option chain data for the given index and expiry
func (m *OptionChainManager) GetLatestChain(index, expiry string) *OptionChainResponse {
	key := fmt.Sprintf("%s:%s", index, expiry)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latestData[key]
}

// storeTimeSeriesMetrics stores the calculated metrics in Redis using sorted sets
func (m *OptionChainManager) storeTimeSeriesMetrics(ctx context.Context, index string, chain []*StrikeData, underlyingPrice float64) error {
	// Create a context with longer timeout for Redis operations
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second) // Increased timeout for Redis operations
	defer cancel()

	// Get time series DB
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return fmt.Errorf("failed to get Redis cache: %w", err)
	}

	tsDB := redisCache.GetTimeSeriesDB()
	if tsDB == nil {
		return fmt.Errorf("time series DB not available")
	}

	// Find lowest straddle price and calculate synthetic future
	var lowestStraddle float64 = math.MaxFloat64
	var lowestStrike float64
	var callLTP, putLTP float64

	for _, item := range chain {
		if item.CE != nil && item.PE != nil {
			straddlePrice := item.CE.LTP + item.PE.LTP
			if straddlePrice < lowestStraddle {
				lowestStraddle = straddlePrice
				lowestStrike = item.Strike
				callLTP = item.CE.LTP
				putLTP = item.PE.LTP
			}
		}
	}

	if lowestStraddle == math.MaxFloat64 {
		return fmt.Errorf("no valid straddle price found")
	}

	// Calculate synthetic future price
	// sf = round((strike + call - put), 2)
	syntheticFuture := math.Round((lowestStrike+callLTP-putLTP)*100) / 100

	// Current timestamp in milliseconds
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)

	// Store metrics using ZADD
	pipe := tsDB.Pipeline()
	baseKey := fmt.Sprintf("metrics:%s", index)

	// Add data points using ZADD
	pipe.ZAdd(ctx, fmt.Sprintf("%s:spot", baseKey), redis.Z{Score: float64(timestamp), Member: underlyingPrice})
	pipe.ZAdd(ctx, fmt.Sprintf("%s:fair", baseKey), redis.Z{Score: float64(timestamp), Member: syntheticFuture})
	pipe.ZAdd(ctx, fmt.Sprintf("%s:straddle", baseKey), redis.Z{Score: float64(timestamp), Member: lowestStraddle})
	pipe.ZAdd(ctx, fmt.Sprintf("%s:strike", baseKey), redis.Z{Score: float64(timestamp), Member: lowestStrike})
	pipe.ZAdd(ctx, fmt.Sprintf("%s:call", baseKey), redis.Z{Score: float64(timestamp), Member: callLTP})
	pipe.ZAdd(ctx, fmt.Sprintf("%s:put", baseKey), redis.Z{Score: float64(timestamp), Member: putLTP})

	// Set expiry for all keys (24 hours)
	expiry := 24 * time.Hour // Increased from 10 hours to 24 hours
	pipe.Expire(ctx, fmt.Sprintf("%s:spot", baseKey), expiry)
	pipe.Expire(ctx, fmt.Sprintf("%s:fair", baseKey), expiry)
	pipe.Expire(ctx, fmt.Sprintf("%s:straddle", baseKey), expiry)
	pipe.Expire(ctx, fmt.Sprintf("%s:strike", baseKey), expiry)
	pipe.Expire(ctx, fmt.Sprintf("%s:call", baseKey), expiry)
	pipe.Expire(ctx, fmt.Sprintf("%s:put", baseKey), expiry)

	// Cleanup old data (keep last 24 hours)
	oldTimestamp := time.Now().Add(-24*time.Hour).UnixNano() / int64(time.Millisecond)
	pipe.ZRemRangeByScore(ctx, fmt.Sprintf("%s:spot", baseKey), "0", fmt.Sprintf("%d", oldTimestamp))
	pipe.ZRemRangeByScore(ctx, fmt.Sprintf("%s:fair", baseKey), "0", fmt.Sprintf("%d", oldTimestamp))
	pipe.ZRemRangeByScore(ctx, fmt.Sprintf("%s:straddle", baseKey), "0", fmt.Sprintf("%d", oldTimestamp))
	pipe.ZRemRangeByScore(ctx, fmt.Sprintf("%s:strike", baseKey), "0", fmt.Sprintf("%d", oldTimestamp))
	pipe.ZRemRangeByScore(ctx, fmt.Sprintf("%s:call", baseKey), "0", fmt.Sprintf("%d", oldTimestamp))
	pipe.ZRemRangeByScore(ctx, fmt.Sprintf("%s:put", baseKey), "0", fmt.Sprintf("%d", oldTimestamp))

	// Execute pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to store time series metrics: %w", err)
	}

	m.log.Info("Stored time series metrics", map[string]interface{}{
		"index":     index,
		"timestamp": timestamp,
		"spot":      underlyingPrice,
		"fair":      syntheticFuture,
		"straddle":  lowestStraddle,
		"strike":    lowestStrike,
		"call_ltp":  callLTP,
		"put_ltp":   putLTP,
	})

	return nil
}

// CalculateOptionChain calculates the option chain for given parameters
func (m *OptionChainManager) CalculateOptionChain(ctx context.Context, index, expiry string, strikesCount int) (*OptionChainResponse, error) {
	// Create a context with longer timeout for the entire operation
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second) // Increased timeout for entire calculation
	defer cancel()

	// Get Redis and in-memory cache instances
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis cache: %w", err)
	}

	// Configure Redis client timeouts
	ltpDB := redisCache.GetLTPDB3()
	positionsDB := redisCache.GetPositionsDB2()
	if ltpDB == nil || positionsDB == nil {
		return nil, fmt.Errorf("redis initialization failed")
	}

	// Set Redis operation timeouts
	ltpDB.Options().WriteTimeout = 10 * time.Second // Increased from default
	ltpDB.Options().ReadTimeout = 10 * time.Second  // Increased from default
	positionsDB.Options().WriteTimeout = 10 * time.Second
	positionsDB.Options().ReadTimeout = 10 * time.Second

	inMemCache := cache.GetInMemoryCacheInstance()

	// Get strikes for this index and expiry
	strikesKey := fmt.Sprintf("strikes:%s_%s", index, expiry)
	strikesValue, exists := inMemCache.Get(strikesKey)
	if !exists {
		return nil, fmt.Errorf("no strikes found for index %s and expiry %s", index, expiry)
	}

	allStrikes := strikesValue.([]string)
	if len(allStrikes) == 0 {
		return nil, fmt.Errorf("empty strikes list")
	}

	// Get tentative ATM strike
	kc := zerodha.GetKiteConnect()
	indices := core.GetIndices()
	atmStrike_tentative := kc.GetTentativeATMBasedonLTP(*indices.GetIndexByName(index), allStrikes)

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

	startIndex := max(0, middleIndex-strikesCount)
	endIndex := min(len(allStrikes), middleIndex+strikesCount+1)
	selectedStrikes := allStrikes[startIndex:endIndex]

	// Get instrument tokens for selected strikes
	ceTokens := make([]string, 0)
	peTokens := make([]string, 0)
	instrumentDetails := make(map[string]string)

	for _, strike := range selectedStrikes {
		expiryKey := fmt.Sprintf("%s_%s_%s", index, expiry, strike)
		strikeValue, exists := inMemCache.Get(expiryKey)
		if !exists {
			continue
		}
		details := strikeValue.(string)
		instrumentDetails[strike] = details

		parts := strings.Split(details, "||")
		if len(parts) == 2 {
			// PE token
			if peTokenParts := strings.Split(parts[0], "|"); len(peTokenParts) == 2 {
				peToken := strings.TrimPrefix(strings.Split(peTokenParts[0], "_")[1], "")
				peTokens = append(peTokens, peToken)
			}
			// CE token
			if ceTokenParts := strings.Split(parts[1], "|"); len(ceTokenParts) == 2 {
				ceToken := strings.TrimPrefix(strings.Split(ceTokenParts[0], "_")[1], "")
				ceTokens = append(ceTokens, ceToken)
			}
		}
	}

	// Get underlying index price
	var indexToken string
	for _, idx := range indices.GetAllIndices() {
		if idx.NameInOptions == index {
			indexToken = idx.InstrumentToken
			break
		}
	}
	if indexToken == "" {
		return nil, fmt.Errorf("invalid index")
	}

	underlyingPrice, err := ltpDB.Get(ctx, fmt.Sprintf("%s_ltp", indexToken)).Float64()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get underlying price: %w", err)
	}

	// Fetch all option data using pipelines
	ltpPipe := ltpDB.Pipeline()
	posPipe := positionsDB.Pipeline()
	ltpCmds := make(map[string]*redis.StringCmd)
	oiCmds := make(map[string]*redis.StringCmd)
	volumeCmds := make(map[string]*redis.StringCmd)
	positionCmds := make(map[string]*redis.StringCmd)

	allTokens := append(ceTokens, peTokens...)
	for _, token := range allTokens {
		ltpCmds[token] = ltpPipe.Get(ctx, fmt.Sprintf("%s_ltp", token))
		oiCmds[token] = ltpPipe.Get(ctx, fmt.Sprintf("%s_oi", token))
		volumeCmds[token] = ltpPipe.Get(ctx, fmt.Sprintf("%s_volume", token))
		positionCmds[token] = posPipe.Get(ctx, fmt.Sprintf("position:token:%s", token))
	}

	// Execute pipelines
	if _, err := ltpPipe.Exec(ctx); err != nil && err != redis.Nil {
		m.log.Error("Failed to execute LTP pipeline", map[string]interface{}{"error": err.Error()})
	}
	if _, err := posPipe.Exec(ctx); err != nil && err != redis.Nil {
		m.log.Error("Failed to execute positions pipeline", map[string]interface{}{"error": err.Error()})
	}

	// Process results
	instrumentData := make(map[string]*OptionData)
	for _, token := range allTokens {
		data := &OptionData{
			InstrumentToken: token,
		}

		if ltp, err := ltpCmds[token].Float64(); err == nil {
			data.LTP = ltp
		}
		if oi, err := oiCmds[token].Float64(); err == nil {
			data.OI = int64(oi)
		}
		if volume, err := volumeCmds[token].Float64(); err == nil {
			data.Volume = int64(volume)
		}
		if qty, err := positionCmds[token].Int64(); err == nil {
			data.PositionQty = qty
		}

		instrumentData[token] = data
	}

	// Build option chain
	chain := make([]*StrikeData, 0, len(selectedStrikes))
	lowestTotal := float64(999999999)
	var atmStrike float64

	// First pass: Calculate CE+PE totals
	for _, strike := range selectedStrikes {
		details := instrumentDetails[strike]
		parts := strings.Split(details, "||")
		if len(parts) != 2 {
			continue
		}

		strikeFloat, _ := strconv.ParseFloat(strike, 64)
		strikeData := &StrikeData{
			Strike: strikeFloat,
		}

		// Add PE data
		if peTokenParts := strings.Split(parts[0], "|"); len(peTokenParts) == 2 {
			peToken := strings.TrimPrefix(strings.Split(peTokenParts[0], "_")[1], "")
			if peData, exists := instrumentData[peToken]; exists {
				strikeData.PE = peData
			}
		}

		// Add CE data
		if ceTokenParts := strings.Split(parts[1], "|"); len(ceTokenParts) == 2 {
			ceToken := strings.TrimPrefix(strings.Split(ceTokenParts[0], "_")[1], "")
			if ceData, exists := instrumentData[ceToken]; exists {
				strikeData.CE = ceData
			}
		}

		// Calculate CE+PE total
		if strikeData.CE != nil && strikeData.PE != nil {
			total := strikeData.CE.LTP + strikeData.PE.LTP
			strikeData.CEPETotal = total

			if total < lowestTotal {
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
		Index:           index,
		Expiry:          expiry,
		UnderlyingPrice: underlyingPrice,
		Chain:           chain,
		ATMStrike:       atmStrike,
		Timestamp:       time.Now().UnixNano(),
	}

	// Store time series metrics
	if err := m.storeTimeSeriesMetrics(ctx, index, chain, underlyingPrice); err != nil {
		m.log.Error("Failed to store time series metrics", map[string]interface{}{
			"error": err.Error(),
			"index": index,
		})
		// Don't return error as option chain calculation is still successful
	}

	// Broadcast the update
	select {
	case m.broadcastChan <- response:
	default:
		m.log.Error("Broadcast channel full, skipping update", map[string]interface{}{
			"index":  index,
			"expiry": expiry,
		})
	}
	logger.L().Info("Option chain calculated", map[string]interface{}{
		"index":  index,
		"expiry": expiry,
	})
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
