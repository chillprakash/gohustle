package zerodha

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gohustle/config"
	"gohustle/db"
	"gohustle/filestore"
	"gohustle/logger"

	"net/http/cookiejar"

	"io"

	"github.com/pquerna/otp/totp"
	kiteconnect "github.com/zerodha/gokiteconnect/v4"

	"google.golang.org/protobuf/proto"
)

type KiteConnector interface {
	GetToken(ctx context.Context) (string, error)
	RefreshAccessToken(ctx context.Context) error
	FetchTokenFromDB(ctx context.Context) (string, error)
	SetAccessToken(token string)

	// Market data operations
	GetCurrentSpotPriceOfAllIndices(ctx context.Context) (map[string]float64, error)

	// New method for downloading instrument data
	DownloadInstrumentData(ctx context.Context) error

	// New method
	GetUpcomingExpiries(ctx context.Context) error

	// GetInstrumentExpiries reads the gzipped instrument data and returns expiry dates
	GetInstrumentExpiries(ctx context.Context) (map[string][]time.Time, error)
}

// Verify KiteConnect implements KiteConnector at compile time
var _ KiteConnector = (*KiteConnect)(nil)

type LoginResponse struct {
	Status string `json:"status"`
	Data   struct {
		UserID     string   `json:"user_id"`
		RequestID  string   `json:"request_id"`
		TwoFAType  string   `json:"twofa_type"`
		TwoFATypes []string `json:"twofa_types"`
		Profile    struct {
			UserName      string `json:"user_name"`
			UserShortname string `json:"user_shortname"`
			AvatarURL     string `json:"avatar_url"`
		} `json:"profile"`
	} `json:"data"`
}

// You might want to add a helper method to check the status
func (r *LoginResponse) IsSuccess() bool {
	return r.Status == "success"
}

// KiteConnect handles broker interactions
type KiteConnect struct {
	Kite      *kiteconnect.Client
	db        *db.TimescaleDB
	config    *config.KiteConfig
	fileStore filestore.FileStore
}

// NewKiteConnect initializes KiteConnect with token management
func NewKiteConnect(database *db.TimescaleDB, cfg *config.KiteConfig) *KiteConnect {
	log := logger.GetLogger()
	ctx := context.Background()

	// Create a new KiteConnect instance
	kite := &KiteConnect{
		config:    cfg,
		db:        database,
		fileStore: filestore.NewDiskFileStore(),
	}

	log.Info("Initializing KiteConnect client", map[string]interface{}{
		"api_key": cfg.APIKey,
	})

	// Initialize KiteConnect client
	kite.Kite = kiteconnect.New(cfg.APIKey)

	// Attempt to refresh the access token
	err := kite.RefreshAccessToken(ctx)
	if err != nil {
		log.Error("Failed to refresh access token", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Get the latest token and set it
	token, err := kite.GetToken(ctx)
	if err != nil {
		log.Error("Failed to get token", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Set the access token
	kite.Kite.SetAccessToken(token)
	log.Info("Set access token for Kite client", map[string]interface{}{
		"token_length": len(token),
	})

	log.Info("Successfully initialized KiteConnect", nil)

	return kite
}

// storeToken stores the access token in the database
func (k *KiteConnect) storeToken(ctx context.Context, token string) error {
	query := `
        WITH updated AS (
            UPDATE access_tokens 
            SET is_active = FALSE 
            WHERE token_type = 'kite' AND is_active = TRUE
            RETURNING 1
        )
        INSERT INTO access_tokens (token_type, access_token, expires_at)
        VALUES ('kite', $1, NOW() + INTERVAL '24 hours')
    `

	_, err := k.db.Exec(ctx, query, token)
	return err
}

// GetToken retrieves the latest valid access token
func (k *KiteConnect) GetToken(ctx context.Context) (string, error) {
	query := `
        SELECT access_token 
        FROM access_tokens 
        WHERE token_type = 'kite' 
        AND is_active = TRUE 
        AND expires_at > NOW() 
        ORDER BY created_at DESC 
        LIMIT 1
    `

	var token string
	err := k.db.QueryRow(ctx, query).Scan(&token)
	if err != nil {
		return "", fmt.Errorf("failed to get token: %w", err)
	}

	return token, nil
}

// GenerateAccessToken generates and sets the access token using the request token
func (k *KiteConnect) GenerateAccessToken(ctx context.Context, requestToken string) (string, error) {
	log := logger.GetLogger()

	// Generate session to get access token
	session, err := k.Kite.GenerateSession(requestToken, k.config.APISecret)
	if err != nil {
		log.Error("Failed to generate session", map[string]interface{}{
			"error":      err.Error(),
			"error_type": fmt.Sprintf("%T", err),
		})
		return "", err
	}

	accessToken := session.AccessToken
	k.Kite.SetAccessToken(accessToken)

	log.Info("Access token generated successfully", map[string]interface{}{
		"access_token": accessToken,
	})

	return accessToken, nil
}

// RefreshAccessToken refreshes the access token
func (k *KiteConnect) RefreshAccessToken(ctx context.Context) error {
	if k.checkExistingToken(ctx) {
		return nil
	}

	// Initialize HTTP client and perform login flow
	client := k.initializeHTTPClient()
	loginResult := k.performLogin(client)
	k.performTwoFactorAuth(client, loginResult)
	requestToken := k.getRequestToken(client)

	// Generate and set the access token
	accessToken, err := k.GenerateAccessToken(ctx, requestToken)
	if err != nil {
		log := logger.GetLogger()
		log.Error("Failed to refresh access token", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Save the token directly
	if err := k.storeToken(ctx, accessToken); err != nil {
		log := logger.GetLogger()
		log.Error("Failed to save access token", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	return nil
}

func (k *KiteConnect) checkExistingToken(ctx context.Context) bool {
	log := logger.GetLogger()
	existingToken, err := k.FetchTokenFromDB(ctx)
	if err != nil {
		log.Error("Failed to fetch token from DB", map[string]interface{}{
			"error": err.Error(),
		})
		return false
	}

	if existingToken != "" {
		log.Info("Valid access token already exists", map[string]interface{}{
			"access_token": existingToken,
		})
		k.Kite.SetAccessToken(existingToken)
		return true
	}
	return false
}

func (k *KiteConnect) initializeHTTPClient() *http.Client {
	log := logger.GetLogger()
	jar, err := cookiejar.New(nil)
	if err != nil {
		log.Error("Failed to create cookie jar", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	return &http.Client{
		Jar: jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			log.Info("Redirect detected", map[string]interface{}{
				"url": req.URL.String(),
			})

			if strings.Contains(req.URL.String(), "request_token=") {
				params := req.URL.Query()
				if token := params.Get("request_token"); token != "" {
					log.Info("Found request token in redirect", map[string]interface{}{
						"request_token": token,
					})
					return fmt.Errorf("got request token: %s", token)
				}
			}
			return nil
		},
	}
}

func (k *KiteConnect) performLogin(client *http.Client) *LoginResponse {
	log := logger.GetLogger()
	loginData := url.Values{}
	loginData.Set("user_id", k.config.UserID)
	loginData.Set("password", k.config.UserPassword)

	log.Info("Attempting login", map[string]interface{}{
		"user_id": k.config.UserID,
		"url":     k.config.LoginURL,
	})

	loginResp, err := client.PostForm(k.config.LoginURL, loginData)
	if err != nil {
		log.Error("Login request failed", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}
	defer loginResp.Body.Close()

	var loginResult LoginResponse
	if err := json.NewDecoder(loginResp.Body).Decode(&loginResult); err != nil {
		log.Error("Failed to parse login response", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	return &loginResult
}

func (k *KiteConnect) performTwoFactorAuth(client *http.Client, loginResult *LoginResponse) {
	log := logger.GetLogger()
	totpCode, err := totp.GenerateCode(k.config.TOTPKey, time.Now())
	if err != nil {
		log.Error("Failed to generate TOTP", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	twoFAData := url.Values{}
	twoFAData.Set("user_id", k.config.UserID)
	twoFAData.Set("request_id", loginResult.Data.RequestID)
	twoFAData.Set("twofa_value", totpCode)
	twoFAData.Set("twofa_type", "totp")

	log.Info("Attempting 2FA", map[string]interface{}{
		"request_id": loginResult.Data.RequestID,
	})

	twoFAResp, err := client.PostForm(k.config.TwoFAURL, twoFAData)
	if err != nil {
		log.Error("2FA request failed", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}
	defer twoFAResp.Body.Close()
}

func (k *KiteConnect) getRequestToken(client *http.Client) string {
	log := logger.GetLogger()
	kiteLoginURL := fmt.Sprintf(
		"https://kite.zerodha.com/connect/login?api_key=%s&v=3",
		k.config.APIKey,
	)

	log.Info("Attempting to get request token", map[string]interface{}{
		"url": kiteLoginURL,
	})

	_, err := client.Get(kiteLoginURL)
	if err != nil {
		if strings.Contains(err.Error(), "got request token:") {
			return k.extractRequestToken(err.Error())
		}
		return ""
	}

	return ""
}

func (k *KiteConnect) extractRequestToken(errorMsg string) string {
	log := logger.GetLogger()

	// Extract token after "request_token=" with optional ending
	re := regexp.MustCompile(`request_token=([^&"]+)(&.*)?`)
	matches := re.FindStringSubmatch(errorMsg)

	if len(matches) > 1 {
		token := matches[1]
		log.Info("Successfully extracted request token", map[string]interface{}{
			"request_token": token,
			"token_length":  len(token),
		})
		return token
	}

	log.Error("Failed to extract request token", map[string]interface{}{
		"error_msg": errorMsg,
	})
	return ""
}

// GetCurrentSpotPriceOfAllIndices fetches current spot prices for all indices
func (k *KiteConnect) GetCurrentSpotPriceOfAllIndices(ctx context.Context) (map[string]float64, error) {
	log := logger.GetLogger()

	// Define exchange trading symbols
	exchangeTradingSymbols := []string{
		"NSE:NIFTY 50",
		"NSE:NIFTY BANK",
		"NSE:NIFTY FIN SERVICE",
		"NSE:NIFTY MID SELECT",
		"BSE:SENSEX",
		"BSE:BANKEX",
	}

	// Fetch quotes for all symbols
	quotes, err := k.Kite.GetQuote(exchangeTradingSymbols...)
	if err != nil {
		log.Error("Failed to fetch spot prices", map[string]interface{}{
			"error":      err.Error(),
			"error_type": fmt.Sprintf("%T", err),
			"symbols":    exchangeTradingSymbols,
		})
		return nil, fmt.Errorf("failed to fetch spot prices: %w", err)
	}

	// Map exchange symbols to index names with their spot prices
	indexVsSpotPrice := map[string]float64{
		"NIFTY":      quotes["NSE:NIFTY 50"].LastPrice,
		"BANKNIFTY":  quotes["NSE:NIFTY BANK"].LastPrice,
		"FINNIFTY":   quotes["NSE:NIFTY FIN SERVICE"].LastPrice,
		"MIDCPNIFTY": quotes["NSE:NIFTY MID SELECT"].LastPrice,
		"SENSEX":     quotes["BSE:SENSEX"].LastPrice,
		"BANKEX":     quotes["BSE:BANKEX"].LastPrice,
	}

	log.Info("Successfully fetched spot prices", map[string]interface{}{
		"prices": indexVsSpotPrice,
	})

	return indexVsSpotPrice, nil
}

// FetchTokenFromDB fetches the access token from the database
func (k *KiteConnect) FetchTokenFromDB(ctx context.Context) (string, error) {
	query := `
        SELECT access_token 
        FROM access_tokens 
        WHERE token_type = 'kite' 
        AND is_active = TRUE 
        AND expires_at > NOW() 
        ORDER BY created_at DESC 
        LIMIT 1
    `

	var token string
	err := k.db.QueryRow(ctx, query).Scan(&token)
	if err != nil {
		return "", fmt.Errorf("failed to fetch token from DB: %w", err)
	}

	return token, nil
}

// SetAccessToken sets the access token for the KiteConnect instance
func (k *KiteConnect) SetAccessToken(token string) {
	k.Kite.SetAccessToken(token)
}

// InstrumentData represents the structure of instrument data
type InstrumentData struct {
	InstrumentToken int     `json:"instrument_token"`
	ExchangeToken   int     `json:"exchange_token"`
	TradingSymbol   string  `json:"tradingsymbol"`
	Name            string  `json:"name"`
	LastPrice       float64 `json:"last_price"`
	Expiry          string  `json:"expiry"`
	Strike          float64 `json:"strike"`
	TickSize        float64 `json:"tick_size"`
	LotSize         int     `json:"lot_size"`
	InstrumentType  string  `json:"instrument_type"`
	Segment         string  `json:"segment"`
	Exchange        string  `json:"exchange"`
}

// Add this helper method
func (k *KiteConnect) getDataPath() string {
	dataPath := "data"
	if k.config != nil && k.config.DataPath != "" {
		dataPath = k.config.DataPath
	}
	// Ensure directory exists
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		log := logger.GetLogger()
		log.Error("Failed to create data directory", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}
	return dataPath
}

// Update DownloadInstrumentData method
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
		return err // Return error instead of os.Exit(1)
	}

	log.Info("Downloaded raw data", map[string]interface{}{
		"total_count": len(allInstruments),
	})

	// Filter instruments
	filteredInstruments := make([]kiteconnect.Instrument, 0)
	for _, inst := range allInstruments {
		for _, name := range instrumentNames {
			if inst.Name == name {
				filteredInstruments = append(filteredInstruments, inst)
				break
			}
		}
	}

	log.Info("Filtered instruments", map[string]interface{}{
		"total_count":    len(allInstruments),
		"filtered_count": len(filteredInstruments),
		"instruments":    instrumentNames,
	})

	// Get current date for file naming
	currentDate := time.Now().Format("02-01-2006")

	// Marshal the filtered instruments
	data, err := proto.Marshal(convertToProtoInstruments(filteredInstruments))
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

// Helper function to convert kiteconnect.Instrument to proto message
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

type ExpiryData struct {
	Symbol       string
	StrikesCount int
}

// Add this struct at the top with other structs
type ExpiryDetails struct {
	Date          time.Time
	StrikesCount  int
	InstrumentMap map[string][]string // map[instrument_type][]tradingsymbols
}

// Update GetUpcomingExpiries method
func (k *KiteConnect) GetUpcomingExpiries(ctx context.Context) error {
	log := logger.GetLogger()

	// Get data path using helper method
	dataPath := k.getDataPath()

	// Get all CSV files in the data directory
	files, err := filepath.Glob(filepath.Join(dataPath, "*.csv"))
	if err != nil {
		log.Error("Failed to read data directory", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Process NIFTY and BANKNIFTY files
	for _, symbol := range []string{"NIFTY", "BANKNIFTY"} {
		var symbolFile string
		for _, file := range files {
			if strings.Contains(file, symbol+"_") {
				symbolFile = file
				break
			}
		}

		if symbolFile == "" {
			log.Error("File not found for symbol", map[string]interface{}{
				"symbol": symbol,
			})
			continue
		}

		err := k.processFile(symbolFile)
		if err != nil {
			log.Error("Failed to process file", map[string]interface{}{
				"error":  err.Error(),
				"symbol": symbol,
			})
			continue
		}
	}

	return nil
}

func (k *KiteConnect) processFile(filePath string) error {
	log := logger.GetLogger()
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	// Read and log each expiry date
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // End of file
			}
			return fmt.Errorf("failed to read record: %w", err)
		}

		expiryDate := record[5] // Assuming expiry date is in the 6th column
		log.Info("Found expiry date", map[string]interface{}{
			"expiry": expiryDate,
		})
	}

	return nil
}

// GetInstrumentExpiries reads the gzipped instrument data and returns expiry dates
func (k *KiteConnect) GetInstrumentExpiries(ctx context.Context) (map[string][]time.Time, error) {
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

	// Map to store expiries for each instrument
	expiryMap := make(map[string][]time.Time)

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

		// Add expiry to map if it's in the future
		if expiry.After(time.Now()) {
			expiryMap[inst.Name] = append(expiryMap[inst.Name], expiry)
		}
	}

	// Sort expiries for each instrument
	for name, expiries := range expiryMap {
		sort.Slice(expiries, func(i, j int) bool {
			return expiries[i].Before(expiries[j])
		})

		log.Info("Found expiries", map[string]interface{}{
			"instrument": name,
			"count":      len(expiries),
			"expiries":   formatExpiries(expiries),
		})
	}

	return expiryMap, nil
}

// Helper function to format expiry dates for logging
func formatExpiries(expiries []time.Time) []string {
	formatted := make([]string, len(expiries))
	for i, expiry := range expiries {
		formatted[i] = expiry.Format("02-Jan-2006")
	}
	return formatted
}
