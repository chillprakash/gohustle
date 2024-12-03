package zerodha

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gohustle/config"
	"gohustle/db"
	"gohustle/logger"

	"net/http/cookiejar"

	"github.com/pquerna/otp/totp"
	kiteconnect "github.com/zerodha/gokiteconnect/v4"
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
	Kite   *kiteconnect.Client
	db     *db.TimescaleDB
	config *config.KiteConfig
}

// NewKiteConnect initializes KiteConnect with token management
func NewKiteConnect(database *db.TimescaleDB, cfg *config.KiteConfig) *KiteConnect {
	log := logger.GetLogger()
	ctx := context.Background()

	// Create a new KiteConnect instance
	kite := &KiteConnect{
		config: cfg,
		db:     database,
	}

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
	k.generateAndStoreSession(ctx, requestToken)

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
	var requestToken string

	if strings.Contains(errorMsg, "request_token=") {
		parts := strings.Split(errorMsg, "request_token=")
		if len(parts) > 1 {
			requestToken = strings.Split(parts[1], "&")[0]
		}
	} else {
		requestToken = strings.TrimPrefix(errorMsg, "got request token: ")
	}

	requestToken = strings.TrimSpace(requestToken)

	log := logger.GetLogger()
	log.Info("Successfully extracted request token", map[string]interface{}{
		"request_token": requestToken,
		"token_length":  len(requestToken),
	})

	if requestToken == "" {
		return ""
	}

	return requestToken
}

func (k *KiteConnect) generateAndStoreSession(ctx context.Context, requestToken string) {
	log := logger.GetLogger()

	// Generate session
	data, err := k.Kite.GenerateSession(requestToken, k.config.APISecret)
	if err != nil {
		log.Error("Failed to generate session", map[string]interface{}{
			"error":         err.Error(),
			"request_token": requestToken,
		})
		os.Exit(1)
	}

	if data.AccessToken == "" {
		log.Error("No access token in session response", nil)
		os.Exit(1)
	}

	// Store the access token
	if err := k.storeToken(ctx, data.AccessToken); err != nil {
		log.Error("Failed to store access token", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	log.Info("Successfully generated and stored access token", map[string]interface{}{
		"token_length": len(data.AccessToken),
	})

	k.Kite.SetAccessToken(data.AccessToken)
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

func (k *KiteConnect) DownloadInstrumentData(ctx context.Context) error {
	log := logger.GetLogger()

	// Define instrument names we're interested in
	instrumentNames := []string{"NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "SENSEX", "BANKEX"}

	// Get all instruments
	instruments, err := k.Kite.GetInstruments()
	if err != nil {
		log.Error("Failed to download instruments", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	log.Info("Downloaded raw data", map[string]interface{}{
		"count": len(instruments),
	})

	// Create data directory if it doesn't exist
	dataPath := "data"
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		log.Error("Failed to create data directory", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Save complete dataset
	k.saveCompleteData(instruments, dataPath)

	// Save individual instrument files
	k.saveInstrumentFiles(instruments, instrumentNames, dataPath)

	return nil
}

func (k *KiteConnect) saveCompleteData(instruments []kiteconnect.Instrument, dataPath string) {
	log := logger.GetLogger()

	// Create complete instruments file
	filePath := filepath.Join(dataPath, "instruments.csv")
	file, err := os.Create(filePath)
	if err != nil {
		log.Error("Failed to create complete data file", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"instrument_token", "exchange_token", "tradingsymbol", "name",
		"last_price", "expiry", "tick_size", "lot_size",
		"instrument_type", "segment", "exchange",
	}
	if err := writer.Write(header); err != nil {
		log.Error("Failed to write header", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Write data
	for _, instrument := range instruments {
		record := []string{
			fmt.Sprintf("%d", instrument.InstrumentToken),
			fmt.Sprintf("%d", instrument.ExchangeToken),
			instrument.Tradingsymbol,
			instrument.Name,
			fmt.Sprintf("%f", instrument.LastPrice),
			instrument.Expiry.String(),
			fmt.Sprintf("%f", instrument.TickSize),
			fmt.Sprintf("%d", instrument.LotSize),
			instrument.InstrumentType,
			instrument.Segment,
			instrument.Exchange,
		}
		if err := writer.Write(record); err != nil {
			log.Error("Failed to write record", map[string]interface{}{
				"error": err.Error(),
			})
			os.Exit(1)
		}
	}

	fileInfo, err := file.Stat()
	if err == nil {
		fileSizeMB := float64(fileInfo.Size()) / (1024 * 1024)
		log.Info("Saved complete data", map[string]interface{}{
			"file":         "instruments.csv",
			"file_size_mb": round(fileSizeMB, 2),
		})
	}
}

func (k *KiteConnect) saveInstrumentFiles(instruments []kiteconnect.Instrument, instrumentNames []string, dataPath string) {
	log := logger.GetLogger()
	currentDate := time.Now().Format("02-01-06")

	// Group instruments by name
	instrumentMap := make(map[string][]kiteconnect.Instrument)
	for _, instrument := range instruments {
		for _, name := range instrumentNames {
			if instrument.Name == name {
				instrumentMap[name] = append(instrumentMap[name], instrument)
			}
		}
	}

	// Save each instrument's data
	for name, items := range instrumentMap {
		fileName := fmt.Sprintf("%s_%s.csv", name, currentDate)
		filePath := filepath.Join(dataPath, fileName)
		file, err := os.Create(filePath)
		if err != nil {
			log.Error("Failed to create instrument file", map[string]interface{}{
				"error": err.Error(),
				"name":  name,
			})
			os.Exit(1)
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()

		// Write header
		header := []string{
			"instrument_token", "exchange_token", "tradingsymbol", "name",
			"last_price", "expiry", "tick_size", "lot_size",
			"instrument_type", "segment", "exchange",
		}
		if err := writer.Write(header); err != nil {
			log.Error("Failed to write header", map[string]interface{}{
				"error": err.Error(),
				"name":  name,
			})
			os.Exit(1)
		}

		// Write data
		for _, instrument := range items {
			record := []string{
				fmt.Sprintf("%d", instrument.InstrumentToken),
				fmt.Sprintf("%d", instrument.ExchangeToken),
				instrument.Tradingsymbol,
				instrument.Name,
				fmt.Sprintf("%f", instrument.LastPrice),
				instrument.Expiry.String(),
				fmt.Sprintf("%f", instrument.TickSize),
				fmt.Sprintf("%d", instrument.LotSize),
				instrument.InstrumentType,
				instrument.Segment,
				instrument.Exchange,
			}
			if err := writer.Write(record); err != nil {
				log.Error("Failed to write record", map[string]interface{}{
					"error": err.Error(),
					"name":  name,
				})
				os.Exit(1)
			}
		}

		fileInfo, err := file.Stat()
		if err == nil {
			fileSizeMB := float64(fileInfo.Size()) / (1024 * 1024)
			log.Info("Saved instrument data", map[string]interface{}{
				"instrument":   name,
				"records":      len(items),
				"file_size_mb": round(fileSizeMB, 2),
			})
		}
	}
}

// Helper function to round float64 to n decimal places
func round(num float64, decimals int) float64 {
	shift := math.Pow(10, float64(decimals))
	return math.Round(num*shift) / shift
}
