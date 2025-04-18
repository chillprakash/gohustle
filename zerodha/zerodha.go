package zerodha

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"gohustle/config"
	"gohustle/db"
	"gohustle/logger"
	"gohustle/token"

	"github.com/pquerna/otp/totp"
	kiteconnect "github.com/zerodha/gokiteconnect/v4"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"
)

const (
	TokenKeyPrefix         = "broker:kite:token:"
	TokenValidity          = 24 * time.Hour
	MaxConnections         = 3
	MaxTokensPerConnection = 3000
)

// KiteConnect provides a wrapper around the Kite Connect API
type KiteConnect struct {
	// Core Kite components
	Kite    *kiteconnect.Client
	Tickers []*kiteticker.Ticker
	tokens  []uint32

	// Dependencies
	db     *db.SQLiteHelper
	log    *logger.Logger
	config *config.Config

	// Synchronization
	mu sync.RWMutex
}

var (
	instance *KiteConnect
	initOnce sync.Once
	mu       sync.RWMutex
)

// GetKiteConnect returns the singleton instance of KiteConnect
func GetKiteConnect() *KiteConnect {
	mu.RLock()
	if instance != nil {
		mu.RUnlock()
		return instance
	}
	mu.RUnlock()

	mu.Lock()
	defer mu.Unlock()

	if instance != nil {
		return instance
	}

	initOnce.Do(func() {
		instance = initializeKiteConnect()
	})

	return instance
}

// initializeKiteConnect creates a new KiteConnect instance with default configuration
func initializeKiteConnect() *KiteConnect {
	log := logger.L()
	cfg := config.GetConfig()

	// Create SQLite instance
	dbHelper, err := db.GetSQLiteHelper()
	if err != nil {
		log.Error("Failed to create SQLite instance", map[string]interface{}{
			"error": err.Error(),
		})
		return nil
	}

	kc := &KiteConnect{
		Kite:    kiteconnect.New(cfg.Kite.APIKey),
		Tickers: make([]*kiteticker.Ticker, MaxConnections),
		db:      dbHelper,
		log:     log,
		config:  cfg,
	}

	// Get valid token and set it
	token, err := kc.GetValidToken(context.Background())
	if err != nil {
		log.Error("Failed to get valid token", map[string]interface{}{
			"error": err.Error(),
		})
	} else {
		kc.Kite.SetAccessToken(token)

		// Initialize tickers with the token
		for i := 0; i < MaxConnections; i++ {
			kc.Tickers[i] = kiteticker.New(cfg.Kite.APIKey, token)
			log.Info("Initialized ticker", map[string]interface{}{
				"connection": i + 1,
			})
		}
	}

	return kc
}

// Token Management Methods

// GetValidToken retrieves a valid token or generates a new one
func (k *KiteConnect) GetValidToken(ctx context.Context) (string, error) {
	if k.IsTokenValid(ctx) {
		token, err := k.getStoredToken(ctx)
		if err == nil && token != "" {
			k.log.Info("Found valid token in SQLite", map[string]interface{}{
				"token_length": len(token),
			})
			return token, nil
		}
	}
	k.log.Info("No valid token found, refreshing token", map[string]interface{}{})
	return k.RefreshToken(ctx)
}

// IsTokenValid checks if the stored token is valid
func (k *KiteConnect) IsTokenValid(ctx context.Context) bool {
	k.mu.RLock()
	defer k.mu.RUnlock()

	cred, err := k.db.GetCredential(k.getTokenKey())
	if err != nil || cred == nil {
		return false
	}

	var tokenData token.TokenData
	if err := json.Unmarshal([]byte(cred.Value), &tokenData); err != nil {
		return false
	}

	return time.Now().Before(tokenData.ExpiresAt)
}

// StoreToken saves the token with expiry
func (k *KiteConnect) StoreToken(ctx context.Context, accessToken string) error {
	k.log.Info("Starting token storage", map[string]interface{}{
		"token_length": len(accessToken),
		"key":          k.getTokenKey(),
	})

	// Create TokenData
	tokenData := token.TokenData{
		AccessToken: accessToken,
		ExpiresAt:   time.Now().Add(TokenValidity),
	}

	// Marshal TokenData to store as value
	tokenBytes, err := json.Marshal(tokenData)
	if err != nil {
		k.log.Error("Failed to marshal token data", map[string]interface{}{
			"error": err.Error(),
		})
		return fmt.Errorf("failed to marshal token data: %w", err)
	}

	k.log.Info("Storing token in SQLite", map[string]interface{}{
		"token_length": len(accessToken),
		"key":          k.getTokenKey(),
		"expires_at":   tokenData.ExpiresAt,
	})

	// Store the marshaled TokenData as a string in SQLite
	err = k.db.StoreCredential(k.getTokenKey(), string(tokenBytes))
	if err != nil {
		k.log.Error("Failed to store token in SQLite", map[string]interface{}{
			"error": err.Error(),
			"key":   k.getTokenKey(),
		})
		return fmt.Errorf("failed to store token in SQLite: %w", err)
	}

	k.log.Info("Successfully stored token in SQLite", map[string]interface{}{
		"token_length": len(accessToken),
		"key":          k.getTokenKey(),
		"expires_at":   tokenData.ExpiresAt,
	})

	// Verify the stored token
	storedCred, err := k.db.GetCredential(k.getTokenKey())
	if err != nil {
		k.log.Error("Failed to verify stored token", map[string]interface{}{
			"error": err.Error(),
			"key":   k.getTokenKey(),
		})
		return fmt.Errorf("failed to verify stored token: %w", err)
	}

	if storedCred == nil {
		k.log.Error("Stored token not found during verification", map[string]interface{}{
			"key": k.getTokenKey(),
		})
		return fmt.Errorf("stored token not found during verification")
	}

	var storedTokenData token.TokenData
	if err := json.Unmarshal([]byte(storedCred.Value), &storedTokenData); err != nil {
		k.log.Error("Failed to unmarshal stored token data", map[string]interface{}{
			"error": err.Error(),
			"key":   k.getTokenKey(),
		})
		return fmt.Errorf("failed to unmarshal stored token data: %w", err)
	}

	k.log.Info("Verified stored token", map[string]interface{}{
		"token_length": len(storedTokenData.AccessToken),
		"key":          k.getTokenKey(),
		"expires_at":   storedTokenData.ExpiresAt,
	})

	return nil
}

// RefreshToken generates a new access token
func (k *KiteConnect) RefreshToken(ctx context.Context) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	client := k.initializeHTTPClient()
	loginResult := k.performLogin(client)
	k.performTwoFactorAuth(client, loginResult)
	requestToken := k.getRequestToken(client)

	accessToken, err := k.generateAccessToken(ctx, requestToken)
	if err != nil {
		k.log.Error("Failed to generate access token", map[string]interface{}{
			"error": err.Error(),
		})
		return "", errors.New("failed to generate access token")
	}

	k.log.Info("Generated access token successfully", map[string]interface{}{
		"token_length": len(accessToken),
		"access_token": accessToken,
	})

	k.log.Info("Attempting to store token", map[string]interface{}{
		"token_length": len(accessToken),
	})

	if err := k.StoreToken(ctx, accessToken); err != nil {
		k.log.Error("Failed to store refreshed token", map[string]interface{}{
			"error": err.Error(),
		})
		return accessToken, fmt.Errorf("token generated but storage failed: %w", err)
	}

	k.log.Info("Refreshed and stored token successfully", map[string]interface{}{
		"token_length": len(accessToken),
		"access_token": accessToken,
	})

	return accessToken, nil
}

// Ticker Management Methods

func (k *KiteConnect) InitializeTickersWithTokens(tokens []uint32) error {
	k.log.Info("Initializing tickers with tokens", map[string]interface{}{
		"tokens_count": len(tokens),
	})

	k.tokens = tokens
	return k.ConnectTicker()
}

// Close closes all connections
func (k *KiteConnect) Close() {
	k.log.Info("Starting KiteConnect shutdown", map[string]interface{}{
		"total_tickers": len(k.Tickers),
	})

	for i, ticker := range k.Tickers {
		if ticker != nil {
			k.log.Info("Closing ticker connection", map[string]interface{}{
				"connection_index": i,
			})
			ticker.Close()
		}
	}

	k.log.Info("Completed KiteConnect shutdown", map[string]interface{}{
		"status": "closed",
	})
}

// Business Logic Methods

// GetCurrentSpotPriceOfAllIndices fetches current spot prices for all indices
func (k *KiteConnect) GetCurrentSpotPriceOfAllIndices(ctx context.Context) (map[string]float64, error) {
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
		k.log.Error("Failed to fetch spot prices", map[string]interface{}{
			"error":      err.Error(),
			"error_type": reflect.TypeOf(err).String(),
			"symbols":    exchangeTradingSymbols,
		})
		return nil, errors.New("failed to fetch spot prices")
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

	k.log.Info("Successfully fetched spot prices", map[string]interface{}{
		"prices": indexVsSpotPrice,
	})

	return indexVsSpotPrice, nil
}

// Helper Methods

func (k *KiteConnect) getTokenKey() string {
	return TokenKeyPrefix + k.config.Kite.UserID
}

func (k *KiteConnect) getStoredToken(ctx context.Context) (string, error) {
	cred, err := k.db.GetCredential(k.getTokenKey())
	if err != nil || cred == nil {
		return "", fmt.Errorf("no token found")
	}

	var tokenData token.TokenData
	if err := json.Unmarshal([]byte(cred.Value), &tokenData); err != nil {
		return "", err
	}

	return tokenData.AccessToken, nil
}

// Authentication Helper Methods

func (k *KiteConnect) performLogin(client *http.Client) *LoginResponse {
	loginData := url.Values{}
	loginData.Set("user_id", k.config.Kite.UserID)
	loginData.Set("password", k.config.Kite.UserPassword)

	k.log.Info("Attempting login", map[string]interface{}{
		"user_id": k.config.Kite.UserID,
		"url":     k.config.Kite.LoginURL,
	})

	loginResp, err := client.PostForm(k.config.Kite.LoginURL, loginData)
	if err != nil {
		k.log.Error("Login request failed", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}
	defer loginResp.Body.Close()

	var loginResult LoginResponse
	if err := json.NewDecoder(loginResp.Body).Decode(&loginResult); err != nil {
		k.log.Error("Failed to parse login response", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	return &loginResult
}

func (k *KiteConnect) performTwoFactorAuth(client *http.Client, loginResult *LoginResponse) {
	totpCode, err := totp.GenerateCode(k.config.Kite.TOTPKey, time.Now())
	if err != nil {
		k.log.Error("Failed to generate TOTP", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	twoFAData := url.Values{}
	twoFAData.Set("user_id", k.config.Kite.UserID)
	twoFAData.Set("request_id", loginResult.Data.RequestID)
	twoFAData.Set("twofa_value", totpCode)
	twoFAData.Set("twofa_type", "totp")

	k.log.Info("Attempting 2FA", map[string]interface{}{
		"request_id": loginResult.Data.RequestID,
	})

	twoFAResp, err := client.PostForm(k.config.Kite.TwoFAURL, twoFAData)
	if err != nil {
		k.log.Error("2FA request failed", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}
	defer twoFAResp.Body.Close()
}

func (k *KiteConnect) getRequestToken(client *http.Client) string {
	kiteLoginURL := fmt.Sprintf(
		"https://kite.zerodha.com/connect/login?api_key=%s&v=3",
		k.config.Kite.APIKey,
	)

	k.log.Info("Attempting to get request token", map[string]interface{}{
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

func (k *KiteConnect) generateAccessToken(ctx context.Context, requestToken string) (string, error) {
	session, err := k.Kite.GenerateSession(requestToken, k.config.Kite.APISecret)
	if err != nil {
		k.log.Error("Failed to generate session", map[string]interface{}{
			"error":      err.Error(),
			"error_type": fmt.Sprintf("%T", err),
		})
		return "", errors.New("failed to generate session")
	}

	accessToken := session.AccessToken
	k.Kite.SetAccessToken(accessToken)

	k.log.Info("Access token generated successfully", map[string]interface{}{
		"token_length": len(accessToken),
	})

	return accessToken, nil
}

func (k *KiteConnect) extractRequestToken(errorMsg string) string {
	re := regexp.MustCompile(`request_token=([^&"]+)(&.*)?`)
	matches := re.FindStringSubmatch(errorMsg)

	if len(matches) > 1 {
		token := matches[1]
		k.log.Info("Successfully extracted request token", map[string]interface{}{
			"request_token": token,
			"token_length":  len(token),
		})
		return token
	}

	k.log.Error("Failed to extract request token", map[string]interface{}{
		"error_msg": errorMsg,
	})
	return ""
}

func (k *KiteConnect) initializeHTTPClient() *http.Client {
	jar, err := cookiejar.New(nil)
	if err != nil {
		k.log.Error("Failed to create cookie jar", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	return &http.Client{
		Jar: jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			k.log.Info("Redirect detected", map[string]interface{}{
				"url": req.URL.String(),
			})

			if strings.Contains(req.URL.String(), "request_token=") {
				params := req.URL.Query()
				if token := params.Get("request_token"); token != "" {
					k.log.Info("Found request token in redirect", map[string]interface{}{
						"request_token": token,
					})
					return fmt.Errorf("got request token: %s", token)
				}
			}
			return nil
		},
	}
}

// Verify KiteConnect implements KiteConnector at compile time

// You might want to add a helper method to check the status
func (r *LoginResponse) IsSuccess() bool {
	return r.Status == "success"
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
	log := logger.L()
	dataPath := "data"
	config := config.GetConfig()
	if config != nil && config.Kite.DataPath != "" {
		dataPath = config.Kite.DataPath
	}
	// Ensure directory exists
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		log.Error("Failed to create data directory", map[string]interface{}{
			"error": err.Error(),
			"path":  dataPath,
		})
		os.Exit(1)
	}
	return dataPath
}

// Update GetUpcomingExpiries method
func (k *KiteConnect) GetUpcomingExpiries(ctx context.Context) error {
	log := logger.L()

	// Get data path using helper method
	dataPath := k.getDataPath()

	// Get all CSV files in the data directory
	files, err := filepath.Glob(filepath.Join(dataPath, "*.csv"))
	if err != nil {
		log.Error("Failed to read data directory", map[string]interface{}{
			"error": err.Error(),
			"path":  dataPath,
		})
		return errors.New("failed to read data directory")
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
				"file":   symbolFile,
			})
			continue
		}
	}

	return nil
}

func (k *KiteConnect) processFile(filePath string) error {
	log := logger.L()
	file, err := os.Open(filePath)
	if err != nil {
		log.Error("Failed to open file", map[string]interface{}{
			"error": err.Error(),
			"path":  filePath,
		})
		return errors.New("failed to open file")
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Skip header
	_, err = reader.Read()
	if err != nil {
		log.Error("Failed to read header", map[string]interface{}{
			"error": err.Error(),
			"path":  filePath,
		})
		return errors.New("failed to read header")
	}

	// Read and log each expiry date
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // End of file
			}
			log.Error("Failed to read record", map[string]interface{}{
				"error": err.Error(),
				"path":  filePath,
			})
			return errors.New("failed to read record")
		}

		expiryDate := record[5] // Assuming expiry date is in the 6th column
		log.Info("Found expiry date", map[string]interface{}{
			"expiry": expiryDate,
			"file":   filePath,
		})
	}

	return nil
}
