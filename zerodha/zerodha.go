package zerodha

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
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
	client      *http.Client
	Kite        *kiteconnect.Client
	db          *db.TimescaleDB
	config      *config.KiteConfig
	accessToken string
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

// RefreshAccessToken refreshes the Kite access token
func (k *KiteConnect) RefreshAccessToken(ctx context.Context) error {
	log := logger.GetLogger()

	// Fetch the token from the database
	existingToken, err := k.FetchTokenFromDB(ctx)
	if err == nil && existingToken != "" {
		log.Info("Valid access token already exists", map[string]interface{}{
			"access_token": existingToken,
		})
		k.Kite.SetAccessToken(existingToken) // Set the existing token
		return nil                           // Exit early if a valid token is found
	}

	// Create a session-like client that maintains cookies
	jar, err := cookiejar.New(nil)
	if err != nil {
		return fmt.Errorf("failed to create cookie jar: %w", err)
	}

	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			log.Info("Redirect detected", map[string]interface{}{
				"url": req.URL.String(),
			})

			// Extract request token from redirect URL
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

	// Step 1: Login
	loginData := url.Values{}
	loginData.Set("user_id", k.config.UserID)
	loginData.Set("password", k.config.UserPassword)

	log.Info("Attempting login", map[string]interface{}{
		"user_id": k.config.UserID,
		"url":     k.config.LoginURL,
	})

	loginResp, err := client.PostForm(k.config.LoginURL, loginData)
	if err != nil {
		return fmt.Errorf("login request failed: %w", err)
	}
	defer loginResp.Body.Close()

	var loginResult LoginResponse
	if err := json.NewDecoder(loginResp.Body).Decode(&loginResult); err != nil {
		return fmt.Errorf("failed to parse login response: %w", err)
	}

	// Step 2: 2FA
	totpCode, err := totp.GenerateCode(k.config.TOTPKey, time.Now())
	if err != nil {
		return fmt.Errorf("failed to generate TOTP: %w", err)
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
		return fmt.Errorf("2FA request failed: %w", err)
	}
	defer twoFAResp.Body.Close()

	// Step 3: Get request token using the same client (maintains session)
	kiteLoginURL := fmt.Sprintf(
		"https://kite.zerodha.com/connect/login?api_key=%s&v=3",
		k.config.APIKey,
	)

	log.Info("Attempting to get request token", map[string]interface{}{
		"url": kiteLoginURL,
	})

	_, err = client.Get(kiteLoginURL)
	if err != nil {
		if strings.Contains(err.Error(), "got request token:") {
			errorMsg := err.Error()
			var requestToken string

			// Extract the request token
			if strings.Contains(errorMsg, "request_token=") {
				parts := strings.Split(errorMsg, "request_token=")
				if len(parts) > 1 {
					requestToken = strings.Split(parts[1], "&")[0]
				}
			} else {
				requestToken = strings.TrimPrefix(errorMsg, "got request token: ")
			}

			requestToken = strings.TrimSpace(requestToken)

			log.Info("Successfully extracted request token", map[string]interface{}{
				"request_token": requestToken,
				"token_length":  len(requestToken),
			})

			if requestToken == "" {
				return fmt.Errorf("failed to extract request token from response")
			}

			// Generate session
			data, err := k.Kite.GenerateSession(requestToken, k.config.APISecret)
			if err != nil {
				log.Error("Failed to generate session", map[string]interface{}{
					"error":         err.Error(),
					"request_token": requestToken,
				})
				return fmt.Errorf("failed to generate session: %w", err)
			}

			if data.AccessToken == "" {
				return fmt.Errorf("no access token in session response")
			}

			// Store the access token
			if err := k.storeToken(ctx, data.AccessToken); err != nil {
				log.Error("Failed to store access token", map[string]interface{}{
					"error": err.Error(),
				})
				return fmt.Errorf("failed to store access token: %w", err)
			}

			log.Info("Successfully generated and stored access token", map[string]interface{}{
				"token_length": len(data.AccessToken),
			})

			k.Kite.SetAccessToken(data.AccessToken) // Set the new access token

			return nil
		}
		return fmt.Errorf("failed to get request token: %w", err)
	}

	return fmt.Errorf("no request token found")
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
