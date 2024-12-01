package go

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "strings"
    "time"

    "github.com/pquerna/otp/totp"
    "hustleapp/go/config"
)

// LoginResponse represents the login API response
type LoginResponse struct {
    Status    bool `json:"status"`
    Data      struct {
        RequestID string `json:"request_id"`
    } `json:"data"`
}

// KiteConnect handles broker interactions
type KiteConnect struct {
    config     *config.KiteConfig
    httpClient *http.Client
    db         *TimescaleDB
}

// NewKiteConnect creates a new KiteConnect instance
func NewKiteConnect(config *config.KiteConfig, db *TimescaleDB) *KiteConnect {
    return &KiteConnect{
        config: config,
        httpClient: &http.Client{
            Timeout: time.Second * 30,
        },
        db: db,
    }
}

// RefreshAccessToken refreshes the Kite access token
func (k *KiteConnect) RefreshAccessToken(ctx context.Context) (string, error) {
    // Step 1: Login
    loginData := url.Values{}
    loginData.Set("user_id", k.config.UserID)
    loginData.Set("password", k.config.UserPassword)

    loginResp, err := k.httpClient.PostForm(k.config.LoginURL, loginData)
    if err != nil {
        return "", fmt.Errorf("login request failed: %w", err)
    }
    defer loginResp.Body.Close()

    body, err := io.ReadAll(loginResp.Body)
    if err != nil {
        return "", fmt.Errorf("failed to read login response: %w", err)
    }

    var loginResult LoginResponse
    if err := json.Unmarshal(body, &loginResult); err != nil {
        return "", fmt.Errorf("failed to parse login response: %w", err)
    }

    // Step 2: 2FA
    totpCode, err := totp.GenerateCode(k.config.TOTPKey, time.Now())
    if err != nil {
        return "", fmt.Errorf("failed to generate TOTP: %w", err)
    }

    twoFAData := url.Values{}
    twoFAData.Set("user_id", k.config.UserID)
    twoFAData.Set("request_id", loginResult.Data.RequestID)
    twoFAData.Set("twofa_value", totpCode)
    twoFAData.Set("twofa_type", "totp")

    twoFAResp, err := k.httpClient.PostForm(k.config.TwoFAURL, twoFAData)
    if err != nil {
        return "", fmt.Errorf("2FA request failed: %w", err)
    }
    defer twoFAResp.Body.Close()

    // Step 3: Get request token
    kiteLoginURL := fmt.Sprintf(
        "https://kite.zerodha.com/connect/login?api_key=%s&v=3",
        k.config.APIKey,
    )

    resp, err := k.httpClient.Get(kiteLoginURL)
    if err != nil {
        return "", fmt.Errorf("failed to get request token: %w", err)
    }
    defer resp.Body.Close()

    // Extract request token from URL
    requestToken := ""
    if resp.Request.URL != nil {
        params := resp.Request.URL.Query()
        if token := params.Get("request_token"); token != "" {
            requestToken = token
        }
    }

    if requestToken == "" {
        return "", fmt.Errorf("request token not found in response URL")
    }

    // Step 4: Generate session
    sessionURL := fmt.Sprintf(
        "https://api.kite.trade/session/token?api_key=%s&request_token=%s&checksum=%s",
        k.config.APIKey,
        requestToken,
        generateChecksum(requestToken, k.config.APISecret),
    )

    sessionResp, err := k.httpClient.Post(
        sessionURL,
        "application/x-www-form-urlencoded",
        bytes.NewBuffer(nil),
    )
    if err != nil {
        return "", fmt.Errorf("session request failed: %w", err)
    }
    defer sessionResp.Body.Close()

    var sessionResult struct {
        Data struct {
            AccessToken string `json:"access_token"`
        } `json:"data"`
    }

    if err := json.NewDecoder(sessionResp.Body).Decode(&sessionResult); err != nil {
        return "", fmt.Errorf("failed to parse session response: %w", err)
    }

    // Store token in database
    if err := k.storeToken(ctx, sessionResult.Data.AccessToken); err != nil {
        return "", fmt.Errorf("failed to store token: %w", err)
    }

    return sessionResult.Data.AccessToken, nil
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

// Helper function to generate checksum
func generateChecksum(requestToken, apiSecret string) string {
    return fmt.Sprintf("%x", strings.Join([]string{requestToken, apiSecret}, ""))
} 