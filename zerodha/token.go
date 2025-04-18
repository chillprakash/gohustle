package zerodha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"gohustle/cache"
	"gohustle/config"
	"gohustle/logger"

	"github.com/pquerna/otp/totp"
)

// Constants for token management
const (
	TokenKeyPrefix = "kite:token:" // Prefix for token keys
	TokenSetKey    = "kite:tokens" // Set to track all tokens
)

func (k *KiteConnect) GetValidToken(ctx context.Context) (string, error) {
	log := logger.L()

	// Get Redis cache instance
	redisCache, err := cache.NewRedisCache()
	if err != nil {
		return "", fmt.Errorf("failed to get Redis cache: %w", err)
	}

	// Try to get token from Redis
	token := redisCache.GetValidToken(ctx)
	if token != "" {
		log.Info("Found valid token in Redis", map[string]interface{}{
			"token_length": len(token),
		})
		return token, nil
	}

	log.Info("No valid token found, refreshing access token", nil)
	token, err = k.refreshAccessToken(ctx)
	if err != nil {
		log.Error("Failed to refresh token", map[string]interface{}{
			"error": err.Error(),
		})
		return "", fmt.Errorf("failed to refresh token: %w", err)
	}

	return token, nil
}

func (k *KiteConnect) refreshAccessToken(ctx context.Context) (string, error) {
	log := logger.L()

	// Get Redis cache instance
	redisCache, err := cache.NewRedisCache()
	if err != nil {
		return "", fmt.Errorf("failed to get Redis cache: %w", err)
	}

	client := k.initializeHTTPClient()
	loginResult := k.performLogin(client)
	k.performTwoFactorAuth(client, loginResult)
	requestToken := k.getRequestToken(client)

	// Generate and set the access token
	accessToken, err := k.generateAccessToken(ctx, requestToken)
	if err != nil {
		log.Error("Failed to generate access token", map[string]interface{}{
			"error": err.Error(),
		})
		return "", errors.New("failed to generate access token")
	}

	// Store new token in Redis
	if err := redisCache.StoreAccessToken(ctx, accessToken); err != nil {
		log.Error("Failed to store token in Redis", map[string]interface{}{
			"error": err.Error(),
		})
		// Don't return error here, token is still valid
	}

	return accessToken, nil
}

func (k *KiteConnect) performLogin(client *http.Client) *LoginResponse {
	log := logger.L()
	cfg := config.GetConfig()

	loginData := url.Values{}
	loginData.Set("user_id", cfg.Kite.UserID)
	loginData.Set("password", cfg.Kite.UserPassword)

	log.Info("Attempting login", map[string]interface{}{
		"user_id": cfg.Kite.UserID,
		"url":     cfg.Kite.LoginURL,
	})

	loginResp, err := client.PostForm(cfg.Kite.LoginURL, loginData)
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
	log := logger.L()
	cfg := config.GetConfig()

	totpCode, err := totp.GenerateCode(cfg.Kite.TOTPKey, time.Now())
	if err != nil {
		log.Error("Failed to generate TOTP", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	twoFAData := url.Values{}
	twoFAData.Set("user_id", cfg.Kite.UserID)
	twoFAData.Set("request_id", loginResult.Data.RequestID)
	twoFAData.Set("twofa_value", totpCode)
	twoFAData.Set("twofa_type", "totp")

	log.Info("Attempting 2FA", map[string]interface{}{
		"request_id": loginResult.Data.RequestID,
	})

	twoFAResp, err := client.PostForm(cfg.Kite.TwoFAURL, twoFAData)
	if err != nil {
		log.Error("2FA request failed", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}
	defer twoFAResp.Body.Close()
}

func (k *KiteConnect) getRequestToken(client *http.Client) string {
	log := logger.L()
	cfg := config.GetConfig()

	kiteLoginURL := fmt.Sprintf(
		"https://kite.zerodha.com/connect/login?api_key=%s&v=3",
		cfg.Kite.APIKey,
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

// GenerateAccessToken generates and sets the access token using the request token
func (k *KiteConnect) generateAccessToken(ctx context.Context, requestToken string) (string, error) {
	log := logger.L()
	cfg := config.GetConfig()

	session, err := k.Kite.GenerateSession(requestToken, cfg.Kite.APISecret)
	if err != nil {
		log.Error("Failed to generate session", map[string]interface{}{
			"error":      err.Error(),
			"error_type": fmt.Sprintf("%T", err),
		})
		return "", errors.New("failed to generate session")
	}

	accessToken := session.AccessToken
	k.Kite.SetAccessToken(accessToken)

	log.Info("Access token generated successfully", map[string]interface{}{
		"token_length": len(accessToken),
	})

	return accessToken, nil
}

// extractRequestToken extracts the request token from the redirect URL
func (k *KiteConnect) extractRequestToken(errorMsg string) string {
	log := logger.L()

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
func (k *KiteConnect) initializeHTTPClient() *http.Client {
	log := logger.L()
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
