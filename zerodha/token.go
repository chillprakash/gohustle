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

	"gohustle/logger"

	"github.com/pquerna/otp/totp"
)

var _ TokenOperations = (*KiteConnect)(nil)

func (k *KiteConnect) GetValidToken(ctx context.Context) (string, error) {
	log := logger.GetLogger()

	token, err := k.getToken(ctx)
	if err == nil && token != "" {
		log.Info("Found valid token in database", map[string]interface{}{
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
		return "", errors.New("failed to refresh token")
	}

	return token, nil
}

func (k *KiteConnect) refreshAccessToken(ctx context.Context) (string, error) {
	log := logger.GetLogger()

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

	// Save the token directly
	if err := k.storeToken(ctx, accessToken); err != nil {
		log.Error("Failed to save access token", map[string]interface{}{
			"error": err.Error(),
		})
		return "", errors.New("failed to save access token")
	}

	return accessToken, nil
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

// GetToken retrieves the latest valid access token
func (k *KiteConnect) getToken(ctx context.Context) (string, error) {
	log := logger.GetLogger()

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
		log.Error("Failed to get token from database", map[string]interface{}{
			"error": err.Error(),
		})
		return "", errors.New("failed to get token from database")
	}

	return token, nil
}

// storeToken stores the access token in the database
func (k *KiteConnect) storeToken(ctx context.Context, token string) error {
	log := logger.GetLogger()

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
	if err != nil {
		log.Error("Failed to store token in database", map[string]interface{}{
			"error": err.Error(),
		})
		return errors.New("failed to store token in database")
	}

	log.Info("Successfully stored token", map[string]interface{}{
		"token_length": len(token),
	})
	return nil
}

// GenerateAccessToken generates and sets the access token using the request token
func (k *KiteConnect) generateAccessToken(ctx context.Context, requestToken string) (string, error) {
	log := logger.GetLogger()

	session, err := k.Kite.GenerateSession(requestToken, k.config.APISecret)
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
	log := logger.GetLogger()

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
