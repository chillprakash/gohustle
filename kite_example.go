package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "hustleapp/go"
)

func main() {
    // Initialize TimescaleDB
    db, err := go.NewTimescaleDB(nil)
    if err != nil {
        log.Fatalf("Failed to initialize database: %v", err)
    }
    defer db.Close()

    // Initialize KiteConnect with config
    config := &go.KiteConfig{
        APIKey:       "your_api_key",
        APISecret:    "your_api_secret",
        UserID:       "your_user_id",
        UserPassword: "your_password",
        TOTPKey:      "your_totp_key",
        LoginURL:     "https://kite.zerodha.com/api/login",
        TwoFAURL:     "https://kite.zerodha.com/api/twofa",
    }

    kite := go.NewKiteConnect(config, db)
    ctx := context.Background()

    // Try to get existing token
    token, err := kite.GetToken(ctx)
    if err != nil {
        // If no valid token exists, refresh it
        token, err = kite.RefreshAccessToken(ctx)
        if err != nil {
            log.Fatalf("Failed to refresh token: %v", err)
        }
    }

    fmt.Printf("Access Token: %s\n", token)
} 