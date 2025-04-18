package token

import (
	"context"
	"time"
)

// TokenManager defines the interface for managing broker tokens
type TokenManager interface {
	GetValidToken(ctx context.Context) (string, error)
	StoreToken(ctx context.Context, token string) error
	RefreshToken(ctx context.Context) (string, error)
	IsTokenValid(ctx context.Context) bool
}

// TokenData represents the common structure for token data
type TokenData struct {
	AccessToken string    `json:"access_token"`
	ExpiresAt   time.Time `json:"expires_at"`
}
