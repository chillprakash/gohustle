package auth

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

var (
	// Initialize with a secure key on startup
	jwtKey []byte

	// Store hashed password
	hashedPassword string

	// Secret key for JWT signing - in production, this should be loaded from secure configuration
	jwtSecret = []byte("your-256-bit-secret")

	// validTokens stores active tokens (for logout support)
	validTokens = make(map[string]bool)
)

// Config holds authentication configuration
type Config struct {
	Username string
	Password string
}

// Initialize sets up the authentication system
func Initialize(cfg Config) error {
	// Generate random JWT key
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return fmt.Errorf("failed to generate JWT key: %w", err)
	}
	jwtKey = key

	// Hash the password
	hash, err := bcrypt.GenerateFromPassword([]byte(cfg.Password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}
	hashedPassword = string(hash)

	return nil
}

// Claims represents JWT claims
type Claims struct {
	Username string `json:"username"`
	jwt.RegisteredClaims
}

// ValidateCredentials checks if the provided credentials are valid
func ValidateCredentials(username, password string) bool {
	// In production, this should check against a secure database
	// For now, we'll use a simple check
	return username == "admin" && password == "admin"
}

// GenerateToken creates a new JWT token for the given username
func GenerateToken(username string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"username": username,
		"exp":      time.Now().Add(24 * time.Hour).Unix(),
	})

	tokenString, err := token.SignedString(jwtSecret)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	// Store token as valid
	validTokens[tokenString] = true

	return tokenString, nil
}

// ValidateToken checks if a token is valid and returns the username
func ValidateToken(tokenString string) (string, error) {
	// Check if token has been invalidated (logged out)
	if !validTokens[tokenString] {
		return "", fmt.Errorf("token has been invalidated")
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return jwtSecret, nil
	})

	if err != nil {
		return "", fmt.Errorf("failed to parse token: %w", err)
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		if username, ok := claims["username"].(string); ok {
			return username, nil
		}
	}

	return "", fmt.Errorf("invalid token claims")
}

// InvalidateToken removes a token from the valid tokens list (logout)
func InvalidateToken(token string) {
	delete(validTokens, token)
}

// GetJWTKey returns the current JWT key
func GetJWTKey() []byte {
	return jwtKey
}
