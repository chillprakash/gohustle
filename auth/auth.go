package auth

import (
	"context"
	"crypto/rand"
	"fmt"
	"gohustle/cache"
	"gohustle/logger"
	"gohustle/utils"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

// Redis key format constants
const (
	// JwtTokenFormat is the format for JWT tokens in Redis
	JwtTokenFormat = "token:jwt:%s" // token
	// JwtKey is the key used to store the JWT signing key in Redis
	JwtKey = "auth:jwt_key"
)

var (
	once     sync.Once
	instance *AuthManager

	// Secret key for JWT signing - in production, this should be loaded from secure configuration
	jwtSecret = []byte("your-256-bit-secret")
)

// AuthManager handles authentication operations
type AuthManager struct {
	jwtKey         []byte
	hashedPassword string
	validTokens    map[string]bool
	mu             sync.RWMutex
	storageRedis01 *redis.Client
}

// Credentials represents user login credentials
type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// UserContextKey is the key used to store username in context
type contextKey string

const UserContextKey contextKey = "user"

// Config holds authentication configuration
type Config struct {
	Username string
	Password string
}

// Initialize sets up the authentication system
func Initialize(cfg Config) error {
	_, err := GetAuthManager(cfg)
	return err
}

// GetAuthManager returns the singleton instance of authManager
func GetAuthManager(cfg Config) (*AuthManager, error) {
	var initErr error
	once.Do(func() {
		redisCache, err := cache.GetRedisCache()
		if err != nil {
			initErr = fmt.Errorf("failed to get Redis cache: %w", err)
			return
		}
		instance = &AuthManager{
			validTokens:    make(map[string]bool),
			storageRedis01: redisCache.GetTokenDB0(),
		}
		initErr = instance.Initialize(cfg)
	})
	return instance, initErr
}

// Initialize sets up the authentication system
func (a *AuthManager) Initialize(cfg Config) error {
	// Try to load JWT key from Redis
	key, err := a.storageRedis01.Get(context.Background(), JwtKey).Bytes()
	if err == nil && len(key) > 0 {
		// Use existing key from Redis
		a.jwtKey = key
	} else {
		// Generate new random JWT key
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return fmt.Errorf("failed to generate JWT key: %w", err)
		}
		// Store the new key in Redis (no expiration)
		if err := a.storageRedis01.Set(context.Background(), JwtKey, key, 0).Err(); err != nil {
			return fmt.Errorf("failed to store JWT key in Redis: %w", err)
		}
		a.jwtKey = key
	}

	// Hash the password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(cfg.Password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}
	a.hashedPassword = string(hashedPassword)

	// Validate credentials to ensure they work
	if !a.ValidateCredentials(cfg.Username, cfg.Password) {
		return fmt.Errorf("invalid credentials")
	}

	return nil
}

// Claims represents JWT claims
type Claims struct {
	Username string `json:"username"`
	jwt.RegisteredClaims
}

// ValidateCredentials checks if the provided credentials are valid
func ValidateCredentials(username, password string) bool {
	a, err := GetAuthManager(Config{})
	if err != nil {
		return false
	}
	return a.ValidateCredentials(username, password)
}

// ValidateCredentials checks if the provided credentials are valid
func (a *AuthManager) ValidateCredentials(username, password string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(a.hashedPassword), []byte(password))
	return err == nil
}

// GenerateToken generates a new JWT token for the given username
func GenerateToken(username string) (string, error) {
	return instance.GenerateToken(username)
}

// GenerateToken generates a new JWT token for the given username
func (a *AuthManager) GenerateToken(username string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"username": username,
		"exp":      utils.NowIST().Add(24 * time.Hour).Unix(),
	})

	tokenString, err := token.SignedString(a.jwtKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	// Store token in Redis with 24-hour expiration
	redisKey := fmt.Sprintf(JwtTokenFormat, tokenString)
	err = a.storageRedis01.Set(context.Background(), redisKey, username, 24*time.Hour).Err()
	if err != nil {
		return "", fmt.Errorf("failed to store token in Redis: %w", err)
	}

	a.mu.Lock()
	a.validTokens[tokenString] = true
	a.mu.Unlock()

	return tokenString, nil
}

// ValidateToken validates a JWT token and returns the username if valid
func ValidateToken(tokenString string) (string, error) {
	return instance.ValidateToken(tokenString)
}

// ValidateToken validates a JWT token and returns the username if valid
func (a *AuthManager) ValidateToken(tokenString string) (string, error) {
	// Check Redis first
	redisKey := fmt.Sprintf(JwtTokenFormat, tokenString)
	username, err := a.storageRedis01.Get(context.Background(), redisKey).Result()
	if err == nil && username != "" {
		// Token exists in Redis, validate JWT
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return a.jwtKey, nil
		})

		if err != nil {
			return "", fmt.Errorf("failed to parse token: %w", err)
		}

		if token.Valid {
			return username, nil
		}
	}

	// Fallback to in-memory map for backward compatibility
	a.mu.RLock()
	_, exists := a.validTokens[tokenString]
	a.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("invalid or expired token")
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return a.jwtKey, nil
	})

	if err != nil {
		return "", fmt.Errorf("failed to parse token: %w", err)
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		username, ok := claims["username"].(string)
		if !ok {
			return "", fmt.Errorf("invalid username in token")
		}
		return username, nil
	}

	return "", fmt.Errorf("invalid token")
}

// InvalidateToken removes a token from the valid tokens list (logout)
func InvalidateToken(token string) {
	instance.InvalidateToken(token)
}

// InvalidateToken removes a token from the valid tokens list (logout)
func (a *AuthManager) InvalidateToken(token string) {
	// Remove from Redis
	redisKey := fmt.Sprintf(JwtTokenFormat, token)
	_ = a.storageRedis01.Del(context.Background(), redisKey).Err()

	logger.L().Info("Token invalidated", map[string]interface{}{
		"token": token,
	})

	// Remove from in-memory map
	a.mu.Lock()
	delete(a.validTokens, token)
	a.mu.Unlock()
}

// GetJWTKey returns the current JWT key
func GetJWTKey() []byte {
	return instance.GetJWTKey()
}

// GetJWTKey returns the current JWT key
func (a *AuthManager) GetJWTKey() []byte {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.jwtKey
}
