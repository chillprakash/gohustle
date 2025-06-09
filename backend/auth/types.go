package auth

// Credentials represents user login credentials
type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// UserContextKey is the key used to store username in context
type contextKey string

const UserContextKey contextKey = "user"
