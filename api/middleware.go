package api

import (
	"context"
	"net/http"
	"strings"

	"gohustle/auth"
)

// AuthMiddleware checks for valid JWT token in Authorization header
func (s *Server) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Get token from Authorization header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			sendErrorResponse(w, "No token provided", http.StatusUnauthorized)
			return
		}

		// Remove Bearer prefix if present
		token := strings.TrimPrefix(authHeader, "Bearer ")

		// Validate token
		username, err := auth.ValidateToken(token)
		if err != nil {
			sendErrorResponse(w, "Invalid token", http.StatusUnauthorized)
			return
		}

		// Add username to context
		ctx := context.WithValue(r.Context(), auth.UserContextKey, username)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
