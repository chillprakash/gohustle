package api

import (
	"gohustle/auth"
	"net/http"
	"strings"
)

// AuthMiddleware checks for valid JWT token
func (s *Server) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for login endpoint and OPTIONS requests
		if r.URL.Path == "/api/auth/login" || r.Method == "OPTIONS" {
			next.ServeHTTP(w, r)
			return
		}

		// Get token from Authorization header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			SendErrorResponse(w, http.StatusUnauthorized, "No authorization header", nil)
			return
		}

		// Remove 'Bearer ' prefix
		tokenString := strings.TrimPrefix(authHeader, "Bearer ")
		if tokenString == authHeader {
			SendErrorResponse(w, http.StatusUnauthorized, "Invalid authorization header format", nil)
			return
		}

		// Validate token
		_, err := auth.ValidateToken(tokenString)
		if err != nil {
			SendErrorResponse(w, http.StatusUnauthorized, "Invalid token", err)
			return
		}

		// Add claims to request context
		ctx := r.Context()
		r = r.WithContext(ctx)

		next.ServeHTTP(w, r)
	})
}
