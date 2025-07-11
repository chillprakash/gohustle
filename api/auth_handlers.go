package api

import (
	"encoding/json"
	"net/http"

	"gohustle/auth"
)

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Token string `json:"token"`
}

// handleLogin handles user login
func (s *APIServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	var creds auth.Credentials
	if err := decodeJSONBody(r, &creds); err != nil {
		sendErrorResponse(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Get auth manager
	authManager, err := auth.GetAuthManager(auth.Config{})
	if err != nil {
		sendErrorResponse(w, "Failed to initialize auth", http.StatusInternalServerError)
		return
	}

	// Validate credentials
	if !authManager.ValidateCredentials(creds.Username, creds.Password) {
		sendErrorResponse(w, "Invalid credentials", http.StatusUnauthorized)
		return
	}

	// Generate token
	token, err := authManager.GenerateToken(creds.Username)
	if err != nil {
		sendErrorResponse(w, "Failed to generate token", http.StatusInternalServerError)
		return
	}

	// Send response
	resp := Response{
		Success: true,
		Message: "Login successful",
		Data: map[string]string{
			"token": token,
		},
	}
	sendJSONResponse(w, resp)
}

// handleLogout handles user logout
func (s *APIServer) handleLogout(w http.ResponseWriter, r *http.Request) {
	// Get token from Authorization header
	token := r.Header.Get("Authorization")
	if token == "" {
		sendErrorResponse(w, "No token provided", http.StatusBadRequest)
		return
	}

	// Get auth manager
	authManager, err := auth.GetAuthManager(auth.Config{})
	if err != nil {
		sendErrorResponse(w, "Failed to initialize auth", http.StatusInternalServerError)
		return
	}

	// Invalidate token
	authManager.InvalidateToken(token)

	resp := Response{
		Success: true,
		Message: "Logout successful",
	}
	sendJSONResponse(w, resp)
}

// handleAuthCheck is a debug endpoint to check authentication status
func (s *APIServer) handleAuthCheck(w http.ResponseWriter, r *http.Request) {
	resp := Response{
		Success: true,
		Message: "Authentication check successful",
		Data: map[string]interface{}{
			"authenticated": true,
			"user":          r.Context().Value(auth.UserContextKey),
		},
	}
	sendJSONResponse(w, resp)
}

// Helper function to decode JSON body
func decodeJSONBody(r *http.Request, v interface{}) error {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		return err
	}
	return nil
}
