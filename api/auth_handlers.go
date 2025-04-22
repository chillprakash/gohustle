package api

import (
	"encoding/json"
	"gohustle/auth"
	"gohustle/config"
	"net/http"
)

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Token string `json:"token"`
}

// handleLogin processes login requests
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		SendErrorResponse(w, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	// Get config for credentials
	cfg := config.GetConfig()
	if cfg.Auth.Username != req.Username || cfg.Auth.Password != req.Password {
		SendErrorResponse(w, http.StatusUnauthorized, "Invalid credentials", nil)
		return
	}

	// Generate JWT token
	token, err := auth.GenerateToken(req.Username, req.Password)
	if err != nil {
		SendErrorResponse(w, http.StatusUnauthorized, "Invalid credentials", nil)
		return
	}

	resp := Response{
		Success: true,
		Message: "Login successful",
		Data: LoginResponse{
			Token: token,
		},
	}

	SendJSONResponse(w, http.StatusOK, resp)
}

// handleLogout processes logout requests (client-side only)
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	// Since JWT is stateless, logout is handled client-side
	// by removing the token. We just send a success response.
	resp := Response{
		Success: true,
		Message: "Logout successful",
	}

	SendJSONResponse(w, http.StatusOK, resp)
}

// handleAuthCheck returns current auth configuration (debug endpoint)
func (s *Server) handleAuthCheck(w http.ResponseWriter, r *http.Request) {
	cfg := config.GetConfig()
	resp := Response{
		Success: true,
		Message: "Auth configuration",
		Data: map[string]interface{}{
			"configured_username": cfg.Auth.Username,
		},
	}

	SendJSONResponse(w, http.StatusOK, resp)
}
