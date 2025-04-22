package api

import (
	"encoding/json"
	"gohustle/auth"
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
