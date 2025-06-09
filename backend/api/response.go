package api

import (
	"encoding/json"
	"net/http"
)

// Response is the standard response structure for all API responses
type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// SendSuccess sends a successful JSON response
func SendSuccess(w http.ResponseWriter, data interface{}, message string) {
	respond(w, http.StatusOK, Response{
		Success: true,
		Message: message,
		Data:    data,
	})
}

// SendError sends an error JSON response
func SendError(w http.ResponseWriter, statusCode int, errorMsg string, message string) {
	respond(w, statusCode, Response{
		Success: false,
		Message: message,
		Error:   errorMsg,
	})
}

// SendValidationError sends a 422 Unprocessable Entity response for validation errors
func SendValidationError(w http.ResponseWriter, errorMsg string) {
	SendError(w, http.StatusUnprocessableEntity, errorMsg, "Validation failed")
}

// SendNotFound sends a 404 Not Found response
func SendNotFound(w http.ResponseWriter, resource string) {
	SendError(w, http.StatusNotFound, "Resource not found",
		resource+" not found")
}

// SendUnauthorized sends a 401 Unauthorized response
func SendUnauthorized(w http.ResponseWriter) {
	SendError(w, http.StatusUnauthorized, "Authentication required",
		"Please provide valid authentication credentials")
}

// SendForbidden sends a 403 Forbidden response
func SendForbidden(w http.ResponseWriter) {
	SendError(w, http.StatusForbidden, "Access denied",
		"You don't have permission to access this resource")
}

// SendInternalServerError sends a 500 Internal Server Error response
func SendInternalServerError(w http.ResponseWriter, err error) {
	// Log the actual error for debugging
	// log.Printf("Internal server error: %v", err)

	SendError(w, http.StatusInternalServerError,
		"An internal server error occurred",
		"Something went wrong. Please try again later.")
}

// respond is a helper function to send JSON responses
func respond(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if data != nil {
		encodeErr := json.NewEncoder(w).Encode(data)
		if encodeErr != nil {
			// If there's an error encoding the response, log it
			http.Error(w, "Error encoding response", http.StatusInternalServerError)
		}
	}
}
