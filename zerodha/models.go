package zerodha

import "time"

// LoginResponse represents the login API response
type LoginResponse struct {
	Status string `json:"status"`
	Data   struct {
		UserID     string   `json:"user_id"`
		RequestID  string   `json:"request_id"`
		TwoFAType  string   `json:"twofa_type"`
		TwoFATypes []string `json:"twofa_types"`
		Profile    struct {
			UserName      string `json:"user_name"`
			UserShortname string `json:"user_shortname"`
			AvatarURL     string `json:"avatar_url"`
		} `json:"profile"`
	} `json:"data"`
}

// InstrumentExpiry represents a row in the instrument_expiries table
type InstrumentExpiry struct {
	ID             int64     `db:"id"`
	InstrumentName string    `db:"instrument_name"`
	ExpiryDate     time.Time `db:"expiry_date"`
	CreatedAt      time.Time `db:"created_at"`
	UpdatedAt      time.Time `db:"updated_at"`
	IsActive       bool      `db:"is_active"`
}

// ExpiryData represents expiry information for an instrument
type ExpiryData struct {
	Symbol       string
	StrikesCount int
}

// ExpiryDetails contains detailed expiry information
type ExpiryDetails struct {
	Date          time.Time
	StrikesCount  int
	InstrumentMap map[string][]string // map[instrument_type][]tradingsymbols
}
