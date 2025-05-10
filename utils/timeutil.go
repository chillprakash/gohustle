package utils

import (
	"fmt"
	"time"
)

// Date format constants
const (
	// Kite API date format (DD-MM-YYYY)
	KiteDateFormat = "02-01-2006"
)

// IST returns the IST timezone location object.
func IST() *time.Location {
	return time.FixedZone("IST", 5*60*60+30*60)
}

// NowIST returns the current time in IST.
func NowIST() time.Time {
	return time.Now().In(IST())
}

// ToIST converts any time.Time to IST.
func ToIST(t time.Time) time.Time {
	return t.In(IST())
}

func FormatKiteDate(t time.Time) string {
	return t.Format(KiteDateFormat)
}

func ParseKiteDate(dateStr string) (time.Time, error) {
	return time.Parse(KiteDateFormat, dateStr)
}

func GetCurrentKiteDate() string {
	return NowIST().Format(KiteDateFormat)
}

// GetTodayIST returns the current date in IST truncated to 24 hours.
func GetTodayIST() time.Time {
	return NowIST().Truncate(24 * time.Hour)
}

// StringSliceToInterfaceSlice converts a string slice to an interface slice
// This is useful for Redis operations that require variadic interface arguments
func StringSliceToInterfaceSlice(strings []string) []interface{} {
	result := make([]interface{}, len(strings))
	for i, s := range strings {
		result[i] = s
	}
	return result
}

// GetNearestFutureDate finds the nearest future date from a slice of date strings in Kite format
func GetNearestFutureDate(dateStrs []string) (string, error) {
	if len(dateStrs) == 0 {
		return "", fmt.Errorf("empty date slice")
	}
	
	today := GetTodayIST()
	var nearestExpiry string
	var nearestDate time.Time
	
	for _, dateStr := range dateStrs {
		date, err := ParseKiteDate(dateStr)
		if err != nil {
			return "", fmt.Errorf("failed to parse date %s: %w", dateStr, err)
		}
		
		// Skip dates in the past
		if date.Before(today) {
			continue
		}
		
		// If this is the first valid date or it's earlier than our current nearest
		if nearestExpiry == "" || date.Before(nearestDate) {
			nearestExpiry = dateStr
			nearestDate = date
		}
	}
	
	if nearestExpiry == "" {
		return "", fmt.Errorf("no future dates found")
	}
	
	return nearestExpiry, nil
}
