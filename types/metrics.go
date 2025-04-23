package types

// MetricsData represents the calculated metrics for storage
type MetricsData struct {
	Timestamp       int64
	UnderlyingPrice float64
	SyntheticFuture float64
	LowestStraddle  float64
	ATMStrike       float64
}
