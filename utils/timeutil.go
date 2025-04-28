package utils

import "time"

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
