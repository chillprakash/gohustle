package utils

import (
	"fmt"
	"strconv"
	"strings"
)

// StringToUint32 converts a string to uint32, handling potential errors
func StringToUint32(s string) uint32 {
	// Parse the string as a 64-bit unsigned integer first
	val, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0
	}

	// Convert to uint32
	return uint32(val)
}

func StringSliceToUint32Slice(slice []string) []uint32 {
	result := make([]uint32, len(slice))
	for i, s := range slice {
		result[i] = StringToUint32(s)
	}
	return result
}

func RemoveDecimal(s string) string {
	return strings.Split(s, ".")[0]
}

func Uint32SliceToStringSlice(slice []uint32) []string {
	result := make([]string, len(slice))
	for i, s := range slice {
		result[i] = Uint32ToString(s)
	}
	return result
}

func StringToFloat64(s string) float64 {
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return val
}

// Uint32ToString converts a uint32 to string
func Uint32ToString(val uint32) string {
	return strconv.FormatUint(uint64(val), 10)
}

// Uint32SliceToInterfaceSlice converts a slice of uint32 to a slice of interface{}
func Uint32SliceToInterfaceSlice(values []uint32) []interface{} {
	interfaces := make([]interface{}, len(values))
	for i, v := range values {
		interfaces[i] = v
	}
	return interfaces
}

// InterfaceSliceToUint32Slice attempts to convert a slice of interface{} to a slice of uint32
func InterfaceSliceToUint32Slice(interfaces []interface{}) ([]uint32, error) {
	values := make([]uint32, len(interfaces))
	for i, v := range interfaces {
		switch val := v.(type) {
		case uint32:
			values[i] = val
		case int:
			values[i] = uint32(val)
		case int64:
			values[i] = uint32(val)
		case float64:
			values[i] = uint32(val)
		case string:
			values[i] = StringToUint32(val)
		default:
			return nil, fmt.Errorf("unsupported type at index %d: %T", i, v)
		}
	}
	return values, nil
}
