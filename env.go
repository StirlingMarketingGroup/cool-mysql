package mysql

import (
	"os"
	"strconv"
)

// getenv gets an environment variable with a default string
func getenv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// getenvInt64 gets an environment variable with a default int64
func getenvInt64(key string, fallback int64) int64 {
	if value, ok := os.LookupEnv(key); ok {
		i, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return int64(i)
		}
	}
	return fallback
}

// getenvFloat gets an environment variable with a default float64
func getenvFloat(key string, fallback float64) float64 {
	if value, ok := os.LookupEnv(key); ok {
		n, err := strconv.ParseFloat(value, 64)
		if err == nil {
			return n
		}
	}
	return fallback
}
