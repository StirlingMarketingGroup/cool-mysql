package mysql

import (
	"os"
	"reflect"
	"strconv"
)

func isNil(a any) bool {
	defer func() { recover() }()
	return a == nil || reflect.ValueOf(a).IsNil()
}

// getEnvInt64 gets an environment variable with a default int64
func getEnvInt64(key string, fallback int64) int64 {
	if value, ok := os.LookupEnv(key); ok {
		i, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return int64(i)
		}
	}
	return fallback
}

// getEnvFloat gets an environment variable with a default float64
func getEnvFloat(key string, fallback float64) float64 {
	if value, ok := os.LookupEnv(key); ok {
		n, err := strconv.ParseFloat(value, 64)
		if err == nil {
			return n
		}
	}
	return fallback
}
