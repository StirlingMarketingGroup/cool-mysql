package mysql

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type recordingLogger struct {
	mu        sync.Mutex
	warnCount int
}

func (l *recordingLogger) Debug(string, ...any) {}
func (l *recordingLogger) Info(string, ...any)  {}
func (l *recordingLogger) Error(string, ...any) {}

func (l *recordingLogger) Warn(string, ...any) {
	l.mu.Lock()
	l.warnCount++
	l.mu.Unlock()
}

func TestFloatPrecisionWarningLogged(t *testing.T) {
	rec := &recordingLogger{}

	floatPrecisionLoggerMu.Lock()
	original := floatPrecisionLogger
	floatPrecisionLogger = rec
	floatPrecisionLoggerMu.Unlock()
	t.Cleanup(func() {
		floatPrecisionLoggerMu.Lock()
		floatPrecisionLogger = original
		floatPrecisionLoggerMu.Unlock()
	})

	threshold := float64(maxExactUint64Float) + 1024
	_, err := floatToUint64(threshold, 64)
	require.NoError(t, err)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Equal(t, 1, rec.warnCount, "precision warning should be logged once")
}
