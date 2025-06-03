package mysql

import (
	"log/slog"
	"os"
)

// Logger defines the minimal logging interface used by this package.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// SlogLogger adapts slog.Logger to the Logger interface.
type SlogLogger struct{ *slog.Logger }

func (l SlogLogger) Debug(msg string, args ...any) { l.Logger.Debug(msg, args...) }
func (l SlogLogger) Info(msg string, args ...any)  { l.Logger.Info(msg, args...) }
func (l SlogLogger) Warn(msg string, args ...any)  { l.Logger.Warn(msg, args...) }
func (l SlogLogger) Error(msg string, args ...any) { l.Logger.Error(msg, args...) }

// DefaultLogger returns a slog-based logger used when none is provided.
func DefaultLogger() Logger {
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	return SlogLogger{slog.New(h).With("module", "cool-mysql")}
}
