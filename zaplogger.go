package mysql

import "go.uber.org/zap"

// ZapLogger adapts zap.Logger to the Logger interface.
type ZapLogger struct{ *zap.Logger }

func (l ZapLogger) Debug(msg string, args ...any) { l.Logger.Sugar().Debugw(msg, args...) }
func (l ZapLogger) Info(msg string, args ...any)  { l.Logger.Sugar().Infow(msg, args...) }
func (l ZapLogger) Warn(msg string, args ...any)  { l.Logger.Sugar().Warnw(msg, args...) }
func (l ZapLogger) Error(msg string, args ...any) { l.Logger.Sugar().Errorw(msg, args...) }
