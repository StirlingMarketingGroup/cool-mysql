package mysql

import "go.uber.org/zap"

// ZapLogger adapts zap.Logger to the Logger interface.
type ZapLogger struct{ *zap.Logger }

func (l ZapLogger) Debug(msg string, args ...any) { l.Logger.Sugar().Debugf(msg, args...) }
func (l ZapLogger) Info(msg string, args ...any)  { l.Logger.Sugar().Infof(msg, args...) }
func (l ZapLogger) Warn(msg string, args ...any)  { l.Logger.Sugar().Warnf(msg, args...) }
func (l ZapLogger) Error(msg string, args ...any) { l.Logger.Sugar().Errorf(msg, args...) }
