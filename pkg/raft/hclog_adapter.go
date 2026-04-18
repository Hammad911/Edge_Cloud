package raft

import (
	"io"
	"log"
	"log/slog"

	hclog "github.com/hashicorp/go-hclog"
)

// hclogAdapter bridges hashicorp/go-hclog (used throughout the raft library)
// to our structured slog.Logger. We keep the mapping conservative: hclog's
// Trace collapses to Debug, and Off silences output.
type hclogAdapter struct {
	log   *slog.Logger
	name  string
	level hclog.Level
	impl  hclog.StandardLoggerOptions
}

func newHclogAdapter(l *slog.Logger) hclog.Logger {
	return &hclogAdapter{log: l, level: hclog.Info}
}

func (a *hclogAdapter) Log(level hclog.Level, msg string, args ...interface{}) {
	switch level {
	case hclog.Trace, hclog.Debug:
		a.log.Debug(msg, args...)
	case hclog.Info:
		a.log.Info(msg, args...)
	case hclog.Warn:
		a.log.Warn(msg, args...)
	case hclog.Error:
		a.log.Error(msg, args...)
	case hclog.NoLevel, hclog.Off:
		// drop
	}
}

func (a *hclogAdapter) Trace(msg string, args ...interface{}) { a.log.Debug(msg, args...) }
func (a *hclogAdapter) Debug(msg string, args ...interface{}) { a.log.Debug(msg, args...) }
func (a *hclogAdapter) Info(msg string, args ...interface{})  { a.log.Info(msg, args...) }
func (a *hclogAdapter) Warn(msg string, args ...interface{})  { a.log.Warn(msg, args...) }
func (a *hclogAdapter) Error(msg string, args ...interface{}) { a.log.Error(msg, args...) }

func (a *hclogAdapter) IsTrace() bool { return a.level <= hclog.Trace }
func (a *hclogAdapter) IsDebug() bool { return a.level <= hclog.Debug }
func (a *hclogAdapter) IsInfo() bool  { return a.level <= hclog.Info }
func (a *hclogAdapter) IsWarn() bool  { return a.level <= hclog.Warn }
func (a *hclogAdapter) IsError() bool { return a.level <= hclog.Error }

func (a *hclogAdapter) ImpliedArgs() []interface{} { return nil }

func (a *hclogAdapter) With(args ...interface{}) hclog.Logger {
	return &hclogAdapter{log: a.log.With(args...), name: a.name, level: a.level}
}

func (a *hclogAdapter) Name() string { return a.name }

func (a *hclogAdapter) Named(name string) hclog.Logger {
	n := a.name
	if n != "" {
		n += "." + name
	} else {
		n = name
	}
	return &hclogAdapter{log: a.log.With(slog.String("logger", n)), name: n, level: a.level}
}

func (a *hclogAdapter) ResetNamed(name string) hclog.Logger {
	return &hclogAdapter{log: a.log.With(slog.String("logger", name)), name: name, level: a.level}
}

func (a *hclogAdapter) SetLevel(level hclog.Level) { a.level = level }

func (a *hclogAdapter) GetLevel() hclog.Level { return a.level }

func (a *hclogAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	if opts != nil {
		a.impl = *opts
	}
	return log.New(a.StandardWriter(opts), "", 0)
}

func (a *hclogAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return &stdWriter{log: a.log}
}

type stdWriter struct{ log *slog.Logger }

func (w *stdWriter) Write(p []byte) (int, error) {
	msg := string(p)
	for len(msg) > 0 && (msg[len(msg)-1] == '\n' || msg[len(msg)-1] == '\r') {
		msg = msg[:len(msg)-1]
	}
	if msg != "" {
		w.log.Info(msg)
	}
	return len(p), nil
}
