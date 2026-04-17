// Package logging provides a structured slog-based logger. It is the single
// way any component in this project should log: no `log.Printf`, no `fmt.Println`
// from production code paths. The logger is built from LoggingConfig and can
// emit either human-readable text or JSON (for shipping to log aggregators).
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// Options controls logger construction. Mirrors config.LoggingConfig but kept
// here to avoid importing config from low-level packages.
type Options struct {
	Level  string
	Format string
	Writer io.Writer // defaults to os.Stdout when nil
}

// New constructs a *slog.Logger based on the given options.
// Unknown levels fall back to info. Unknown formats fall back to text.
func New(opts Options) *slog.Logger {
	w := opts.Writer
	if w == nil {
		w = os.Stdout
	}

	var level slog.Level
	switch strings.ToLower(opts.Level) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	handlerOpts := &slog.HandlerOptions{
		Level:     level,
		AddSource: level == slog.LevelDebug,
	}

	var h slog.Handler
	if strings.EqualFold(opts.Format, "json") {
		h = slog.NewJSONHandler(w, handlerOpts)
	} else {
		h = slog.NewTextHandler(w, handlerOpts)
	}

	return slog.New(h)
}

// With returns a child logger enriched with the given attributes.
// Provided as a thin helper so callers don't need to import slog directly
// for common cases.
func With(l *slog.Logger, attrs ...any) *slog.Logger {
	return l.With(attrs...)
}
