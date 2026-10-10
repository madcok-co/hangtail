package console

import (
	"context"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
)

// Handler is a slog.Handler that:
//   - filters records by minimum level (HANGTAIL_CONSOLE_LEVEL)
//   - filters records by include / exclude regex on the message (HANGTAIL_CONSOLE_INCLUDE / EXCLUDE)
//   - optionally buffers records per request-context and only prints them to
//     the inner handler when the request ends with an error (HANGTAIL_CONSOLE_FLUSH_ON_ERROR=true)
type Handler struct {
	inner      slog.Handler
	minLevel   slog.Level
	include    *regexp.Regexp
	exclude    *regexp.Regexp
	flushOnErr bool
}

type bufKey struct{}

type requestBuffer struct {
	mu      sync.Mutex
	records []slog.Record
}

// New creates a Handler wrapping inner, reading filter config from env vars.
// If inner is nil, a JSON handler writing to os.Stdout is used.
//
// Env vars:
//
//	HANGTAIL_CONSOLE_LEVEL         minimum level: debug|info|warn|error (default: debug)
//	HANGTAIL_CONSOLE_INCLUDE       regex — only messages matching this pattern are printed
//	HANGTAIL_CONSOLE_EXCLUDE       regex — messages matching this pattern are dropped
//	HANGTAIL_CONSOLE_FLUSH_ON_ERROR "true" enables per-request buffering
func New(inner slog.Handler) *Handler {
	if inner == nil {
		inner = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	}
	h := &Handler{inner: inner, minLevel: slog.LevelDebug}

	if lvl := os.Getenv("HANGTAIL_CONSOLE_LEVEL"); lvl != "" {
		var l slog.Level
		if err := l.UnmarshalText([]byte(strings.ToUpper(lvl))); err == nil {
			h.minLevel = l
		}
	}
	if s := os.Getenv("HANGTAIL_CONSOLE_INCLUDE"); s != "" {
		if re, err := regexp.Compile(s); err == nil {
			h.include = re
		}
	}
	if s := os.Getenv("HANGTAIL_CONSOLE_EXCLUDE"); s != "" {
		if re, err := regexp.Compile(s); err == nil {
			h.exclude = re
		}
	}
	h.flushOnErr = os.Getenv("HANGTAIL_CONSOLE_FLUSH_ON_ERROR") == "true"
	return h
}

// WithBuffer returns ctx carrying a per-request record buffer.
// Records handled while this context is active (and flushOnErr=true) are
// held until Flush is called rather than written immediately.
func WithBuffer(ctx context.Context) context.Context {
	return context.WithValue(ctx, bufKey{}, &requestBuffer{})
}

// Flush writes buffered records to the inner handler when err != nil.
// When err is nil the buffer is silently discarded.
// Calling Flush on a context that was not created by WithBuffer is a no-op.
func (h *Handler) Flush(ctx context.Context, err error) {
	buf, ok := ctx.Value(bufKey{}).(*requestBuffer)
	if !ok {
		return
	}
	buf.mu.Lock()
	records := make([]slog.Record, len(buf.records))
	copy(records, buf.records)
	buf.records = buf.records[:0]
	buf.mu.Unlock()
	if err == nil {
		return
	}
	for _, r := range records {
		_ = h.inner.Handle(ctx, r)
	}
}

var _ slog.Handler = (*Handler)(nil)

func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.minLevel
}

func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level < h.minLevel {
		return nil
	}
	if !h.passes(r.Message) {
		return nil
	}
	if h.flushOnErr {
		if buf, ok := ctx.Value(bufKey{}).(*requestBuffer); ok {
			buf.mu.Lock()
			buf.records = append(buf.records, r.Clone())
			buf.mu.Unlock()
			return nil
		}
	}
	return h.inner.Handle(ctx, r)
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &Handler{
		inner:      h.inner.WithAttrs(attrs),
		minLevel:   h.minLevel,
		include:    h.include,
		exclude:    h.exclude,
		flushOnErr: h.flushOnErr,
	}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{
		inner:      h.inner.WithGroup(name),
		minLevel:   h.minLevel,
		include:    h.include,
		exclude:    h.exclude,
		flushOnErr: h.flushOnErr,
	}
}

func (h *Handler) passes(msg string) bool {
	if h.exclude != nil && h.exclude.MatchString(msg) {
		return false
	}
	if h.include != nil && !h.include.MatchString(msg) {
		return false
	}
	return true
}
