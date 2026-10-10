package console_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/madcok/hangtail/internal/console"
)

// newHandlerTo builds a Handler backed by a text writer into buf,
// picking up whatever env vars the caller has already set via t.Setenv.
func newHandlerTo(buf *bytes.Buffer) *console.Handler {
	inner := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return console.New(inner)
}

func TestLevelFilter_DropsBelow(t *testing.T) {
	t.Setenv("HANGTAIL_CONSOLE_LEVEL", "warn")
	var buf bytes.Buffer
	log := slog.New(newHandlerTo(&buf))

	log.Info("should be dropped")
	log.Warn("should pass")

	if strings.Contains(buf.String(), "should be dropped") {
		t.Error("info log should be filtered out by HANGTAIL_CONSOLE_LEVEL=warn")
	}
	if !strings.Contains(buf.String(), "should pass") {
		t.Error("warn log should pass HANGTAIL_CONSOLE_LEVEL=warn")
	}
}

func TestLevelFilter_ErrorPasses(t *testing.T) {
	t.Setenv("HANGTAIL_CONSOLE_LEVEL", "error")
	var buf bytes.Buffer
	log := slog.New(newHandlerTo(&buf))

	log.Warn("dropped")
	log.Error("kept")

	if strings.Contains(buf.String(), "dropped") {
		t.Error("warn should be dropped at level=error")
	}
	if !strings.Contains(buf.String(), "kept") {
		t.Error("error should pass at level=error")
	}
}

func TestIncludePattern(t *testing.T) {
	t.Setenv("HANGTAIL_CONSOLE_INCLUDE", `payment.*`)
	var buf bytes.Buffer
	log := slog.New(newHandlerTo(&buf))

	log.Info("payment processed")
	log.Info("healthcheck ok")

	if !strings.Contains(buf.String(), "payment processed") {
		t.Error("message matching INCLUDE should pass")
	}
	if strings.Contains(buf.String(), "healthcheck ok") {
		t.Error("message not matching INCLUDE should be dropped")
	}
}

func TestExcludePattern(t *testing.T) {
	t.Setenv("HANGTAIL_CONSOLE_EXCLUDE", "healthcheck")
	var buf bytes.Buffer
	log := slog.New(newHandlerTo(&buf))

	log.Info("healthcheck ok")
	log.Info("order created")

	if strings.Contains(buf.String(), "healthcheck ok") {
		t.Error("message matching EXCLUDE should be dropped")
	}
	if !strings.Contains(buf.String(), "order created") {
		t.Error("message not matching EXCLUDE should pass")
	}
}

func TestIncludeAndExclude(t *testing.T) {
	// include=order.*, exclude=order.cancelled → only non-cancelled orders pass
	t.Setenv("HANGTAIL_CONSOLE_INCLUDE", `order\..*`)
	t.Setenv("HANGTAIL_CONSOLE_EXCLUDE", `order\.cancelled`)
	var buf bytes.Buffer
	log := slog.New(newHandlerTo(&buf))

	log.Info("order.created")
	log.Info("order.cancelled")
	log.Info("payment.done")

	if !strings.Contains(buf.String(), "order.created") {
		t.Error("order.created should pass (matches include, not exclude)")
	}
	if strings.Contains(buf.String(), "order.cancelled") {
		t.Error("order.cancelled should be dropped by exclude")
	}
	if strings.Contains(buf.String(), "payment.done") {
		t.Error("payment.done should be dropped by include filter")
	}
}

func TestFlushOnError_WritesBufferedOnError(t *testing.T) {
	t.Setenv("HANGTAIL_CONSOLE_FLUSH_ON_ERROR", "true")
	var buf bytes.Buffer
	h := newHandlerTo(&buf)
	log := slog.New(h)

	ctx := console.WithBuffer(context.Background())
	log.InfoContext(ctx, "buffered message")

	if strings.Contains(buf.String(), "buffered message") {
		t.Error("message should not appear before Flush")
	}

	h.Flush(ctx, errors.New("request failed"))

	if !strings.Contains(buf.String(), "buffered message") {
		t.Error("message should appear after Flush with non-nil error")
	}
}

func TestFlushOnError_DiscardsOnSuccess(t *testing.T) {
	t.Setenv("HANGTAIL_CONSOLE_FLUSH_ON_ERROR", "true")
	var buf bytes.Buffer
	h := newHandlerTo(&buf)
	log := slog.New(h)

	ctx := console.WithBuffer(context.Background())
	log.InfoContext(ctx, "buffered message")
	h.Flush(ctx, nil) // nil = success → discard

	if strings.Contains(buf.String(), "buffered message") {
		t.Error("message should be discarded when Flush called with nil error")
	}
}

func TestFlushOnError_NoBuffer_WritesDirect(t *testing.T) {
	t.Setenv("HANGTAIL_CONSOLE_FLUSH_ON_ERROR", "true")
	var buf bytes.Buffer
	log := slog.New(newHandlerTo(&buf))

	// No WithBuffer — records should be written directly even with flushOnErr=true
	log.Info("direct message")

	if !strings.Contains(buf.String(), "direct message") {
		t.Error("message outside a buffered context should be written immediately")
	}
}

func TestFlushNoop_NoContext(t *testing.T) {
	t.Setenv("HANGTAIL_CONSOLE_FLUSH_ON_ERROR", "true")
	var buf bytes.Buffer
	h := newHandlerTo(&buf)

	// Flush on a plain context with an error should not panic.
	h.Flush(context.Background(), errors.New("oops"))
}

func TestWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(newHandlerTo(&buf)).With("component", "worker")
	log.Info("attrs test")

	if !strings.Contains(buf.String(), "component=worker") {
		t.Error("WithAttrs should propagate to inner handler output")
	}
}

func TestWithGroup(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(newHandlerTo(&buf)).WithGroup("req")
	log.Info("group test", "id", "x")

	if !strings.Contains(buf.String(), "req.id=x") {
		t.Error("WithGroup should prefix attribute keys")
	}
}
