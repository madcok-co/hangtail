// api-go is the demo HTTP gateway for hangtail.
// It accepts POST /order, enqueues the job to Redis (orders:queue) with
// W3C trace context, and returns immediately.  Downstream processing
// (worker-py → notifier-js) completes the flow and sends flow.end=true.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	otellog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/propagation"
)

var (
	tracer trace.Tracer
	logger *slog.Logger
	rdb    *redis.Client
)

func main() {
	shutdown := initOTel()
	defer shutdown()

	tracer = otel.Tracer("api-go")
	logger = slog.New(otelslog.NewHandler("api-go"))

	rdb = redis.NewClient(&redis.Options{
		Addr: envOrDefault("REDIS_ADDR", "redis:6379"),
	})

	http.HandleFunc("/order", handleOrder)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if err := rdb.Ping(context.Background()).Err(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	slog.Info("api-go listening", "addr", ":8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}

func handleOrder(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "handle_order")
	defer span.End()

	orderID := fmt.Sprintf("ord-%d", time.Now().UnixNano())
	span.SetAttributes(attribute.String("order_id", orderID))

	logger.InfoContext(ctx, "order received",
		slog.String("order_id", orderID),
		slog.String("remote_addr", r.RemoteAddr),
	)

	// Inject W3C trace context so downstream services share the same trace_id
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	payload, _ := json.Marshal(map[string]string{
		"order_id":   orderID,
		"traceparent": carrier["traceparent"],
	})

	if err := rdb.LPush(ctx, "orders:queue", payload).Err(); err != nil {
		span.RecordError(err)
		logger.ErrorContext(ctx, "enqueue failed",
			slog.String("order_id", orderID),
			slog.String("error", err.Error()),
		)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	logger.InfoContext(ctx, "order queued",
		slog.String("order_id", orderID),
	)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"order_id":%q,"status":"queued"}`, orderID)
}

func initOTel() func() {
	ctx := context.Background()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(envOrDefault("OTEL_SERVICE_NAME", "api-go")),
			semconv.ServiceVersion("0.1.0"),
		),
	)
	if err != nil {
		slog.Error("otel resource", "err", err)
		os.Exit(1)
	}

	traceExp, err := otlptracehttp.New(ctx)
	if err != nil {
		slog.Error("trace exporter", "err", err)
		os.Exit(1)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	// W3C TraceContext propagator — required for cross-service trace_id sharing
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
	))

	logExp, err := otlploghttp.New(ctx)
	if err != nil {
		slog.Error("log exporter", "err", err)
		os.Exit(1)
	}
	lp := otellog.NewLoggerProvider(
		otellog.WithProcessor(otellog.NewBatchProcessor(logExp)),
		otellog.WithResource(res),
	)
	global.SetLoggerProvider(lp)

	return func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutCtx)
		_ = lp.Shutdown(shutCtx)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
