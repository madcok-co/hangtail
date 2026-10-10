package bench_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/madcok/hangtail/internal/completedset"
	"github.com/madcok/hangtail/internal/segment"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func makeLogBatch(n int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "bench")
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < n; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.Body().SetStr("benchmark log record")
		var tid [16]byte
		tid[0] = byte(i)
		tid[1] = byte(i >> 8)
		lr.SetTraceID(pcommon.TraceID(tid))
	}
	return ld
}

func makeTraceBatch(n int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "bench")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < n; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetName("bench-span")
		sp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		sp.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(time.Millisecond)))
		var tid [16]byte
		tid[0] = byte(i)
		sp.SetTraceID(pcommon.TraceID(tid))
	}
	return td
}

// ── segment writer ────────────────────────────────────────────────────────────

func BenchmarkSegmentWrite_Logs(b *testing.B) {
	dir := b.TempDir()
	w, err := segment.NewWriter(filepath.Join(dir, "bench.seg"), time.Second)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { w.Close() })

	ld := makeLogBatch(10)
	payload, _ := segment.MarshalLogs(ld)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := w.Write(segment.SignalLogs, payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	// Count log records per op for throughput reporting.
	b.ReportMetric(float64(10*b.N)/b.Elapsed().Seconds(), "records/s")
}

func BenchmarkSegmentWrite_Traces(b *testing.B) {
	dir := b.TempDir()
	w, _ := segment.NewWriter(filepath.Join(dir, "bench.seg"), time.Second)
	b.Cleanup(func() { w.Close() })

	td := makeTraceBatch(10)
	payload, _ := segment.MarshalTraces(td)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := w.Write(segment.SignalTraces, payload); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(10*b.N)/b.Elapsed().Seconds(), "spans/s")
}

// ── marshal / unmarshal ───────────────────────────────────────────────────────

func BenchmarkMarshalLogs(b *testing.B) {
	ld := makeLogBatch(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := segment.MarshalLogs(ld); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshalLogs(b *testing.B) {
	payload, _ := segment.MarshalLogs(makeLogBatch(100))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := segment.UnmarshalLogs(payload); err != nil {
			b.Fatal(err)
		}
	}
}

// ── completedset ──────────────────────────────────────────────────────────────

func BenchmarkCompletedSet_Add(b *testing.B) {
	cs := completedset.New(time.Hour)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cs.Add(traceIDStr(i), nil)
	}
}

func BenchmarkCompletedSet_Contains_Hit(b *testing.B) {
	cs := completedset.New(time.Hour)
	for i := 0; i < 10_000; i++ {
		cs.Add(traceIDStr(i), nil)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cs.Contains(traceIDStr(i % 10_000))
	}
}

func BenchmarkCompletedSet_Contains_Miss(b *testing.B) {
	cs := completedset.New(time.Hour)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cs.Contains(traceIDStr(i))
	}
}

// ── segment scan (reaper read path) ──────────────────────────────────────────

func BenchmarkScanSegment(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.seg")

	// Pre-fill with 1000 log batches of 10 records each.
	w, _ := segment.NewWriter(path, time.Second)
	ld := makeLogBatch(10)
	payload, _ := segment.MarshalLogs(ld)
	for i := 0; i < 1000; i++ {
		w.Write(segment.SignalLogs, payload)
	}
	w.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := segment.ScanSegment(path); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(1000*b.N)/b.Elapsed().Seconds(), "batches/s")
}

// ── console handler ───────────────────────────────────────────────────────────

// BenchmarkConsoleHandler_Direct measures the overhead of our slog wrapper
// relative to writing directly when no buffering is active.
func BenchmarkConsoleHandler_Direct(b *testing.B) {
	// Tested separately to avoid importing console from bench package;
	// see internal/console/handler_test.go for correctness tests.
	// This benchmark is a placeholder that confirms build works.
	b.Skip("run via: go test -bench=. ./internal/console/...")
}

// ── full pipeline sim ─────────────────────────────────────────────────────────

// BenchmarkFullPipeline simulates the hot path: marshal → write → scan → unmarshal.
func BenchmarkFullPipeline(b *testing.B) {
	dir := b.TempDir()
	cs := completedset.New(time.Hour)
	_ = cs
	_ = context.Background()

	ld := makeLogBatch(50)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(dir, "pipe.seg")
		w, _ := segment.NewWriter(path, time.Second)
		payload, _ := segment.MarshalLogs(ld)
		w.Write(segment.SignalLogs, payload)
		w.Close()

		records, _ := segment.ScanSegment(path)
		for _, rec := range records {
			if rec.Signal == segment.SignalLogs {
				segment.UnmarshalLogs(rec.Payload)
			}
		}
	}
	b.ReportMetric(float64(50*b.N)/b.Elapsed().Seconds(), "records/s")
}

// ── helpers ───────────────────────────────────────────────────────────────────

func traceIDStr(i int) string {
	var b [16]byte
	b[0] = byte(i)
	b[1] = byte(i >> 8)
	b[2] = byte(i >> 16)
	b[3] = byte(i >> 24)
	return pcommon.TraceID(b).String()
}
