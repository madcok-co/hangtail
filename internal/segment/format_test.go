package segment_test

import (
	"bytes"
	"io"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/madcok/hangtail/internal/segment"
)

func TestRoundtripLogs(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-svc")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr("hello hangtail")
	lr.Attributes().PutStr("trace_id", "abc123")

	payload, err := segment.MarshalLogs(ld)
	if err != nil {
		t.Fatalf("MarshalLogs: %v", err)
	}

	var buf bytes.Buffer
	if err := segment.EncodeRecord(&buf, segment.SignalLogs, payload); err != nil {
		t.Fatalf("EncodeRecord: %v", err)
	}

	sig, got, err := segment.DecodeRecord(&buf)
	if err != nil {
		t.Fatalf("DecodeRecord: %v", err)
	}
	if sig != segment.SignalLogs {
		t.Fatalf("signal: got %d, want %d", sig, segment.SignalLogs)
	}

	ld2, err := segment.UnmarshalLogs(got)
	if err != nil {
		t.Fatalf("UnmarshalLogs: %v", err)
	}
	if ld2.LogRecordCount() != 1 {
		t.Fatalf("LogRecordCount: got %d, want 1", ld2.LogRecordCount())
	}
	body := ld2.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
	if body != "hello hangtail" {
		t.Fatalf("body: got %q, want %q", body, "hello hangtail")
	}
}

func TestRoundtripTraces(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-svc")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("test-span")

	payload, err := segment.MarshalTraces(td)
	if err != nil {
		t.Fatalf("MarshalTraces: %v", err)
	}

	var buf bytes.Buffer
	if err := segment.EncodeRecord(&buf, segment.SignalTraces, payload); err != nil {
		t.Fatalf("EncodeRecord: %v", err)
	}

	sig, got, err := segment.DecodeRecord(&buf)
	if err != nil {
		t.Fatalf("DecodeRecord: %v", err)
	}
	if sig != segment.SignalTraces {
		t.Fatalf("signal: got %d, want %d", sig, segment.SignalTraces)
	}

	td2, err := segment.UnmarshalTraces(got)
	if err != nil {
		t.Fatalf("UnmarshalTraces: %v", err)
	}
	if td2.SpanCount() != 1 {
		t.Fatalf("SpanCount: got %d, want 1", td2.SpanCount())
	}
	name := td2.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name()
	if name != "test-span" {
		t.Fatalf("span name: got %q, want %q", name, "test-span")
	}
}

func TestMultipleRecords(t *testing.T) {
	var buf bytes.Buffer
	const n = 5
	for i := 0; i < n; i++ {
		payload := []byte{byte('A' + i)}
		if err := segment.EncodeRecord(&buf, segment.SignalLogs, payload); err != nil {
			t.Fatalf("EncodeRecord[%d]: %v", i, err)
		}
	}

	r := bytes.NewReader(buf.Bytes())
	count := 0
	for {
		_, _, err := segment.DecodeRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("DecodeRecord[%d]: %v", count, err)
		}
		count++
	}
	if count != n {
		t.Fatalf("count: got %d, want %d", count, n)
	}
}

func TestDecodeEOFOnEmpty(t *testing.T) {
	_, _, err := segment.DecodeRecord(bytes.NewReader(nil))
	if err != io.EOF {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

func TestDecodeTruncatedHeader(t *testing.T) {
	// Only 3 bytes — header needs 5.
	_, _, err := segment.DecodeRecord(bytes.NewReader([]byte{0, 0, 0}))
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestDecodeTruncatedPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := segment.EncodeRecord(&buf, segment.SignalLogs, []byte("full payload here")); err != nil {
		t.Fatal(err)
	}
	// Chop 5 bytes off the end so payload is incomplete.
	truncated := buf.Bytes()[:buf.Len()-5]
	_, _, err := segment.DecodeRecord(bytes.NewReader(truncated))
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
	}
}
