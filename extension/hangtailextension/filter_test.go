package hangtailextension

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/madcok/hangtail/internal/completedset"
	"github.com/madcok/hangtail/internal/segment"
)

func traceID(hex string) pcommon.TraceID {
	var b [16]byte
	for i := 0; i < 16 && i*2+1 < len(hex); i++ {
		hi := hexVal(hex[i*2])
		lo := hexVal(hex[i*2+1])
		b[i] = hi<<4 | lo
	}
	return pcommon.TraceID(b)
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	}
	return 0
}

func makeLog(tid pcommon.TraceID, sev plog.SeverityNumber) plog.Logs {
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTraceID(tid)
	lr.SetSeverityNumber(sev)
	return ld
}

func makeSpan(tid pcommon.TraceID, errStatus bool) ptrace.Traces {
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(tid)
	if errStatus {
		sp.Status().SetCode(ptrace.StatusCodeError)
	}
	return td
}

var (
	tid1 = traceID("01000000000000000000000000000000")
	tid2 = traceID("02000000000000000000000000000000")
)

func TestFilterLogs_IncompletePasses(t *testing.T) {
	cs := completedset.New(time.Hour)
	// tid1 NOT in completed set → should be forwarded
	ld := makeLog(tid1, plog.SeverityNumberInfo)
	out := filterLogs(ld, cs, nil, true)
	if out.LogRecordCount() != 1 {
		t.Fatalf("want 1, got %d", out.LogRecordCount())
	}
}

func TestFilterLogs_CompletedDropped(t *testing.T) {
	cs := completedset.New(time.Hour)
	cs.Add(tid1.String(), nil)
	ld := makeLog(tid1, plog.SeverityNumberInfo)
	out := filterLogs(ld, cs, nil, true)
	if out.LogRecordCount() != 0 {
		t.Fatalf("completed trace should be dropped, got %d", out.LogRecordCount())
	}
}

func TestFilterLogs_KeepOnError(t *testing.T) {
	cs := completedset.New(time.Hour)
	cs.Add(tid1.String(), nil)
	// errorTraces says tid1 has an error → force forward
	errorTraces := map[string]bool{tid1.String(): true}
	ld := makeLog(tid1, plog.SeverityNumberError)
	out := filterLogs(ld, cs, errorTraces, true)
	if out.LogRecordCount() != 1 {
		t.Fatalf("error trace should pass keep_on_error, got %d", out.LogRecordCount())
	}
}

func TestFilterLogs_UntracedForward(t *testing.T) {
	cs := completedset.New(time.Hour)
	ld := makeLog(pcommon.TraceID{}, plog.SeverityNumberInfo) // empty trace_id
	out := filterLogs(ld, cs, nil, true /* forwardUntraced */)
	if out.LogRecordCount() != 1 {
		t.Fatalf("untraced should be forwarded, got %d", out.LogRecordCount())
	}
}

func TestFilterLogs_UntracedDrop(t *testing.T) {
	cs := completedset.New(time.Hour)
	ld := makeLog(pcommon.TraceID{}, plog.SeverityNumberInfo)
	out := filterLogs(ld, cs, nil, false /* forwardUntraced=false */)
	if out.LogRecordCount() != 0 {
		t.Fatalf("untraced should be dropped, got %d", out.LogRecordCount())
	}
}

func TestFilterTraces_CompletedDropped(t *testing.T) {
	cs := completedset.New(time.Hour)
	cs.Add(tid2.String(), nil)
	td := makeSpan(tid2, false)
	out := filterTraces(td, cs, nil, true)
	if out.SpanCount() != 0 {
		t.Fatalf("completed span should be dropped, got %d", out.SpanCount())
	}
}

func TestFilterTraces_IncompletePasses(t *testing.T) {
	cs := completedset.New(time.Hour)
	td := makeSpan(tid2, false)
	out := filterTraces(td, cs, nil, true)
	if out.SpanCount() != 1 {
		t.Fatalf("incomplete span should pass, got %d", out.SpanCount())
	}
}

func TestCollectErrorTraces(t *testing.T) {
	cs := completedset.New(time.Hour)
	cs.Add(tid1.String(), nil)

	// Build a ScanRecord with an error log for tid1
	ld := makeLog(tid1, plog.SeverityNumberError)
	payload, _ := segment.MarshalLogs(ld)
	records := []segment.ScanRecord{{Signal: segment.SignalLogs, Payload: payload}}

	errTraces := collectErrorTraces(records, cs)
	if !errTraces[tid1.String()] {
		t.Fatal("tid1 should be in error traces")
	}
}
