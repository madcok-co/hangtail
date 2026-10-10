package hangtailprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/madcok/hangtail/extension/hangtailextension"
)

// ── mock StateHolder ──────────────────────────────────────────────────────────

type mockState struct {
	endMarkerAttr  string
	endMarkerValue string
	markedEnds     []string
	ingestedLogs   []plog.Logs
	ingestedTraces []ptrace.Traces
}

func newMock() *mockState {
	return &mockState{endMarkerAttr: "flow.end", endMarkerValue: "true"}
}

func (m *mockState) IngestLogs(_ context.Context, ld plog.Logs) error {
	m.ingestedLogs = append(m.ingestedLogs, ld)
	return nil
}
func (m *mockState) IngestTraces(_ context.Context, td ptrace.Traces) error {
	m.ingestedTraces = append(m.ingestedTraces, td)
	return nil
}
func (m *mockState) MarkEnd(traceID string) { m.markedEnds = append(m.markedEnds, traceID) }
func (m *mockState) IsFlowEnd(attrs pcommon.Map) bool {
	v, ok := attrs.Get(m.endMarkerAttr)
	return ok && v.AsString() == m.endMarkerValue
}
func (m *mockState) ForwardUntraced() bool                     { return true }
func (m *mockState) RegisterLogsConsumer(_ consumer.Logs)      {}
func (m *mockState) RegisterTracesConsumer(_ consumer.Traces)  {}

var _ hangtailextension.StateHolder = (*mockState)(nil)

// ── helpers ───────────────────────────────────────────────────────────────────

func tid(b0 byte) pcommon.TraceID {
	var t [16]byte
	t[0] = b0
	return pcommon.TraceID(t)
}

func logBatch(traceID pcommon.TraceID, flowEnd bool) plog.Logs {
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTraceID(traceID)
	if flowEnd {
		lr.Attributes().PutStr("flow.end", "true")
	}
	return ld
}

func logBatchWithKey(key, value string, flowEnd bool) plog.Logs {
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Attributes().PutStr(key, value)
	if flowEnd {
		lr.Attributes().PutStr("flow.end", "true")
	}
	return ld
}

func traceBatch(traceID pcommon.TraceID, flowEnd bool) ptrace.Traces {
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(traceID)
	if flowEnd {
		sp.Attributes().PutStr("flow.end", "true")
	}
	return td
}

func logsProcessor(st hangtailextension.StateHolder, correlationKey string) *hangtailLogsProcessor {
	return &hangtailLogsProcessor{
		cfg:   &Config{CorrelationKey: correlationKey},
		state: st,
	}
}

func tracesProcessor(st hangtailextension.StateHolder) *hangtailTracesProcessor {
	return &hangtailTracesProcessor{
		cfg:   &Config{},
		state: st,
	}
}

// ── logs processor ────────────────────────────────────────────────────────────

func TestLogsProcessor_IngestsEveryBatch(t *testing.T) {
	st := newMock()
	p := logsProcessor(st, "")

	_ = p.ConsumeLogs(context.Background(), logBatch(tid(1), false))
	_ = p.ConsumeLogs(context.Background(), logBatch(tid(2), false))

	if len(st.ingestedLogs) != 2 {
		t.Fatalf("want 2 ingested batches, got %d", len(st.ingestedLogs))
	}
}

func TestLogsProcessor_FlowEndCallsMarkEnd(t *testing.T) {
	st := newMock()
	p := logsProcessor(st, "")
	id := tid(0x42)

	_ = p.ConsumeLogs(context.Background(), logBatch(id, true))

	if len(st.markedEnds) != 1 {
		t.Fatalf("want 1 MarkEnd call, got %d", len(st.markedEnds))
	}
	if st.markedEnds[0] != id.String() {
		t.Fatalf("wrong trace_id marked: %s", st.markedEnds[0])
	}
}

func TestLogsProcessor_NoFlowEndNoMarkEnd(t *testing.T) {
	st := newMock()
	p := logsProcessor(st, "")

	_ = p.ConsumeLogs(context.Background(), logBatch(tid(1), false))

	if len(st.markedEnds) != 0 {
		t.Fatalf("expected no MarkEnd calls, got %d", len(st.markedEnds))
	}
}

func TestLogsProcessor_EmptyTraceIDNotMarked(t *testing.T) {
	st := newMock()
	p := logsProcessor(st, "")

	// flow.end=true but trace_id is empty → must not call MarkEnd
	_ = p.ConsumeLogs(context.Background(), logBatch(pcommon.TraceID{}, true))

	if len(st.markedEnds) != 0 {
		t.Fatalf("empty trace_id should not be marked, got %d MarkEnd calls", len(st.markedEnds))
	}
}

func TestLogsProcessor_CorrelationKey_SyntheticTraceID(t *testing.T) {
	st := newMock()
	p := logsProcessor(st, "request_id")

	ld := logBatchWithKey("request_id", "req-abc", false)
	_ = p.ConsumeLogs(context.Background(), ld)

	// Verify the record was mutated with a non-empty synthetic trace_id.
	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if lr.TraceID().IsEmpty() {
		t.Fatal("expected synthetic trace_id to be set, got empty")
	}
	// Same key → same trace_id (deterministic).
	ld2 := logBatchWithKey("request_id", "req-abc", false)
	_ = p.ConsumeLogs(context.Background(), ld2)
	lr2 := ld2.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if lr.TraceID() != lr2.TraceID() {
		t.Fatal("same correlation key must produce same trace_id")
	}
}

func TestLogsProcessor_CorrelationKey_FlowEndMarked(t *testing.T) {
	st := newMock()
	p := logsProcessor(st, "request_id")

	ld := logBatchWithKey("request_id", "req-xyz", true)
	_ = p.ConsumeLogs(context.Background(), ld)

	if len(st.markedEnds) != 1 {
		t.Fatalf("want 1 MarkEnd for correlation_key flow, got %d", len(st.markedEnds))
	}
}

func TestLogsProcessor_CorrelationKey_MissingAttr_NoMutation(t *testing.T) {
	st := newMock()
	p := logsProcessor(st, "request_id")

	// Record has no request_id attribute → trace_id stays empty, no MarkEnd.
	ld := logBatch(pcommon.TraceID{}, false)
	_ = p.ConsumeLogs(context.Background(), ld)

	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	if !lr.TraceID().IsEmpty() {
		t.Fatal("missing attr should not mutate trace_id")
	}
	if len(st.markedEnds) != 0 {
		t.Fatal("missing attr should not call MarkEnd")
	}
}

func TestLogsProcessor_Capabilities(t *testing.T) {
	st := newMock()
	withKey := logsProcessor(st, "request_id")
	withoutKey := logsProcessor(st, "")

	if !withKey.Capabilities().MutatesData {
		t.Fatal("correlation_key set → MutatesData must be true")
	}
	if withoutKey.Capabilities().MutatesData {
		t.Fatal("no correlation_key → MutatesData must be false")
	}
}

// ── traces processor ──────────────────────────────────────────────────────────

func TestTracesProcessor_IngestsBatch(t *testing.T) {
	st := newMock()
	p := tracesProcessor(st)

	_ = p.ConsumeTraces(context.Background(), traceBatch(tid(1), false))

	if len(st.ingestedTraces) != 1 {
		t.Fatalf("want 1 ingested trace batch, got %d", len(st.ingestedTraces))
	}
}

func TestTracesProcessor_FlowEndCallsMarkEnd(t *testing.T) {
	st := newMock()
	p := tracesProcessor(st)
	id := tid(0x55)

	_ = p.ConsumeTraces(context.Background(), traceBatch(id, true))

	if len(st.markedEnds) != 1 || st.markedEnds[0] != id.String() {
		t.Fatalf("want MarkEnd(%s), got %v", id.String(), st.markedEnds)
	}
}

func TestTracesProcessor_NoFlowEndNoMarkEnd(t *testing.T) {
	st := newMock()
	p := tracesProcessor(st)

	_ = p.ConsumeTraces(context.Background(), traceBatch(tid(1), false))

	if len(st.markedEnds) != 0 {
		t.Fatalf("no flow.end → no MarkEnd, got %d", len(st.markedEnds))
	}
}

func TestTracesProcessor_Capabilities(t *testing.T) {
	p := tracesProcessor(newMock())
	if p.Capabilities().MutatesData {
		t.Fatal("traces processor must not mutate data")
	}
}
