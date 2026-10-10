package hangtailextension

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// TailLogRecord is one log record within a forwarded flow.
type TailLogRecord struct {
	Timestamp int64             `json:"ts"`               // Unix nanoseconds
	Severity  string            `json:"severity"`
	Body      string            `json:"body"`
	Service   string            `json:"service,omitempty"`
	Attrs     map[string]string `json:"attrs,omitempty"`
}

// TailEvent is one forwarded-trace notification streamed to CLI subscribers.
// Logs carries the full log records for the flow; it is populated when there
// are active subscribers so the allocation is skipped otherwise.
type TailEvent struct {
	TraceID string          `json:"trace_id"`
	Signal  string          `json:"signal"`           // "logs" | "traces"
	Service string          `json:"service,omitempty"` // primary service (first seen)
	Count   int             `json:"count"`             // total log records or spans
	Logs    []TailLogRecord `json:"logs,omitempty"`    // populated for signal="logs"
}

// broadcaster fan-outs TailEvents to all active SSE subscribers.
type broadcaster struct {
	mu   sync.Mutex
	subs map[chan TailEvent]struct{}
}

func newBroadcaster() *broadcaster {
	return &broadcaster{subs: make(map[chan TailEvent]struct{})}
}

func (b *broadcaster) subscribe() chan TailEvent {
	ch := make(chan TailEvent, 64)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *broadcaster) unsubscribe(ch chan TailEvent) {
	b.mu.Lock()
	delete(b.subs, ch)
	b.mu.Unlock()
}

// send delivers ev to all subscribers; skips slow ones rather than blocking.
func (b *broadcaster) send(ev TailEvent) {
	b.mu.Lock()
	for ch := range b.subs {
		select {
		case ch <- ev:
		default:
		}
	}
	b.mu.Unlock()
}

func (b *broadcaster) closeAll() {
	b.mu.Lock()
	for ch := range b.subs {
		close(ch)
	}
	b.subs = make(map[chan TailEvent]struct{})
	b.mu.Unlock()
}

func (b *broadcaster) hasSubscribers() bool {
	b.mu.Lock()
	n := len(b.subs)
	b.mu.Unlock()
	return n > 0
}

// startTailServer starts the HTTP SSE server in a background goroutine.
func (e *HangtailExtension) startTailServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/tail", e.sseHandler)
	e.srv = &http.Server{Addr: e.cfg.TailAddr, Handler: mux}
	go func() {
		e.logger.Info("tail server listening", zap.String("addr", e.cfg.TailAddr))
		if err := e.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			e.logger.Error("tail server", zap.Error(err))
		}
	}()
}

func (e *HangtailExtension) sseHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := e.tail.subscribe()
	defer e.tail.unsubscribe(ch)

	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return
			}
			data, _ := json.Marshal(ev)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// emitForwardedLogs broadcasts one TailEvent per trace_id, including all
// log records for that trace sorted by timestamp.
func (e *HangtailExtension) emitForwardedLogs(ld plog.Logs) {
	if !e.tail.hasSubscribers() {
		return
	}

	type accum struct {
		ev   TailEvent
		recs []TailLogRecord
	}
	byTrace := map[string]*accum{}

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		svc := ""
		if v, ok := rl.Resource().Attributes().Get("service.name"); ok {
			svc = v.AsString()
		}
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			for k := 0; k < rl.ScopeLogs().At(j).LogRecords().Len(); k++ {
				lr := rl.ScopeLogs().At(j).LogRecords().At(k)
				tid := lr.TraceID().String()

				rec := TailLogRecord{
					Timestamp: int64(lr.Timestamp()),
					Severity:  lr.SeverityText(),
					Body:      lr.Body().AsString(),
					Service:   svc,
					Attrs:     extractAttrs(lr.Attributes()),
				}

				if a, ok := byTrace[tid]; ok {
					a.ev.Count++
					a.recs = append(a.recs, rec)
				} else {
					byTrace[tid] = &accum{
						ev:   TailEvent{TraceID: tid, Signal: "logs", Service: svc, Count: 1},
						recs: []TailLogRecord{rec},
					}
				}
			}
		}
	}

	for _, a := range byTrace {
		sort.Slice(a.recs, func(i, j int) bool {
			return a.recs[i].Timestamp < a.recs[j].Timestamp
		})
		a.ev.Logs = a.recs
		e.tail.send(a.ev)
	}
}

// emitForwardedTraces broadcasts one TailEvent per trace_id present in td.
func (e *HangtailExtension) emitForwardedTraces(td ptrace.Traces) {
	if !e.tail.hasSubscribers() {
		return
	}
	byTrace := map[string]*TailEvent{}
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		svc := ""
		if v, ok := rs.Resource().Attributes().Get("service.name"); ok {
			svc = v.AsString()
		}
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			for k := 0; k < rs.ScopeSpans().At(j).Spans().Len(); k++ {
				tid := rs.ScopeSpans().At(j).Spans().At(k).TraceID().String()
				if ev, ok := byTrace[tid]; ok {
					ev.Count++
				} else {
					byTrace[tid] = &TailEvent{TraceID: tid, Signal: "traces", Service: svc, Count: 1}
				}
			}
		}
	}
	for _, ev := range byTrace {
		e.tail.send(*ev)
	}
}

// skipAttrs contains OTel SDK-internal attribute keys that clutter the tail output.
var skipAttrs = map[string]bool{
	// OTel SDK metadata
	"telemetry.sdk.language": true,
	"telemetry.sdk.name":     true,
	"telemetry.sdk.version":  true,
	// Source location (Python SDK style)
	"code.filepath": true,
	"code.function": true,
	"code.lineno":   true,
	// Source location (semconv v1.24+ style)
	"code.file.path":      true,
	"code.function.name":  true,
	"code.line.number":    true,
	"code.column.number":  true,
}

func extractAttrs(m pcommon.Map) map[string]string {
	if m.Len() == 0 {
		return nil
	}
	out := make(map[string]string, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		if !skipAttrs[k] {
			out[k] = v.AsString()
		}
		return true
	})
	if len(out) == 0 {
		return nil
	}
	return out
}
