package hangtailprocessor

import (
	"context"
	"crypto/md5" //nolint:gosec // MD5 used as a deterministic 16-byte hash, not for security
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"

	"github.com/madcok/hangtail/extension/hangtailextension"
)

type hangtailLogsProcessor struct {
	cfg   *Config
	next  consumer.Logs
	set   processor.Settings
	state hangtailextension.StateHolder
}

var _ processor.Logs = (*hangtailLogsProcessor)(nil)

func (p *hangtailLogsProcessor) Start(_ context.Context, host component.Host) error {
	ext, ok := host.GetExtensions()[p.cfg.Extension]
	if !ok {
		return fmt.Errorf("hangtail: extension %q not found; add it to service.extensions", p.cfg.Extension)
	}
	sh, ok := ext.(hangtailextension.StateHolder)
	if !ok {
		return fmt.Errorf("hangtail: extension %q does not implement hangtail.StateHolder", p.cfg.Extension)
	}
	p.state = sh
	// Register next consumer so the reaper can forward via the same pipeline path.
	p.state.RegisterLogsConsumer(p.next)
	return nil
}

func (p *hangtailLogsProcessor) Shutdown(_ context.Context) error {
	return nil
}

// ConsumeLogs detects flow.end markers, stages the batch to disk, and returns.
// Forwarding to the next consumer is handled asynchronously by the reaper.
//
// When correlation_key is set, the attribute value is hashed to a synthetic
// trace_id so the rest of the pipeline (reaper, filter, completed set) works
// without modification.
func (p *hangtailLogsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)

				// Resolve effective trace ID: use correlation_key attribute when
				// configured, otherwise fall back to the OTel trace_id field.
				effectiveTID := lr.TraceID()
				if p.cfg.CorrelationKey != "" {
					if v, ok := lr.Attributes().Get(p.cfg.CorrelationKey); ok {
						effectiveTID = attrToTraceID(v.AsString())
						lr.SetTraceID(effectiveTID)
					}
					// No attribute → leave trace_id as-is; empty = treated as
					// untraced per the extension's untraced config.
				}

				if p.state.IsFlowEnd(lr.Attributes()) && !effectiveTID.IsEmpty() {
					p.state.MarkEnd(effectiveTID.String())
				}
			}
		}
	}
	return p.state.IngestLogs(ctx, ld)
}

func (p *hangtailLogsProcessor) Capabilities() consumer.Capabilities {
	// When correlation_key is set we mutate records (SetTraceID).
	return consumer.Capabilities{MutatesData: p.cfg.CorrelationKey != ""}
}

// attrToTraceID derives a deterministic 16-byte TraceID from an arbitrary string.
func attrToTraceID(s string) pcommon.TraceID {
	h := md5.Sum([]byte(s)) //nolint:gosec
	var tid pcommon.TraceID
	copy(tid[:], h[:])
	return tid
}
