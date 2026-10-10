package hangtailprocessor

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
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
func (p *hangtailLogsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				if p.state.IsFlowEnd(lr.Attributes()) && !lr.TraceID().IsEmpty() {
					p.state.MarkEnd(lr.TraceID().String())
				}
			}
		}
	}
	return p.state.IngestLogs(ctx, ld)
}

func (p *hangtailLogsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
