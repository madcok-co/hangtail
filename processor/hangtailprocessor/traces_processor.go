package hangtailprocessor

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"

	"github.com/madcok/hangtail/extension/hangtailextension"
)

type hangtailTracesProcessor struct {
	cfg   *Config
	next  consumer.Traces
	set   processor.Settings
	state hangtailextension.StateHolder
}

var _ processor.Traces = (*hangtailTracesProcessor)(nil)

func (p *hangtailTracesProcessor) Start(_ context.Context, host component.Host) error {
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
	p.state.RegisterTracesConsumer(p.next)
	return nil
}

func (p *hangtailTracesProcessor) Shutdown(_ context.Context) error {
	return nil
}

// ConsumeTraces detects flow.end markers on span attributes, stages the batch
// to disk, and returns. Forwarding is handled asynchronously by the reaper.
func (p *hangtailTracesProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				sp := ss.Spans().At(k)
				if p.state.IsFlowEnd(sp.Attributes()) && !sp.TraceID().IsEmpty() {
					p.state.MarkEnd(sp.TraceID().String())
				}
			}
		}
	}
	return p.state.IngestTraces(ctx, td)
}

func (p *hangtailTracesProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
