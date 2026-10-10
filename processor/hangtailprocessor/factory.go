package hangtailprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/madcok/hangtail/extension/hangtailextension"
)

// Type is the component type identifier for the hangtail processor.
var Type = component.MustNewType("hangtail")

const (
	logsStability   = component.StabilityLevelDevelopment
	tracesStability = component.StabilityLevelDevelopment
)

// NewFactory creates the processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		Type,
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, logsStability),
		processor.WithTraces(createTracesProcessor, tracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Extension: component.NewID(hangtailextension.Type),
	}
}

func createLogsProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Logs,
) (processor.Logs, error) {
	return &hangtailLogsProcessor{
		cfg:  cfg.(*Config),
		next: next,
		set:  set,
	}, nil
}

func createTracesProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Traces,
) (processor.Traces, error) {
	return &hangtailTracesProcessor{
		cfg:  cfg.(*Config),
		next: next,
		set:  set,
	}, nil
}
