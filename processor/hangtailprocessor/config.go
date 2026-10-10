package hangtailprocessor

import (
	"go.opentelemetry.io/collector/component"

	"github.com/madcok/hangtail/extension/hangtailextension"
)

// Config is the configuration for the hangtail processor.
type Config struct {
	// Extension is the component ID of the hangtail extension that holds
	// shared runtime state (segment writers, completed set, reaper).
	// Defaults to component.NewID(hangtailextension.Type) which resolves to
	// "hangtail" in YAML config.
	Extension component.ID `mapstructure:"extension"`

	// CorrelationKey is the log record attribute whose value is used as the
	// flow grouping key instead of the OTel trace_id. Useful when logs come
	// from non-OTel sources (e.g. filelog receiver) and carry a custom
	// request_id or correlation_id attribute.
	// Leave empty (default) to use the standard OTel trace_id field.
	CorrelationKey string `mapstructure:"correlation_key"`
}

func (c *Config) Validate() error {
	_ = hangtailextension.Type // ensure compile-time link
	return nil
}
