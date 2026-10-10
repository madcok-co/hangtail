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
}

func (c *Config) Validate() error {
	_ = hangtailextension.Type // ensure compile-time link
	return nil
}
