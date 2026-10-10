package hangtailextension

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

// Type is the component type identifier for the hangtail extension.
var Type = component.MustNewType("hangtail")

const extensionStability = component.StabilityLevelDevelopment

// NewFactory creates the extension factory.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		Type,
		createDefaultConfig,
		createExtension,
		extensionStability,
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		SegmentWindow:   60 * time.Second,
		SegmentMaxAge:   5 * 60 * time.Second,
		MaxStagingBytes: 20 * 1024 * 1024 * 1024,
		FsyncInterval:   time.Second,
		KeepOnError:     true,
		Untraced:        "forward",
		EndMarker: EndMarkerConfig{
			Attribute: "flow.end",
			Value:     "true",
		},
	}
}

func createExtension(
	_ context.Context,
	set extension.Settings,
	cfg component.Config,
) (extension.Extension, error) {
	return newHangtailExtension(set, cfg.(*Config)), nil
}
