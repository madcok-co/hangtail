// hangtail-collector is a minimal OTel Collector build that includes
// the hangtail extension and processor alongside the standard OTLP receiver
// and debug/otlphttp exporters.
package main

import (
	"log"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"

	"github.com/madcok/hangtail/extension/hangtailextension"
	"github.com/madcok/hangtail/processor/hangtailprocessor"
)

func main() {
	cmd := otelcol.NewCommand(otelcol.CollectorSettings{
		BuildInfo: component.BuildInfo{
			Command:     "hangtail-collector",
			Description: "OpenTelemetry Collector with hangtail processor",
			Version:     "0.1.0",
		},
		Factories: factories,
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				ProviderFactories: []confmap.ProviderFactory{
					fileprovider.NewFactory(),
					envprovider.NewFactory(),
					yamlprovider.NewFactory(),
				},
			},
		},
	})
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func factories() (otelcol.Factories, error) {
	recv, err := otelcol.MakeFactoryMap(otlpreceiver.NewFactory())
	if err != nil {
		return otelcol.Factories{}, err
	}
	proc, err := otelcol.MakeFactoryMap(
		hangtailprocessor.NewFactory(),
		batchprocessor.NewFactory(),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}
	exp, err := otelcol.MakeFactoryMap(
		debugexporter.NewFactory(),
		otlphttpexporter.NewFactory(),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}
	ext, err := otelcol.MakeFactoryMap(hangtailextension.NewFactory())
	if err != nil {
		return otelcol.Factories{}, err
	}
	return otelcol.Factories{
		Receivers:  recv,
		Processors: proc,
		Exporters:  exp,
		Extensions: ext,
		Telemetry:  otelconftelemetry.NewFactory(),
	}, nil
}
