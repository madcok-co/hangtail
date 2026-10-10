package hangtailextension

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/metric"
)

type telemetry struct {
	forwardedLogs  metric.Int64Counter
	forwardedSpans metric.Int64Counter
	droppedLogs    metric.Int64Counter
	droppedSpans   metric.Int64Counter
}

func (e *HangtailExtension) initTelemetry() error {
	m := e.meterProvider.Meter("github.com/madcok/hangtail")

	t := &telemetry{}
	var err error

	if t.forwardedLogs, err = m.Int64Counter(
		"hangtail.forwarded_log_records",
		metric.WithDescription("Log records forwarded to the downstream consumer."),
	); err != nil {
		return err
	}
	if t.forwardedSpans, err = m.Int64Counter(
		"hangtail.forwarded_spans",
		metric.WithDescription("Spans forwarded to the downstream consumer."),
	); err != nil {
		return err
	}
	if t.droppedLogs, err = m.Int64Counter(
		"hangtail.dropped_log_records",
		metric.WithDescription("Log records dropped because their flow completed cleanly."),
	); err != nil {
		return err
	}
	if t.droppedSpans, err = m.Int64Counter(
		"hangtail.dropped_spans",
		metric.WithDescription("Spans dropped because their flow completed cleanly."),
	); err != nil {
		return err
	}
	e.tel = t

	stagingBytesG, err := m.Int64ObservableGauge(
		"hangtail.staging_bytes",
		metric.WithDescription("Total size in bytes of all staging segment files on disk."),
		metric.WithUnit("By"),
	)
	if err != nil {
		return err
	}
	oldestAgeG, err := m.Int64ObservableGauge(
		"hangtail.oldest_segment_age",
		metric.WithDescription("Age in seconds of the oldest staging segment file."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}
	completedG, err := m.Int64ObservableGauge(
		"hangtail.completed_traces",
		metric.WithDescription("Number of trace IDs currently held in the completed set."),
	)
	if err != nil {
		return err
	}

	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		bytes, ageSec := e.stagingStats()
		o.ObserveInt64(stagingBytesG, bytes)
		o.ObserveInt64(oldestAgeG, ageSec)
		if e.completed != nil {
			o.ObserveInt64(completedG, int64(e.completed.Len()))
		}
		return nil
	}, stagingBytesG, oldestAgeG, completedG)
	return err
}

// stagingStats walks the storage directory and returns total .seg file bytes
// and the age in seconds of the oldest segment window.
func (e *HangtailExtension) stagingStats() (totalBytes, oldestAgeSec int64) {
	paths, err := filepath.Glob(filepath.Join(e.cfg.StorageDir, "*.seg"))
	if err != nil || len(paths) == 0 {
		return 0, 0
	}
	now := time.Now().Unix()
	oldestWS := now
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			continue
		}
		totalBytes += fi.Size()
		base := strings.TrimSuffix(filepath.Base(p), ".seg")
		if ws, parseErr := strconv.ParseInt(base, 10, 64); parseErr == nil && ws < oldestWS {
			oldestWS = ws
		}
	}
	if oldestWS < now {
		oldestAgeSec = now - oldestWS
	}
	return totalBytes, oldestAgeSec
}
