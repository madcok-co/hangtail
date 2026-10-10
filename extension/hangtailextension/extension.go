package hangtailextension

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/madcok/hangtail/internal/completedset"
	"github.com/madcok/hangtail/internal/segment"
)

// SignalType identifies whether a record carries log data or trace/span data.
type SignalType = segment.SignalType

const (
	SignalLogs   = segment.SignalLogs
	SignalTraces = segment.SignalTraces
)

// StateHolder is the interface processors use to interact with the extension.
// Using an interface keeps the processor decoupled from the concrete extension type.
type StateHolder interface {
	IngestLogs(ctx context.Context, ld plog.Logs) error
	IngestTraces(ctx context.Context, td ptrace.Traces) error
	MarkEnd(traceID string)
	IsFlowEnd(attrs pcommon.Map) bool
	ForwardUntraced() bool
	RegisterLogsConsumer(c consumer.Logs)
	RegisterTracesConsumer(c consumer.Traces)
}

// HangtailExtension owns all runtime state: segment writers, completed set,
// and the reaper goroutine.
type HangtailExtension struct {
	cfg    *Config
	logger *zap.Logger

	mu             sync.Mutex
	writers        map[int64]*segment.Writer // windowStart → writer
	completedFiles map[int64]*os.File        // windowStart → .completed file handle
	completed      *completedset.Set

	logsConsumer   consumer.Logs
	tracesConsumer consumer.Traces

	meterProvider metric.MeterProvider
	tel           *telemetry

	tail *broadcaster
	srv  *http.Server

	stopCh chan struct{}
	doneCh chan struct{}
}

var _ extension.Extension = (*HangtailExtension)(nil)
var _ StateHolder = (*HangtailExtension)(nil)

func newHangtailExtension(set extension.Settings, cfg *Config) *HangtailExtension {
	return &HangtailExtension{
		cfg:            cfg,
		logger:         set.Logger,
		meterProvider:  set.MeterProvider,
		writers:        make(map[int64]*segment.Writer),
		completedFiles: make(map[int64]*os.File),
		tail:           newBroadcaster(),
	}
}

func (e *HangtailExtension) Start(_ context.Context, _ component.Host) error {
	if err := os.MkdirAll(e.cfg.StorageDir, 0o700); err != nil {
		return fmt.Errorf("hangtail: mkdir %s: %w", e.cfg.StorageDir, err)
	}

	e.completed = completedset.New(e.cfg.SegmentMaxAge * 3)

	if err := e.initTelemetry(); err != nil {
		return fmt.Errorf("hangtail: init telemetry: %w", err)
	}

	if err := e.recoverOnStartup(); err != nil {
		return fmt.Errorf("hangtail: recovery: %w", err)
	}

	e.stopCh = make(chan struct{})
	e.doneCh = make(chan struct{})
	go e.reaperLoop()

	if e.cfg.TailAddr != "" {
		e.startTailServer()
	}

	e.logger.Info("hangtail started",
		zap.String("storage_dir", e.cfg.StorageDir),
		zap.Duration("segment_window", e.cfg.SegmentWindow),
		zap.Duration("segment_max_age", e.cfg.SegmentMaxAge),
	)
	return nil
}

func (e *HangtailExtension) Shutdown(ctx context.Context) error {
	close(e.stopCh)
	<-e.doneCh

	if e.srv != nil {
		_ = e.srv.Shutdown(ctx)
	}
	e.tail.closeAll()

	e.mu.Lock()
	defer e.mu.Unlock()
	for ws, w := range e.writers {
		if err := w.Close(); err != nil {
			e.logger.Error("closing writer on shutdown", zap.Int64("window", ws), zap.Error(err))
		}
	}
	for _, f := range e.completedFiles {
		_ = f.Close()
	}
	e.writers = nil
	e.completedFiles = nil
	return nil
}

// IngestLogs implements StateHolder.
func (e *HangtailExtension) IngestLogs(_ context.Context, ld plog.Logs) error {
	payload, err := segment.MarshalLogs(ld)
	if err != nil {
		return err
	}
	w, err := e.writerForNow()
	if err != nil {
		return err
	}
	return w.Write(segment.SignalLogs, payload)
}

// IngestTraces implements StateHolder.
func (e *HangtailExtension) IngestTraces(_ context.Context, td ptrace.Traces) error {
	payload, err := segment.MarshalTraces(td)
	if err != nil {
		return err
	}
	w, err := e.writerForNow()
	if err != nil {
		return err
	}
	return w.Write(segment.SignalTraces, payload)
}

// MarkEnd implements StateHolder.
func (e *HangtailExtension) MarkEnd(traceID string) {
	f := e.completedFileForNow()
	if err := e.completed.Add(traceID, f); err != nil {
		e.logger.Warn("persist completed trace_id", zap.String("trace_id", traceID), zap.Error(err))
	}
}

// IsFlowEnd implements StateHolder.
func (e *HangtailExtension) IsFlowEnd(attrs pcommon.Map) bool {
	v, ok := attrs.Get(e.cfg.EndMarker.Attribute)
	if !ok {
		return false
	}
	return v.AsString() == e.cfg.EndMarker.Value
}

// ForwardUntraced implements StateHolder.
func (e *HangtailExtension) ForwardUntraced() bool {
	return e.cfg.Untraced == "forward"
}

// RegisterLogsConsumer implements StateHolder.
func (e *HangtailExtension) RegisterLogsConsumer(c consumer.Logs) {
	e.mu.Lock()
	e.logsConsumer = c
	e.mu.Unlock()
}

// RegisterTracesConsumer implements StateHolder.
func (e *HangtailExtension) RegisterTracesConsumer(c consumer.Traces) {
	e.mu.Lock()
	e.tracesConsumer = c
	e.mu.Unlock()
}

// --- internal helpers ---

func (e *HangtailExtension) currentWindowStart() int64 {
	sec := int64(e.cfg.SegmentWindow.Seconds())
	return (time.Now().Unix() / sec) * sec
}

func (e *HangtailExtension) segPath(ws int64) string {
	return filepath.Join(e.cfg.StorageDir, strconv.FormatInt(ws, 10)+".seg")
}

func (e *HangtailExtension) completedPath(ws int64) string {
	return filepath.Join(e.cfg.StorageDir, strconv.FormatInt(ws, 10)+".completed")
}

func (e *HangtailExtension) writerForNow() (*segment.Writer, error) {
	ws := e.currentWindowStart()
	e.mu.Lock()
	defer e.mu.Unlock()
	if w, ok := e.writers[ws]; ok {
		return w, nil
	}
	w, err := segment.NewWriter(e.segPath(ws), e.cfg.FsyncInterval)
	if err != nil {
		return nil, fmt.Errorf("open segment writer: %w", err)
	}
	e.writers[ws] = w
	return w, nil
}

func (e *HangtailExtension) completedFileForNow() *os.File {
	ws := e.currentWindowStart()
	e.mu.Lock()
	defer e.mu.Unlock()
	if f, ok := e.completedFiles[ws]; ok {
		return f
	}
	f, err := os.OpenFile(e.completedPath(ws), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		e.logger.Error("open completed file", zap.String("path", e.completedPath(ws)), zap.Error(err))
		return nil
	}
	e.completedFiles[ws] = f
	return f
}

func (e *HangtailExtension) recoverOnStartup() error {
	// Load all .completed files into the in-memory set.
	completed, err := filepath.Glob(filepath.Join(e.cfg.StorageDir, "*.completed"))
	if err != nil {
		return err
	}
	expireAt := time.Now().Add(e.cfg.SegmentMaxAge)
	for _, p := range completed {
		if loadErr := e.completed.LoadFile(p, expireAt); loadErr != nil {
			e.logger.Warn("load completed file", zap.String("path", p), zap.Error(loadErr))
		}
	}

	// Recover (and truncate if needed) all surviving .seg files.
	segs, err := filepath.Glob(filepath.Join(e.cfg.StorageDir, "*.seg"))
	if err != nil {
		return err
	}
	for _, p := range segs {
		if _, recErr := segment.RecoverSegment(p); recErr != nil {
			e.logger.Warn("recover segment", zap.String("path", p), zap.Error(recErr))
		}
	}
	return nil
}

// --- reaper ---

func (e *HangtailExtension) reaperLoop() {
	defer close(e.doneCh)
	scanTick := time.NewTicker(e.cfg.SegmentWindow / 2)
	defer scanTick.Stop()
	gcTick := time.NewTicker(e.cfg.SegmentWindow * 5)
	defer gcTick.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-scanTick.C:
			e.reapExpired()
		case <-gcTick.C:
			e.completed.Cleanup()
		}
	}
}

func (e *HangtailExtension) reapExpired() {
	paths, err := filepath.Glob(filepath.Join(e.cfg.StorageDir, "*.seg"))
	if err != nil {
		e.logger.Error("glob segments", zap.Error(err))
		return
	}
	now := time.Now().Unix()
	maxAgeSec := int64(e.cfg.SegmentMaxAge.Seconds())

	for _, p := range paths {
		base := strings.TrimSuffix(filepath.Base(p), ".seg")
		ws, err := strconv.ParseInt(base, 10, 64)
		if err != nil {
			continue
		}
		if now-ws <= maxAgeSec {
			continue
		}
		if err := e.reapSegment(ws, p); err != nil {
			e.logger.Error("reap segment", zap.String("path", p), zap.Error(err))
		}
	}
}

func (e *HangtailExtension) reapSegment(ws int64, segPath string) error {
	// Close active writer and completed-file handle for this window first.
	e.mu.Lock()
	if w, ok := e.writers[ws]; ok {
		if err := w.Close(); err != nil {
			e.logger.Error("close writer for reap", zap.Int64("window", ws), zap.Error(err))
		}
		delete(e.writers, ws)
	}
	if f, ok := e.completedFiles[ws]; ok {
		_ = f.Close()
		delete(e.completedFiles, ws)
	}
	lc := e.logsConsumer
	tc := e.tracesConsumer
	e.mu.Unlock()

	// Reload this window's .completed entries (may contain IDs written
	// after the last in-memory load, e.g. after a restart).
	completedPath := e.completedPath(ws)
	if err := e.completed.LoadFile(completedPath, time.Now().Add(e.cfg.SegmentMaxAge)); err != nil {
		e.logger.Warn("load completed for reap", zap.String("path", completedPath), zap.Error(err))
	}

	records, err := segment.ScanSegment(segPath)
	if err != nil {
		return fmt.Errorf("scan %s: %w", segPath, err)
	}

	// Pass 1 (only when keep_on_error=true): find completed traces that
	// also have error records — those must still be forwarded.
	var errorTraceIDs map[string]bool
	if e.cfg.KeepOnError {
		errorTraceIDs = collectErrorTraces(records, e.completed)
	}

	// Pass 2: forward records whose trace_id is NOT in the drop set.
	forwardUntraced := e.cfg.Untraced == "forward"
	forwarded := 0
	ctx := context.Background()
	for _, rec := range records {
		switch rec.Signal {
		case segment.SignalLogs:
			if lc == nil {
				continue
			}
			ld, err := segment.UnmarshalLogs(rec.Payload)
			if err != nil {
				e.logger.Warn("unmarshal logs in reap", zap.Error(err))
				continue
			}
			filtered := filterLogs(ld, e.completed, errorTraceIDs, forwardUntraced)
			fwd := filtered.LogRecordCount()
			dropped := ld.LogRecordCount() - fwd
			if fwd > 0 {
				if err := lc.ConsumeLogs(ctx, filtered); err != nil {
					e.logger.Error("forward logs", zap.Error(err))
				}
				forwarded += fwd
				e.emitForwardedLogs(filtered)
			}
			if e.tel != nil {
				e.tel.forwardedLogs.Add(ctx, int64(fwd))
				e.tel.droppedLogs.Add(ctx, int64(dropped))
			}

		case segment.SignalTraces:
			if tc == nil {
				continue
			}
			td, err := segment.UnmarshalTraces(rec.Payload)
			if err != nil {
				e.logger.Warn("unmarshal traces in reap", zap.Error(err))
				continue
			}
			filtered := filterTraces(td, e.completed, errorTraceIDs, forwardUntraced)
			fwd := filtered.SpanCount()
			dropped := td.SpanCount() - fwd
			if fwd > 0 {
				if err := tc.ConsumeTraces(ctx, filtered); err != nil {
					e.logger.Error("forward traces", zap.Error(err))
				}
				forwarded += fwd
				e.emitForwardedTraces(filtered)
			}
			if e.tel != nil {
				e.tel.forwardedSpans.Add(ctx, int64(fwd))
				e.tel.droppedSpans.Add(ctx, int64(dropped))
			}
		}
	}

	// Delete both files.
	if err := os.Remove(segPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove segment: %w", err)
	}
	if err := os.Remove(completedPath); err != nil && !os.IsNotExist(err) {
		e.logger.Warn("remove completed file", zap.String("path", completedPath), zap.Error(err))
	}

	e.logger.Info("reaped segment",
		zap.Int64("window", ws),
		zap.Int("records_in_segment", len(records)),
		zap.Int("forwarded", forwarded),
	)
	return nil
}
