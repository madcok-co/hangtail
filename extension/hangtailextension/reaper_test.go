package hangtailextension

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/madcok/hangtail/internal/completedset"
	"github.com/madcok/hangtail/internal/segment"
)

// ── capture consumer ──────────────────────────────────────────────────────────

type captureConsumer struct {
	logs []plog.Logs
}

func (c *captureConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
func (c *captureConsumer) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	c.logs = append(c.logs, ld)
	return nil
}
func (c *captureConsumer) totalRecords() int {
	n := 0
	for _, ld := range c.logs {
		n += ld.LogRecordCount()
	}
	return n
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newReaperExt(t *testing.T) *HangtailExtension {
	t.Helper()
	dir := t.TempDir()
	cfg := &Config{
		StorageDir:    dir,
		SegmentWindow: time.Minute,
		SegmentMaxAge: 5 * time.Minute,
		FsyncInterval: time.Second,
		EndMarker:     EndMarkerConfig{Attribute: "flow.end", Value: "true"},
		KeepOnError:   true,
		Untraced:      "forward",
	}
	logger, _ := zap.NewDevelopment()
	ext := &HangtailExtension{
		cfg:            cfg,
		logger:         logger,
		writers:        make(map[int64]*segment.Writer),
		completedFiles: make(map[int64]*os.File),
		completed:      completedset.New(cfg.SegmentMaxAge * 3),
		tail:           newBroadcaster(),
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
	close(ext.doneCh)
	return ext
}

func makeTID(b0 byte) pcommon.TraceID {
	var t [16]byte
	t[0] = b0
	return pcommon.TraceID(t)
}

type reapEntry struct {
	tid      pcommon.TraceID
	sev      plog.SeverityNumber
	complete bool
}

// buildSeg writes log records to a .seg file for the given window start.
// Completed trace_ids are registered in ext.completed.
func buildSeg(t *testing.T, ext *HangtailExtension, ws int64, entries []reapEntry) string {
	t.Helper()
	path := filepath.Join(ext.cfg.StorageDir, strconv.FormatInt(ws, 10)+".seg")
	w, err := segment.NewWriter(path, time.Second)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, e := range entries {
		ld := plog.NewLogs()
		lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.SetTraceID(e.tid)
		if e.sev != 0 {
			lr.SetSeverityNumber(e.sev)
		} else {
			lr.SetSeverityNumber(plog.SeverityNumberInfo)
		}
		payload, _ := segment.MarshalLogs(ld)
		if err := w.Write(segment.SignalLogs, payload); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if e.complete && !e.tid.IsEmpty() {
			ext.completed.Add(e.tid.String(), nil)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path
}

func expiredWS() int64 {
	return time.Now().Add(-10 * time.Minute).Unix()
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestReapSegment_ForwardsIncompleteDropsCompleted(t *testing.T) {
	ext := newReaperExt(t)
	cap := &captureConsumer{}
	ext.logsConsumer = cap

	ws := expiredWS()
	path := buildSeg(t, ext, ws, []reapEntry{
		{tid: makeTID(0x01), complete: false},
		{tid: makeTID(0x02), complete: true},
		{tid: makeTID(0x03), complete: false},
	})

	if err := ext.reapSegment(ws, path); err != nil {
		t.Fatalf("reapSegment: %v", err)
	}

	if cap.totalRecords() != 2 {
		t.Fatalf("want 2 forwarded (incomplete only), got %d", cap.totalRecords())
	}
}

func TestReapSegment_AllCompleted_NothingForwarded(t *testing.T) {
	ext := newReaperExt(t)
	cap := &captureConsumer{}
	ext.logsConsumer = cap

	ws := expiredWS()
	path := buildSeg(t, ext, ws, []reapEntry{
		{tid: makeTID(0x01), complete: true},
		{tid: makeTID(0x02), complete: true},
	})

	if err := ext.reapSegment(ws, path); err != nil {
		t.Fatalf("reapSegment: %v", err)
	}

	if cap.totalRecords() != 0 {
		t.Fatalf("all completed → want 0 forwarded, got %d", cap.totalRecords())
	}
}

func TestReapSegment_KeepOnError_CompletedWithErrorForwarded(t *testing.T) {
	ext := newReaperExt(t)
	cap := &captureConsumer{}
	ext.logsConsumer = cap

	ws := expiredWS()
	// tid 0x01: completed but ERROR → keep_on_error must forward it
	// tid 0x02: completed, info only → drop
	path := buildSeg(t, ext, ws, []reapEntry{
		{tid: makeTID(0x01), sev: plog.SeverityNumberError, complete: true},
		{tid: makeTID(0x02), sev: plog.SeverityNumberInfo, complete: true},
	})

	if err := ext.reapSegment(ws, path); err != nil {
		t.Fatalf("reapSegment: %v", err)
	}

	if cap.totalRecords() != 1 {
		t.Fatalf("keep_on_error: want 1 forwarded (error trace), got %d", cap.totalRecords())
	}
}

func TestReapSegment_DeletesFiles(t *testing.T) {
	ext := newReaperExt(t)
	ext.logsConsumer = &captureConsumer{}

	ws := expiredWS()
	path := buildSeg(t, ext, ws, []reapEntry{
		{tid: makeTID(0x01), complete: false},
	})
	completedPath := ext.completedPath(ws)

	if err := ext.reapSegment(ws, path); err != nil {
		t.Fatalf("reapSegment: %v", err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error(".seg file should be deleted after reap")
	}
	if _, err := os.Stat(completedPath); !os.IsNotExist(err) {
		t.Error(".completed file should be deleted after reap")
	}
}

func TestReapSegment_UntracedForwarded(t *testing.T) {
	ext := newReaperExt(t)
	cap := &captureConsumer{}
	ext.logsConsumer = cap

	ws := expiredWS()
	path := buildSeg(t, ext, ws, []reapEntry{
		{tid: pcommon.TraceID{}, complete: false}, // empty trace_id = untraced
	})

	if err := ext.reapSegment(ws, path); err != nil {
		t.Fatalf("reapSegment: %v", err)
	}

	// cfg.Untraced = "forward" → untraced record must pass through.
	if cap.totalRecords() != 1 {
		t.Fatalf("untraced should be forwarded (Untraced=forward), got %d", cap.totalRecords())
	}
}

func TestReapSegment_UntracedDropped(t *testing.T) {
	ext := newReaperExt(t)
	ext.cfg.Untraced = "drop"
	cap := &captureConsumer{}
	ext.logsConsumer = cap

	ws := expiredWS()
	path := buildSeg(t, ext, ws, []reapEntry{
		{tid: pcommon.TraceID{}, complete: false},
	})

	if err := ext.reapSegment(ws, path); err != nil {
		t.Fatalf("reapSegment: %v", err)
	}

	if cap.totalRecords() != 0 {
		t.Fatalf("untraced should be dropped (Untraced=drop), got %d", cap.totalRecords())
	}
}

func TestReapSegment_NoConsumer_NoError(t *testing.T) {
	ext := newReaperExt(t)
	// logsConsumer intentionally nil — reap should not panic or error.

	ws := expiredWS()
	path := buildSeg(t, ext, ws, []reapEntry{
		{tid: makeTID(0x01), complete: false},
	})

	if err := ext.reapSegment(ws, path); err != nil {
		t.Fatalf("reapSegment with nil consumer: %v", err)
	}
}
