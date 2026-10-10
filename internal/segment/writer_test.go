package segment_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/madcok/hangtail/internal/segment"
)

func TestWriterRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.seg")

	w, err := segment.NewWriter(path, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("hello writer")
	if err := w.Write(segment.SignalLogs, payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	records, err := segment.ScanSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("want 1 record, got %d", len(records))
	}
	if string(records[0].Payload) != "hello writer" {
		t.Fatalf("payload mismatch: %q", records[0].Payload)
	}
}

func TestWriterMultiple(t *testing.T) {
	dir := t.TempDir()
	w, err := segment.NewWriter(filepath.Join(dir, "multi.seg"), 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	const n = 10
	for i := 0; i < n; i++ {
		if err := w.Write(segment.SignalTraces, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	records, err := segment.ScanSegment(filepath.Join(dir, "multi.seg"))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != n {
		t.Fatalf("want %d records, got %d", n, len(records))
	}
}

func TestRecoverTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trunc.seg")

	// Write 3 complete records.
	w, _ := segment.NewWriter(path, 100*time.Millisecond)
	for i := 0; i < 3; i++ {
		w.Write(segment.SignalLogs, []byte{byte(i)})
	}
	w.Close()

	// Manually truncate the file by 3 bytes (corrupts last record payload).
	fi, _ := os.Stat(path)
	os.Truncate(path, fi.Size()-3)

	records, err := segment.RecoverSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	// Should recover only 2 complete records.
	if len(records) != 2 {
		t.Fatalf("want 2 records after recovery, got %d", len(records))
	}
	// File should now be truncated to 2 complete records.
	fi2, _ := os.Stat(path)
	original3 := fi.Size()
	if fi2.Size() >= original3 {
		t.Fatal("file should be shorter after truncation recovery")
	}
}
