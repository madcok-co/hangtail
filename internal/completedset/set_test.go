package completedset_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/madcok/hangtail/internal/completedset"
)

func TestAddAndContains(t *testing.T) {
	s := completedset.New(time.Hour)
	s.Add("abc", nil)
	if !s.Contains("abc") {
		t.Fatal("expected abc to be in set")
	}
	if s.Contains("xyz") {
		t.Fatal("xyz should not be in set")
	}
}

func TestTTLExpiry(t *testing.T) {
	s := completedset.New(50 * time.Millisecond)
	s.Add("expired", nil)
	if !s.Contains("expired") {
		t.Fatal("should be present before TTL")
	}
	time.Sleep(100 * time.Millisecond)
	if s.Contains("expired") {
		t.Fatal("should be absent after TTL")
	}
}

func TestPersistAndLoadFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.completed")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}

	s := completedset.New(time.Hour)
	s.Add("trace-1", f)
	s.Add("trace-2", f)
	f.Close()

	// Load into a new set
	s2 := completedset.New(time.Hour)
	if err := s2.LoadFile(path, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, tid := range []string{"trace-1", "trace-2"} {
		if !s2.Contains(tid) {
			t.Fatalf("expected %s after load", tid)
		}
	}
}

func TestLoadFileMissing(t *testing.T) {
	s := completedset.New(time.Hour)
	if err := s.LoadFile("/does/not/exist.completed", time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
}

func TestCleanup(t *testing.T) {
	s := completedset.New(50 * time.Millisecond)
	s.Add("old", nil)
	time.Sleep(100 * time.Millisecond)
	s.Cleanup()
	if s.Len() != 0 {
		t.Fatalf("expected 0 entries after cleanup, got %d", s.Len())
	}
}
