package completedset

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

// Set is a thread-safe set of completed trace IDs. Each entry has a TTL so
// memory stays bounded even when no segments are reaped for a while.
// Entries are persisted by appending to caller-supplied files.
type Set struct {
	mu      sync.RWMutex
	entries map[string]int64 // traceID → expireAt (unix seconds)
	ttl     time.Duration
}

// New creates an empty Set. Entries expire after ttl from the time they were added.
func New(ttl time.Duration) *Set {
	return &Set{
		entries: make(map[string]int64),
		ttl:     ttl,
	}
}

// Add marks traceID as completed. If f is non-nil, the trace ID is appended to it
// (one per line) for crash-durability.
func (s *Set) Add(traceID string, f *os.File) error {
	exp := time.Now().Add(s.ttl).UnixMilli()
	s.mu.Lock()
	s.entries[traceID] = exp
	s.mu.Unlock()
	if f != nil {
		_, err := fmt.Fprintf(f, "%s\n", traceID)
		return err
	}
	return nil
}

// Contains reports whether traceID is in the set and has not yet expired.
func (s *Set) Contains(traceID string) bool {
	s.mu.RLock()
	exp, ok := s.entries[traceID]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	if time.Now().UnixMilli() > exp {
		s.mu.Lock()
		delete(s.entries, traceID)
		s.mu.Unlock()
		return false
	}
	return true
}

// LoadFile reads newline-separated trace IDs from path and adds them with expireAt.
// Silently returns nil if the file does not exist.
func (s *Set) LoadFile(path string, expireAt time.Time) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	exp := expireAt.UnixMilli()
	sc := bufio.NewScanner(f)
	s.mu.Lock()
	defer s.mu.Unlock()
	for sc.Scan() {
		if tid := sc.Text(); tid != "" {
			if _, exists := s.entries[tid]; !exists {
				s.entries[tid] = exp
			}
		}
	}
	return sc.Err()
}

// Cleanup removes all expired entries. Call periodically to bound memory usage.
func (s *Set) Cleanup() {
	now := time.Now().UnixMilli()
	s.mu.Lock()
	for tid, exp := range s.entries {
		if now > exp {
			delete(s.entries, tid)
		}
	}
	s.mu.Unlock()
}

// Len returns the number of live entries.
func (s *Set) Len() int {
	s.mu.RLock()
	n := len(s.entries)
	s.mu.RUnlock()
	return n
}
