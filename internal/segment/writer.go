package segment

import (
	"errors"
	"os"
	"sync"
	"time"
)

var errWriterClosed = errors.New("segment writer: closed")

// Writer appends records to a single segment file using a dedicated goroutine
// to serialize I/O. Callers block on each Write until the data is in the
// kernel page cache; fsync runs independently on a ticker.
type Writer struct {
	path          string
	fsyncInterval time.Duration
	ch            chan writeReq
	closeCh       chan struct{}
	doneCh        chan struct{}
	mu            sync.Mutex
	err           error // first permanent I/O error; once set, all writes fail
}

type writeReq struct {
	sig     SignalType
	payload []byte
	ackCh   chan error
}

// NewWriter creates (or opens for append) the segment file at path and starts
// the background write loop.
func NewWriter(path string, fsyncInterval time.Duration) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		path:          path,
		fsyncInterval: fsyncInterval,
		ch:            make(chan writeReq, 256),
		closeCh:       make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	go w.loop(f)
	return w, nil
}

// Write encodes sig+payload and stages them into the segment file.
// It blocks until the write has been accepted by the OS page cache.
func (w *Writer) Write(sig SignalType, payload []byte) error {
	w.mu.Lock()
	ferr := w.err
	w.mu.Unlock()
	if ferr != nil {
		return ferr
	}
	ack := make(chan error, 1)
	select {
	case w.ch <- writeReq{sig: sig, payload: payload, ackCh: ack}:
	case <-w.closeCh:
		return errWriterClosed
	}
	return <-ack
}

// Close drains pending writes, fsyncs, and closes the underlying file.
// Subsequent Write calls return errWriterClosed.
func (w *Writer) Close() error {
	select {
	case <-w.closeCh:
	default:
		close(w.closeCh)
	}
	<-w.doneCh
	w.mu.Lock()
	err := w.err
	w.mu.Unlock()
	return err
}

// Path returns the path of the underlying segment file.
func (w *Writer) Path() string { return w.path }

func (w *Writer) setErr(err error) {
	if err != nil {
		w.mu.Lock()
		if w.err == nil {
			w.err = err
		}
		w.mu.Unlock()
	}
}

func (w *Writer) loop(f *os.File) {
	ticker := time.NewTicker(w.fsyncInterval)
	defer ticker.Stop()
	defer close(w.doneCh)
	defer f.Close()

	for {
		select {
		case req := <-w.ch:
			err := EncodeRecord(f, req.sig, req.payload)
			w.setErr(err)
			req.ackCh <- err

		case <-ticker.C:
			w.setErr(f.Sync())

		case <-w.closeCh:
			// Drain buffered requests before exiting.
			for {
				select {
				case req := <-w.ch:
					err := EncodeRecord(f, req.sig, req.payload)
					w.setErr(err)
					req.ackCh <- err
				default:
					w.setErr(f.Sync())
					return
				}
			}
		}
	}
}
