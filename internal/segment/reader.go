package segment

import (
	"io"
	"os"
)

// ScanRecord is one decoded, complete record from a segment file.
type ScanRecord struct {
	Signal  SignalType
	Payload []byte
}

// ScanSegment reads all valid complete records from path without modifying the
// file. A truncated trailing record is silently ignored.
func ScanSegment(path string) ([]ScanRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return scanAll(f)
}

// RecoverSegment reads all valid records from path. If the last record is
// truncated (crash mid-write), the file is truncated to the last complete
// record boundary. Call this once per surviving segment on startup.
func RecoverSegment(path string) ([]ScanRecord, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var (
		records    []ScanRecord
		lastGoodAt int64
	)
	for {
		sig, payload, derr := DecodeRecord(f)
		if derr == io.EOF {
			break
		}
		if derr == io.ErrUnexpectedEOF {
			if terr := f.Truncate(lastGoodAt); terr != nil {
				return records, terr
			}
			break
		}
		if derr != nil {
			return records, derr
		}
		records = append(records, ScanRecord{Signal: sig, Payload: payload})

		pos, serr := f.Seek(0, io.SeekCurrent)
		if serr != nil {
			return records, serr
		}
		lastGoodAt = pos
	}
	return records, nil
}

func scanAll(r io.Reader) ([]ScanRecord, error) {
	var out []ScanRecord
	for {
		sig, payload, err := DecodeRecord(r)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, ScanRecord{Signal: sig, Payload: payload})
	}
}
