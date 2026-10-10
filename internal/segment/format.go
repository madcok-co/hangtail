package segment

import (
	"encoding/binary"
	"io"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

// SignalType identifies whether a record carries log data or trace/span data.
type SignalType byte

const (
	SignalLogs   SignalType = 0
	SignalTraces SignalType = 1
)

// headerSize is [signal(1)][length(4)].
const headerSize = 5

// EncodeRecord writes one record to w using the segment wire format:
//
//	[1 byte signal][4 bytes BE uint32 payload length][N bytes payload]
//
// A record is always written atomically from the caller's perspective;
// the segment writer is responsible for fsync and atomicity across crashes.
func EncodeRecord(w io.Writer, sig SignalType, payload []byte) error {
	var h [headerSize]byte
	h[0] = byte(sig)
	binary.BigEndian.PutUint32(h[1:], uint32(len(payload)))
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// DecodeRecord reads one record from r.
//   - Returns (_, _, io.EOF) when r is at EOF before the header starts.
//   - Returns (_, _, io.ErrUnexpectedEOF) when r is truncated mid-record.
//     The caller (recovery) should truncate the file at the last valid offset.
func DecodeRecord(r io.Reader) (SignalType, []byte, error) {
	var h [headerSize]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, nil, err // io.EOF or io.ErrUnexpectedEOF
	}
	sig := SignalType(h[0])
	n := binary.BigEndian.Uint32(h[1:])
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, io.ErrUnexpectedEOF
	}
	return sig, payload, nil
}

// MarshalLogs serializes plog.Logs to raw OTLP protobuf bytes.
func MarshalLogs(ld plog.Logs) ([]byte, error) {
	return plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
}

// UnmarshalLogs deserializes raw OTLP protobuf bytes to plog.Logs.
func UnmarshalLogs(b []byte) (plog.Logs, error) {
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(b); err != nil {
		return plog.NewLogs(), err
	}
	return req.Logs(), nil
}

// MarshalTraces serializes ptrace.Traces to raw OTLP protobuf bytes.
func MarshalTraces(td ptrace.Traces) ([]byte, error) {
	return ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
}

// UnmarshalTraces deserializes raw OTLP protobuf bytes to ptrace.Traces.
func UnmarshalTraces(b []byte) (ptrace.Traces, error) {
	req := ptraceotlp.NewExportRequest()
	if err := req.UnmarshalProto(b); err != nil {
		return ptrace.NewTraces(), err
	}
	return req.Traces(), nil
}
