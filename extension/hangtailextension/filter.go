package hangtailextension

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/madcok/hangtail/internal/completedset"
	"github.com/madcok/hangtail/internal/segment"
)

// collectErrorTraces scans records and returns the set of trace IDs that are
// both in the completed set AND have at least one ERROR log or error-status span.
// Used to implement keep_on_error: such traces must be forwarded despite being complete.
func collectErrorTraces(records []segment.ScanRecord, completed *completedset.Set) map[string]bool {
	out := map[string]bool{}
	for _, rec := range records {
		switch rec.Signal {
		case segment.SignalLogs:
			ld, err := segment.UnmarshalLogs(rec.Payload)
			if err != nil {
				continue
			}
			for i := 0; i < ld.ResourceLogs().Len(); i++ {
				rl := ld.ResourceLogs().At(i)
				for j := 0; j < rl.ScopeLogs().Len(); j++ {
					sl := rl.ScopeLogs().At(j)
					for k := 0; k < sl.LogRecords().Len(); k++ {
						lr := sl.LogRecords().At(k)
						if lr.SeverityNumber() >= plog.SeverityNumberError {
							tid := lr.TraceID().String()
							if completed.Contains(tid) {
								out[tid] = true
							}
						}
					}
				}
			}

		case segment.SignalTraces:
			td, err := segment.UnmarshalTraces(rec.Payload)
			if err != nil {
				continue
			}
			for i := 0; i < td.ResourceSpans().Len(); i++ {
				rs := td.ResourceSpans().At(i)
				for j := 0; j < rs.ScopeSpans().Len(); j++ {
					ss := rs.ScopeSpans().At(j)
					for k := 0; k < ss.Spans().Len(); k++ {
						sp := ss.Spans().At(k)
						if sp.Status().Code() == ptrace.StatusCodeError {
							tid := sp.TraceID().String()
							if completed.Contains(tid) {
								out[tid] = true
							}
						}
					}
				}
			}
		}
	}
	return out
}

// shouldForward returns true if the record identified by tid should be forwarded.
func shouldForward(tid pcommon.TraceID, completed *completedset.Set, errorTraces map[string]bool, forwardUntraced bool) bool {
	if tid.IsEmpty() {
		return forwardUntraced
	}
	tidStr := tid.String()
	if !completed.Contains(tidStr) {
		return true // not yet complete → keep
	}
	return errorTraces[tidStr] // complete + error → keep if keep_on_error
}

// filterLogs returns a new plog.Logs containing only records that should be
// forwarded (incomplete traces, untraced records per policy, error override).
func filterLogs(ld plog.Logs, completed *completedset.Set, errorTraces map[string]bool, forwardUntraced bool) plog.Logs {
	out := plog.NewLogs()
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		srcRL := ld.ResourceLogs().At(i)
		dstRLAdded := false
		var dstRL plog.ResourceLogs

		for j := 0; j < srcRL.ScopeLogs().Len(); j++ {
			srcSL := srcRL.ScopeLogs().At(j)
			dstSLAdded := false
			var dstSL plog.ScopeLogs

			for k := 0; k < srcSL.LogRecords().Len(); k++ {
				lr := srcSL.LogRecords().At(k)
				if !shouldForward(lr.TraceID(), completed, errorTraces, forwardUntraced) {
					continue
				}
				if !dstRLAdded {
					dstRL = out.ResourceLogs().AppendEmpty()
					srcRL.Resource().CopyTo(dstRL.Resource())
					dstRLAdded = true
				}
				if !dstSLAdded {
					dstSL = dstRL.ScopeLogs().AppendEmpty()
					srcSL.Scope().CopyTo(dstSL.Scope())
					dstSLAdded = true
				}
				lr.CopyTo(dstSL.LogRecords().AppendEmpty())
			}
		}
	}
	return out
}

// filterTraces returns a new ptrace.Traces containing only spans that should be
// forwarded.
func filterTraces(td ptrace.Traces, completed *completedset.Set, errorTraces map[string]bool, forwardUntraced bool) ptrace.Traces {
	out := ptrace.NewTraces()
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		srcRS := td.ResourceSpans().At(i)
		dstRSAdded := false
		var dstRS ptrace.ResourceSpans

		for j := 0; j < srcRS.ScopeSpans().Len(); j++ {
			srcSS := srcRS.ScopeSpans().At(j)
			dstSSAdded := false
			var dstSS ptrace.ScopeSpans

			for k := 0; k < srcSS.Spans().Len(); k++ {
				sp := srcSS.Spans().At(k)
				if !shouldForward(sp.TraceID(), completed, errorTraces, forwardUntraced) {
					continue
				}
				if !dstRSAdded {
					dstRS = out.ResourceSpans().AppendEmpty()
					srcRS.Resource().CopyTo(dstRS.Resource())
					dstRSAdded = true
				}
				if !dstSSAdded {
					dstSS = dstRS.ScopeSpans().AppendEmpty()
					srcSS.Scope().CopyTo(dstSS.Scope())
					dstSSAdded = true
				}
				sp.CopyTo(dstSS.Spans().AppendEmpty())
			}
		}
	}
	return out
}
