package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

type tailLogRecord struct {
	Timestamp int64             `json:"ts"`
	Severity  string            `json:"severity"`
	Body      string            `json:"body"`
	Service   string            `json:"service,omitempty"`
	Attrs     map[string]string `json:"attrs,omitempty"`
}

type tailEvent struct {
	TraceID string          `json:"trace_id"`
	Signal  string          `json:"signal"`
	Service string          `json:"service,omitempty"`
	Count   int             `json:"count"`
	Logs    []tailLogRecord `json:"logs,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: hangtail <command>")
		fmt.Fprintln(os.Stderr, "  tail  --addr <host:port> [--service <name>] [--match <pattern>] [--level <level>]")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "tail":
		runTailCmd(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func runTailCmd(args []string) {
	fs := flag.NewFlagSet("tail", flag.ExitOnError)
	addr    := fs.String("addr",    "localhost:4320", "hangtail tail server (host:port)")
	service := fs.String("service", "",               "show only flows whose service name contains this substring")
	match   := fs.String("match",   "",               "show only flows whose trace_id or service matches this regex")
	level   := fs.String("level",   "",               "minimum log level to display per record: debug|info|warn|error")
	_ = fs.Parse(args)

	var matchRe *regexp.Regexp
	if *match != "" {
		var err error
		matchRe, err = regexp.Compile(*match)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid --match pattern: %v\n", err)
			os.Exit(1)
		}
	}

	minLevel := severityNum(strings.ToUpper(*level))

	if err := streamTail(*addr, *service, matchRe, minLevel); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func streamTail(addr, service string, matchRe *regexp.Regexp, minLevel int) error {
	url := "http://" + addr + "/tail"
	resp, err := http.Get(url) //nolint:gosec
	if err != nil {
		return fmt.Errorf("connect %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %s", resp.Status)
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20) // 1 MiB — log payloads can be large
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev tailEvent
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
			continue
		}

		// Flow-level filters
		if service != "" && !strings.Contains(ev.Service, service) {
			continue
		}
		if matchRe != nil && !matchRe.MatchString(ev.TraceID) && !matchRe.MatchString(ev.Service) {
			continue
		}

		printEvent(ev, minLevel)
	}
	return sc.Err()
}

// printEvent renders a forwarded flow to stdout.
// When the event carries log records (signal="logs"), each record is shown
// with timestamp, severity, service, message, and attributes.
// For trace events or events without records, a compact summary is printed.
func printEvent(ev tailEvent, minLevel int) {
	if ev.Signal == "logs" && len(ev.Logs) > 0 {
		printLogFlow(ev, minLevel)
	} else {
		printSummary(ev)
	}
}

func printLogFlow(ev tailEvent, minLevel int) {
	// Collect services seen in this flow
	svcs := uniqueServices(ev.Logs)

	tid := ev.TraceID
	if len(tid) > 16 {
		tid = tid[:16]
	}
	fmt.Printf("━━ FLOW  trace=%-16s  [%s]  %d records ━━\n",
		tid,
		strings.Join(svcs, " → "),
		ev.Count,
	)

	// Sort attrs keys for deterministic output; put "error" first
	for _, lr := range ev.Logs {
		if severityNum(strings.ToUpper(lr.Severity)) < minLevel {
			continue
		}

		var ts string
		if lr.Timestamp > 0 {
			ts = time.Unix(0, lr.Timestamp).UTC().Format("15:04:05.000")
		} else {
			ts = time.Now().UTC().Format("15:04:05.000")
		}

		sev := padRight(normSeverity(lr.Severity), 5)
		svc := padRight(lr.Service, 18)
		body := lr.Body

		attrs := formatAttrs(lr.Attrs)

		fmt.Printf("  %s  %s  %s  %s%s\n", ts, sev, svc, body, attrs)
	}
	fmt.Println()
}

func printSummary(ev tailEvent) {
	sig := "L"
	if ev.Signal == "traces" {
		sig = "T"
	}
	fmt.Printf("%s  %s  trace=%-32s  svc=%-20s  count=%d\n",
		time.Now().UTC().Format("15:04:05.000"),
		sig,
		ev.TraceID,
		ev.Service,
		ev.Count,
	)
}

// formatAttrs returns sorted key=value pairs, with "error" and "order_id" first.
func formatAttrs(attrs map[string]string) string {
	if len(attrs) == 0 {
		return ""
	}
	priority := []string{"error", "order_id", "request_id", "user_id"}
	seen := map[string]bool{}
	var parts []string

	for _, k := range priority {
		if v, ok := attrs[k]; ok {
			parts = append(parts, k+"="+v)
			seen[k] = true
		}
	}
	rest := make([]string, 0, len(attrs))
	for k := range attrs {
		if !seen[k] {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	for _, k := range rest {
		parts = append(parts, k+"="+attrs[k])
	}

	return "  " + strings.Join(parts, "  ")
}

// uniqueServices returns unique service names in order of first appearance.
func uniqueServices(recs []tailLogRecord) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range recs {
		if r.Service != "" && !seen[r.Service] {
			seen[r.Service] = true
			out = append(out, r.Service)
		}
	}
	return out
}

// normSeverity normalises OTel severity text to a short uppercase label.
func normSeverity(s string) string {
	s = strings.ToUpper(s)
	switch {
	case strings.HasPrefix(s, "ERROR"):
		return "ERROR"
	case strings.HasPrefix(s, "WARN"):
		return "WARN"
	case strings.HasPrefix(s, "INFO"):
		return "INFO"
	case strings.HasPrefix(s, "DEBUG"):
		return "DEBUG"
	case strings.HasPrefix(s, "TRACE"):
		return "TRACE"
	case strings.HasPrefix(s, "FATAL"):
		return "FATAL"
	default:
		if s == "" {
			return "INFO"
		}
		return s
	}
}

// severityNum maps a severity label to a numeric order for filtering.
// Returns 0 when s is empty (no filter).
func severityNum(s string) int {
	switch {
	case strings.HasPrefix(s, "FATAL"):
		return 50
	case strings.HasPrefix(s, "ERROR"):
		return 40
	case strings.HasPrefix(s, "WARN"):
		return 30
	case strings.HasPrefix(s, "INFO"):
		return 20
	case strings.HasPrefix(s, "DEBUG"):
		return 10
	case strings.HasPrefix(s, "TRACE"):
		return 5
	default:
		return 0
	}
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s[:n]
	}
	return s + strings.Repeat(" ", n-len(s))
}
