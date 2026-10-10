# hangtail

**Default-keep OTel Collector processor** — buffers every log and span to disk, and only forwards them once a flow signals completion via `flow.end=true`. Flows that crash, time out, or never finish are forwarded automatically.

```
your services  ──OTLP──►  [hangtail processor]  ──►  Loki / any exporter
                                │
                         segments on disk
                         (recovery-safe)
```

## Why

Every service logs everything — but in a multi-service system, most of that is noise from successful flows. When a flow fails, its logs are buried under thousands of success entries. You end up filtering manually, and cross-service correlation is painful.

hangtail flips the default: **keep everything on disk, forward only the flows that failed**. Successful flows (`flow.end=true`) are discarded before they reach the backend. What arrives in Loki or CloudWatch is already filtered to failures, ready to correlate by `trace_id` or `request_id`.

---

## How it works

1. Every log batch and trace arriving at the processor is written to a **segment file** on disk (signal type + length-prefixed protobuf).
2. When a log record carries `flow.end=true` (or your custom attribute), the `trace_id` is added to a **completed set** (append-only file).
3. A **reaper** runs every `segment_window / 2`. When a segment is older than `segment_max_age`, it scans the segment, skips any trace_id that is in the completed set, and forwards the rest.
4. On **collector restart**, segment files are recovered from disk and incomplete flows are eventually forwarded.

---

## Integrating into an existing project

hangtail is a standard OTel Collector component. You need a **custom collector build** that includes it — the pre-built `otelcol-contrib` does not include hangtail.

### Option A — OCB (recommended for production)

Add hangtail to your `builder-config.yaml`:

```yaml
dist:
  name: my-collector
  go_version: "1.26"

extensions:
  - gomod: github.com/madcok/hangtail v0.1.0
    import: github.com/madcok/hangtail/extension/hangtailextension

processors:
  - gomod: github.com/madcok/hangtail v0.1.0
    import: github.com/madcok/hangtail/processor/hangtailprocessor

# ... your existing receivers, exporters, etc.
```

Build:

```bash
ocb --config builder-config.yaml
```

### Option B — Manual assembly (Go code)

Wire the factories directly in `main.go`:

```go
import (
    "github.com/madcok/hangtail/extension/hangtailextension"
    "github.com/madcok/hangtail/processor/hangtailprocessor"
    "go.opentelemetry.io/collector/otelcol"
    // ... your other imports
)

func factories() (otelcol.Factories, error) {
    ext, _ := otelcol.MakeFactoryMap(hangtailextension.NewFactory())
    proc, _ := otelcol.MakeFactoryMap(
        hangtailprocessor.NewFactory(),
        // ... your other processors
    )
    // ...
    return otelcol.Factories{
        Extensions: ext,
        Processors: proc,
        // ...
    }, nil
}
```

See [`demo/cmd/collector/main.go`](demo/cmd/collector/main.go) for a complete working example.

---

## Collector config

Minimal working config:

```yaml
extensions:
  hangtail:
    storage_dir: /var/hangtail/segments   # required — must be persistent storage
    segment_window: 1m
    segment_max_age: 5m
    end_marker:
      attribute: flow.end
      value: "true"

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

processors:
  hangtail:
    extension: hangtail

exporters:
  otlphttp/loki:
    endpoint: http://loki:3100/otlp

service:
  extensions: [hangtail]
  pipelines:
    logs:
      receivers: [otlp]
      processors: [hangtail, batch]
      exporters: [otlphttp/loki]
    traces:
      receivers: [otlp]
      processors: [hangtail]
      exporters: [otlphttp/loki]
```

> **Important:** `storage_dir` must survive collector restarts. Use a host-mounted volume or a persistent disk — never a tmpfs.

### Without OTel SDK — file-based logs

Services that write plain JSON logs to files (no OTel instrumentation) can use the `filelog` receiver paired with `correlation_key`. No changes to application code required.

```yaml
extensions:
  hangtail:
    storage_dir: /var/hangtail/segments
    segment_window: 1m
    segment_max_age: 5m
    end_marker:
      attribute: flow.end   # a field in your JSON logs
      value: "true"

receivers:
  filelog:
    include:
      - /var/log/api/*.log
      - /var/log/worker/*.log
    operators:
      - type: json_parser   # parse each line as JSON

processors:
  hangtail:
    extension: hangtail
    correlation_key: request_id   # group flows by this JSON field instead of trace_id

exporters:
  awscloudwatchlogs:
    log_group_name: /app/failed-flows
    log_stream_name: "{service_name}"

service:
  extensions: [hangtail]
  pipelines:
    logs:
      receivers: [filelog]
      processors: [hangtail, batch]
      exporters: [awscloudwatchlogs]
```

Each log line must carry the `request_id` (or whichever field you set as `correlation_key`). A successful flow signals completion with `"flow.end": "true"` in the same field namespace:

```json
{"ts":"2026-10-11T10:00:00Z","level":"INFO","service":"api","request_id":"req-abc","msg":"order received"}
{"ts":"2026-10-11T10:00:01Z","level":"INFO","service":"worker","request_id":"req-abc","msg":"processing order"}
{"ts":"2026-10-11T10:00:02Z","level":"INFO","service":"api","request_id":"req-abc","msg":"order completed","flow.end":"true"}
```

If `flow.end` never arrives (crash, timeout, error), all log lines for that `request_id` are forwarded to CloudWatch after `segment_max_age`.

---

## Processor config reference

| Field | Type | Default | Description |
|---|---|---|---|
| `extension` | component ID | `hangtail` | ID of the hangtail extension that owns shared state. |
| `correlation_key` | string | `""` | Attribute name to use as the flow grouping key instead of the OTel `trace_id`. Set this when using `filelog` or any non-OTel log source. |

---

## Extension config reference

| Field | Type | Default | Description |
|---|---|---|---|
| `storage_dir` | string | — | **Required.** Directory for segment files. |
| `segment_window` | duration | `1m` | Duration covered by one segment file. |
| `segment_max_age` | duration | `5m` | How long before the reaper processes a segment. Must be `>= segment_window`. |
| `max_staging_bytes` | int64 | `21474836480` (20 GiB) | Total staging size cap. When reached, the oldest segment is force-reaped early. |
| `fsync_interval` | duration | `1s` | How often segment data is fsynced to disk. Lower values reduce data loss on crash. |
| `end_marker.attribute` | string | `flow.end` | Log/span attribute name that signals flow completion. |
| `end_marker.value` | string | `true` | Expected string value of the end marker attribute. |
| `keep_on_error` | bool | `true` | Forward a flow even if it sent `flow.end`, if it also contains an ERROR log or error-status span. |
| `untraced` | string | `forward` | What to do with records that have no `trace_id`. `"forward"` or `"drop"`. |
| `tail_addr` | string | `""` | `host:port` for the SSE tail server used by `hangtail tail`. Empty disables it. |

---

## Instrumenting your service

Add a single log record with `flow.end=true` at the end of every successful flow. The attribute name and value are configurable (see `end_marker` above).

### Go

```go
// success path — signals hangtail to drop the buffered records
logger.InfoContext(ctx, "order completed",
    slog.String("flow.end", "true"),
)

// failure path — do nothing; hangtail forwards the flow after segment_max_age
logger.ErrorContext(ctx, "payment failed", slog.String("err", err.Error()))
```

### Python (opentelemetry-sdk)

```python
from opentelemetry import trace

# success path
logger.info("order completed", extra={"flow.end": "true"})

# failure path — omit the marker, hangtail forwards automatically
logger.error("payment failed", extra={"err": str(e)})
```

### Node.js / NestJS

```typescript
// success path
logger.emit({
  severityNumber: SeverityNumber.INFO,
  body: "order completed",
  attributes: { "flow.end": "true" },
});

// failure path — do nothing
logger.emit({
  severityNumber: SeverityNumber.ERROR,
  body: "payment failed",
  attributes: { err: e.message },
});
```

The `trace_id` is taken from the active span in `ctx`. If there is no active span, the record is treated as untraced (controlled by the `untraced` config field).

---

## CLI — `hangtail tail`

Watch forwarded flows in real time. Requires `tail_addr` to be set in the extension config.

```bash
go install github.com/madcok/hangtail/cmd/hangtail@latest

# stream all forwarded flows
hangtail tail --addr localhost:4320

# filter by service
hangtail tail --addr localhost:4320 --service worker

# filter by trace_id or service (regex)
hangtail tail --addr localhost:4320 --match "order"

# show only warnings and above
hangtail tail --addr localhost:4320 --level warn
```

Example output — a payment-declined flow spans three services:

```
━━ FLOW  trace=91cd907abd78fed0  [api-go]  2 records ━━
  15:53:17.772  INFO   api-go              order received    order_id=ord-179156  remote_addr=...
  15:53:17.776  INFO   api-go              order queued      order_id=ord-179156

━━ FLOW  trace=91cd907abd78fed0  [worker-py]  2 records ━━
  15:53:17.882  INFO   worker-py           processing order  order_id=ord-179156
  15:53:17.911  ERROR  worker-py           payment declined  error=card declined  order_id=ord-179156
```

Each `FLOW` block is one batch forwarded by the reaper. Records from the same `trace_id` across different services appear as separate blocks (one per reaper batch) but share the same `trace=` prefix so they are easy to correlate.

For trace signals (spans), a compact summary line is shown instead:

```
15:55:13.828  T  trace=91cd907abd78fed002402afbe9c5fab8  svc=api-go  count=1
```

---

## Go helper — `internal/console`

A `slog.Handler` wrapper for Go services that:
- Filters log records by minimum level
- Filters by include / exclude regex on the message
- Optionally buffers per-request logs and only prints them if the request fails

```go
import "github.com/madcok/hangtail/internal/console"

// wrap your existing handler (or pass nil for default JSON→stdout)
h := console.New(slog.NewTextHandler(os.Stderr, nil))
slog.SetDefault(slog.New(h))

// in a request handler: buffer logs, only print on error
ctx = console.WithBuffer(ctx)
defer func() { h.Flush(ctx, err) }()
```

Configured via env vars:

| Env var | Description |
|---|---|
| `HANGTAIL_CONSOLE_LEVEL` | Minimum log level: `debug` \| `info` \| `warn` \| `error` |
| `HANGTAIL_CONSOLE_INCLUDE` | Regex — only messages matching this are printed |
| `HANGTAIL_CONSOLE_EXCLUDE` | Regex — messages matching this are dropped |
| `HANGTAIL_CONSOLE_FLUSH_ON_ERROR` | `true` to enable per-request buffering |

---

## Internal metrics (Prometheus / OTLP)

Exposed automatically when the collector's telemetry backend is configured.

| Metric | Type | Description |
|---|---|---|
| `hangtail.forwarded_logs` | Counter | Log records forwarded to downstream exporters |
| `hangtail.forwarded_spans` | Counter | Spans forwarded to downstream exporters |
| `hangtail.dropped_logs` | Counter | Log records dropped (flow completed successfully) |
| `hangtail.dropped_spans` | Counter | Spans dropped (flow completed successfully) |
| `hangtail.staging_bytes` | Gauge | Current total size of all segment files |
| `hangtail.oldest_segment_age` | Gauge | Age in seconds of the oldest unreaped segment |
| `hangtail.completed_traces` | Gauge | Number of trace IDs in the completed set |

---

## Development

```bash
# run unit tests
make test

# run benchmarks
make bench

# start demo stack (Loki + Grafana + api-go)
make demo-up

# run chaos test (200 requests, kill collector, verify zero loss)
./chaos_test.sh

# watch live forwarded flows
go run ./cmd/hangtail/ tail --addr localhost:4320
```

### Demo stacks

Three demo stacks are included under `demo/`:

| Stack | Command | Description |
|---|---|---|
| Full (OTel + Loki) | `make demo-up` | api-go + worker-py + notifier-js → Loki → Grafana |
| Tail-only | `docker compose -f demo/docker-compose-tail.yml up -d` | Same services, no cloud exporter — watch live via `hangtail tail` |
| Non-OTel | `docker compose -f demo/docker-compose-nootel.yml up -d` | api-plain + worker-plain write JSON to files, no OTel SDK |

**Full demo architecture:**

```
┌─────────────┐  OTLP/HTTP  ┌──────────────────────────┐  OTLP/HTTP  ┌──────┐
│  api-go     │────────────►│  hangtail-collector       │────────────►│ Loki │
│  worker-py  │             │  :4317 (gRPC)             │             │ :3100│
│  notifier-js│             │  :4318 (HTTP)             │             └──────┘
└─────────────┘             │  :4320 (tail SSE)         │  Grafana :3000
                            └──────────────────────────┘
```

**Non-OTel demo architecture:**

```
┌──────────────┐  JSON → file  ┌──────────────────────────┐
│  api-plain   │──────────────►│  /var/log/app/*.log       │
│  worker-plain│               └──────────┬───────────────┘
└──────────────┘                          │ filelog receiver
                                          ▼
                               ┌──────────────────────────┐
                               │  hangtail-collector       │
                               │  correlation_key:         │
                               │    request_id             │
                               │  :4320 (tail SSE)         │
                               └──────────────────────────┘
```

After `make demo-up`, open `http://localhost:3000` and query Loki with `{service_name="api-go"}` — you will only see logs from failed orders.

---

## Cloud provider guides

Step-by-step setup for each cloud provider is available in [`docs/cloud-exporters.md`](docs/cloud-exporters.md):

- **AWS** — CloudWatch Logs (IAM Role / Access Key)
- **GCP** — Cloud Logging (Workload Identity / Service Account JSON)
- **Azure** — Azure Monitor (Managed Identity / Connection String)
- **Datadog** — API Key
- **New Relic** — Native OTLP, no extra exporter needed
- **Elastic** — Native OTLP, no extra exporter needed
- **Splunk** — HEC Token

## Cost analysis

A worked example with concrete numbers is available in [`docs/cost-analysis.md`](docs/cost-analysis.md): 1M records/s at 5 KB each across AWS, GCP, and Azure — including a side-by-side comparison with storing everything unfiltered.

---

## Compatibility

| Component | Version |
|---|---|
| Go | 1.22+ |
| OTel Collector | v0.162.0 / component v1.68.0 |
| OCB | 0.116.0+ |

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
