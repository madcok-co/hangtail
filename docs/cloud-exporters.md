# hangtail — Cloud Exporter Setup

hangtail is an OTel **processor**, not an exporter. It filters logs and spans inside the pipeline before data is sent downstream. This means **you only need to change the `exporters:` section of your collector config** — no changes to service code, hangtail config, or instrumentation.

```
OTLP receiver
     │
     ▼
[hangtail processor]   ← only forwards failed flows
     │
     ▼
[ exporter: CloudWatch / Cloud Logging / Azure / Datadog / ... ]
```

---

## Prerequisites

You need a custom collector build that includes hangtail. All examples below add an exporter to an existing OCB builder config.

**`collector/builder-config.yaml` (base):**
```yaml
dist:
  name: my-collector
  go_version: "1.22"

extensions:
  - gomod: github.com/madcok/hangtail v0.1.0
    import: github.com/madcok/hangtail/extension/hangtailextension

processors:
  - gomod: github.com/madcok/hangtail v0.1.0
    import: github.com/madcok/hangtail/processor/hangtailprocessor
  - gomod: go.opentelemetry.io/collector/processor/batchprocessor v0.162.0

receivers:
  - gomod: go.opentelemetry.io/collector/receiver/otlpreceiver v0.162.0

# Add exporters for your chosen provider (see sections below)
```

---

## AWS — CloudWatch Logs

**When to use:** Applications running on EC2, ECS, EKS, Lambda, or anywhere in AWS.

### 1. Add to builder-config.yaml

```yaml
exporters:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awscloudwatchlogsexporter v0.162.0
```

### 2. Collector config

```yaml
extensions:
  hangtail:
    storage_dir: /var/hangtail/segments
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
      http:
        endpoint: 0.0.0.0:4318

processors:
  hangtail:
    extension: hangtail
  batch:
    timeout: 5s

exporters:
  awscloudwatchlogs:
    log_group_name: /apps/${env:APP_NAME}/errors   # auto-created if missing
    log_stream_name: hangtail
    region: ${env:AWS_REGION}

service:
  extensions: [hangtail]
  pipelines:
    logs:
      receivers: [otlp]
      processors: [hangtail, batch]
      exporters: [awscloudwatchlogs]
    traces:
      receivers: [otlp]
      processors: [hangtail]
      exporters: [awscloudwatchlogs]   # optional: forward traces too
```

### 3. Authentication

**Option A — IAM Role (recommended for EC2 / ECS / EKS):**

Attach the following policy to your instance role, task role, or IRSA service account:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams"
      ],
      "Resource": "arn:aws:logs:*:*:/apps/*/errors*"
    }
  ]
}
```

The collector picks up credentials from instance metadata automatically — no extra config needed.

**Option B — Access Key (local / non-AWS):**

```bash
AWS_ACCESS_KEY_ID=AKIA... \
AWS_SECRET_ACCESS_KEY=... \
AWS_REGION=ap-southeast-1 \
./my-collector --config config.yaml
```

**Option C — EKS with IRSA:**

Annotate the collector's Kubernetes service account:
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: otel-collector
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789:role/otel-collector-role
```

### 4. What you see in CloudWatch

Log group `/apps/my-service/errors` contains **only failed flow logs**. Query with CloudWatch Logs Insights:

```
fields @timestamp, service.name, order_id, @message
| filter @logStream = "hangtail"
| sort @timestamp desc
| limit 100
```

---

## GCP — Cloud Logging

**When to use:** Applications running on GKE, GCE, Cloud Run, or using Google Cloud.

### 1. Add to builder-config.yaml

```yaml
exporters:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/exporter/googlecloudexporter v0.162.0
```

### 2. Collector config

```yaml
exporters:
  googlecloud:
    project: ${env:GCP_PROJECT_ID}
    log:
      default_log_name: hangtail-errors

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [hangtail, batch]
      exporters: [googlecloud]
    traces:
      receivers: [otlp]
      processors: [hangtail]
      exporters: [googlecloud]
```

### 3. Authentication

**GKE Workload Identity (recommended):**

```bash
# Create a GCP service account
gcloud iam service-accounts create otel-collector

# Grant logging writer permission
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:otel-collector@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/logging.logWriter"

# Bind to Kubernetes service account
gcloud iam service-accounts add-iam-policy-binding \
  otel-collector@$PROJECT_ID.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:$PROJECT_ID.svc.id.goog[default/otel-collector]"
```

**Service Account JSON (VM / local):**

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json ./my-collector --config config.yaml
```

### 4. What you see in Cloud Logging

Filter in Log Explorer:
```
logName="projects/my-project/logs/hangtail-errors"
```

Or more specifically:
```
logName="projects/my-project/logs/hangtail-errors"
labels."order_id"!=""
severity>=ERROR
```

---

## Azure — Azure Monitor Logs

**When to use:** Applications running on AKS, Azure Container Apps, VMs, or using Azure.

### 1. Add to builder-config.yaml

```yaml
exporters:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/exporter/azuremonitorexporter v0.162.0
```

### 2. Collector config

```yaml
exporters:
  azuremonitor:
    connection_string: ${env:APPLICATIONINSIGHTS_CONNECTION_STRING}

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [hangtail, batch]
      exporters: [azuremonitor]
    traces:
      receivers: [otlp]
      processors: [hangtail]
      exporters: [azuremonitor]
```

### 3. Authentication

Get the connection string from Azure Portal:
```
Application Insights → Overview → Connection String
```

Format: `InstrumentationKey=<key>;IngestionEndpoint=https://...`

**AKS with Managed Identity:**

Assign the `Monitoring Metrics Publisher` role to the pod's managed identity, then set `AZURE_CLIENT_ID` in the collector's environment.

### 4. What you see in Azure Monitor

In Application Insights → Logs, query with KQL:
```kusto
traces
| where customDimensions["order_id"] != ""
| order by timestamp desc
| project timestamp, message, customDimensions
```

Or in a Log Analytics Workspace:
```kusto
AppTraces
| summarize count() by bin(TimeGenerated, 5m), tostring(Properties["service.name"])
| order by TimeGenerated desc
```

---

## Datadog

**When to use:** Teams already using Datadog as their observability platform.

### 1. Add to builder-config.yaml

```yaml
exporters:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter v0.162.0
```

### 2. Collector config

```yaml
exporters:
  datadog:
    api:
      key: ${env:DD_API_KEY}
      site: datadoghq.com        # datadoghq.eu for EU region

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [hangtail, batch]
      exporters: [datadog]
    traces:
      receivers: [otlp]
      processors: [hangtail]
      exporters: [datadog]
```

### 3. Authentication

Create an API key in Datadog:
```
Organization Settings → API Keys → New Key
```

Set `DD_API_KEY=<your-key>` in the collector's environment.

### 4. What you see in Datadog

**Log Explorer** — filter `@order_id:*` to view all forwarded flows.

**APM Traces** — forwarded traces appear as error traces with full span waterfalls across services.

Saved search in Log Explorer:
```
service:(api-go OR worker-py OR notifier-js) status:error
```

---

## New Relic

New Relic supports **native OTLP ingestion** — no custom exporter needed, the built-in `otlphttp` exporter works directly.

### Collector config

```yaml
exporters:
  otlphttp/newrelic:
    endpoint: https://otlp.nr-data.net        # US datacenter
    # endpoint: https://otlp.eu01.nr-data.net  # EU datacenter
    headers:
      api-key: ${env:NEW_RELIC_LICENSE_KEY}

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [hangtail, batch]
      exporters: [otlphttp/newrelic]
    traces:
      receivers: [otlp]
      processors: [hangtail]
      exporters: [otlphttp/newrelic]
```

### Authentication

Get the License Key from:
```
Account Settings → API Keys → License Key
```

**No changes to builder-config.yaml required** — `otlphttp` is built into OTel Collector core.

### What you see in New Relic

**Logs UI** — NRQL query:
```sql
SELECT * FROM Log
WHERE order_id IS NOT NULL
SINCE 1 hour ago
ORDER BY timestamp DESC
```

**Distributed Tracing** — full cross-service traces with errors highlighted.

---

## Elastic (Elasticsearch / Elastic Cloud)

Elastic 8.x+ supports native OTLP ingestion through the APM Server — no custom exporter needed.

### Collector config

```yaml
exporters:
  # Self-hosted Elastic
  otlphttp/elastic:
    endpoint: ${env:ELASTIC_APM_SERVER_URL}
    headers:
      Authorization: "Bearer ${env:ELASTIC_APM_SECRET_TOKEN}"

  # Elastic Cloud (API Key)
  otlphttp/elasticcloud:
    endpoint: ${env:ELASTIC_APM_SERVER_URL}
    headers:
      Authorization: "ApiKey ${env:ELASTIC_API_KEY}"

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [hangtail, batch]
      exporters: [otlphttp/elastic]
    traces:
      receivers: [otlp]
      processors: [hangtail]
      exporters: [otlphttp/elastic]
```

### Authentication

**Self-hosted:**
```bash
ELASTIC_APM_SERVER_URL=https://apm.internal:8200
ELASTIC_APM_SECRET_TOKEN=<your-secret-token>
```

**Elastic Cloud** — get the APM endpoint and API key from:
```
Kibana → Observability → Add data → APM → OpenTelemetry
```

**No changes to builder-config.yaml required.**

### What you see in Kibana

**Logs Explorer** — filter:
```
service.name : "api-go" OR "worker-py" OR "notifier-js"
```

**APM** — error traces with full span waterfall across services.

---

## Splunk

**When to use:** Enterprises already using Splunk for log management or SIEM.

### 1. Add to builder-config.yaml

```yaml
exporters:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunkhecexporter v0.162.0
```

### 2. Collector config

```yaml
exporters:
  splunk_hec:
    token: ${env:SPLUNK_HEC_TOKEN}
    endpoint: https://splunk.internal:8088/services/collector/event
    source: hangtail
    sourcetype: otel_log
    index: app_errors

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [hangtail, batch]
      exporters: [splunk_hec]
```

### 3. Authentication

Create an HEC token in Splunk:
```
Settings → Data Inputs → HTTP Event Collector → New Token
```

Set `SPLUNK_HEC_TOKEN=<your-token>` in the collector's environment.

### 4. What you see in Splunk

```splunk
index=app_errors source=hangtail
| table _time, service.name, order_id, severity_text, body
| sort - _time
```

---

## Sending to Multiple Destinations

hangtail supports multiple exporters in a single pipeline — useful for sending to a SIEM (Splunk) and an observability platform (Datadog) simultaneously.

```yaml
exporters:
  datadog:
    api:
      key: ${env:DD_API_KEY}
  awscloudwatchlogs:
    log_group_name: /apps/my-service/errors
    region: ${env:AWS_REGION}

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [hangtail, batch]
      exporters: [datadog, awscloudwatchlogs]   # both receive the same forwarded flows
```

---

## Summary

| Provider | Exporter package | Auth | Native OTLP |
|---|---|---|---|
| AWS CloudWatch | `awscloudwatchlogsexporter` | IAM Role / Access Key | ✗ |
| GCP Cloud Logging | `googlecloudexporter` | Workload Identity / SA JSON | ✗ |
| Azure Monitor | `azuremonitorexporter` | Connection String / Managed Identity | ✗ |
| Datadog | `datadogexporter` | API Key | ✗ |
| New Relic | `otlphttp` (built-in) | License Key (header) | ✓ |
| Elastic | `otlphttp` (built-in) | Bearer / API Key (header) | ✓ |
| Splunk | `splunkhecexporter` | HEC Token | ✗ |

**Native OTLP providers** (New Relic, Elastic) require no changes to the builder config — easiest to get started with.

---

## Exporter versions

All exporters from `opentelemetry-collector-contrib` must use the same version as the core collector. If core is `v0.162.0`, use contrib `v0.162.0` as well.

```yaml
# builder-config.yaml — keep all versions aligned
exporters:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awscloudwatchlogsexporter v0.162.0
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter v0.162.0
```
