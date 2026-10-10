# Cloud Cost Analysis

> **Disclaimer:** The figures in this document are **estimates** based on a specific scenario and public list prices as of October 2026. Actual costs will vary depending on region, commitment plans (reserved/savings plans), enterprise agreements, traffic patterns, and deployment architecture. Use this as a ballpark reference, not a firm quote.

---

## Scenario

| Parameter | Value |
|---|---|
| Log volume | 1,000,000 records/second |
| Record size | 5 KB (structured JSON, nested fields) |
| Raw throughput | ~4.8 GB/s |
| Failure rate | 5% (flows that never send `flow.end`) |
| Log compression ratio | 10:1 (typical for JSON logs) |
| Scale | 3 collector instances (horizontal, consistent hash by trace/request ID) |

**Context:** Each flow consists of ~10 log records (1 request = 10 log lines), yielding ~100,000 flows/second with ~5,000 failed flows/second ultimately forwarded to the log backend.

---

## Volume Breakdown

| | GB/month | TB/month |
|---|---|---|
| Raw input (all incoming logs) | 12,531,281 GB | ~12,238 TB |
| Forwarded to backend (5%) | 626,564 GB | ~612 TB |
| **Forwarded after compression** | **62,656 GB** | **~61 TB** |

95% of all logs are discarded by hangtail after `segment_max_age` — they never reach the log backend.

---

## Instance Specifications

Instances are chosen with **built-in local NVMe storage**. Using managed disks (EBS io2, GCP Persistent Disk SSD) to sustain 1.6 GB/s write per instance would make IOPS costs far exceed compute costs.

| Provider | Instance | vCPU | RAM | Local storage |
|---|---|---|---|---|
| AWS | i4i.2xlarge | 8 | 64 GB | 2× 1.25 TB NVMe |
| GCP | n2-highmem-8 + Local SSD | 8 | 64 GB | 1.5 TB (4× 375 GB) |
| Azure | Standard_L8s_v3 | 8 | 64 GB | 1× 1.92 TB NVMe |

Recommended hangtail config at this scale:

```yaml
extension:
  hangtail:
    max_staging_bytes: 150GiB   # raise from the 20GiB default
    segment_max_age: 1m         # 1-minute window → ~95 GB staging per instance
```

---

## Estimated Cost With hangtail

Prices based on **us-east-1 / us-central1 / eastus** regions (on-demand, no reserved instances).

### AWS (us-east-1)

| Component | Detail | Cost/month |
|---|---|---|
| Compute | 3× i4i.2xlarge @ $0.757/hr × 730 hrs | $1,658 |
| S3 Standard | 62,656 GB compressed @ $0.023/GB | $1,441 |
| S3 PUT requests | ~131M PUTs/month @ $0.000005 | $657 |
| Data transfer | Intra-region (services → collector → S3) | $0 |
| **Total** | | **~$3,756/month** |

### GCP (us-central1)

| Component | Detail | Cost/month |
|---|---|---|
| Compute | 3× n2-highmem-8 @ $0.3948/hr | $865 |
| Local SSD | 3× 1.5 TB @ $0.048/GB/month | $216 |
| GCS Standard | 62,656 GB @ $0.020/GB | $1,253 |
| **Total** | | **~$2,334/month** |

### Azure (eastus)

| Component | Detail | Cost/month |
|---|---|---|
| Compute | 3× Standard_L8s_v3 @ $0.624/hr | $1,367 |
| Blob Storage (Hot) | 62,656 GB @ $0.018/GB | $1,128 |
| **Total** | | **~$2,494/month** |

---

## Estimated Cost Without hangtail

If all logs are forwarded and stored directly in the backend with no filtering:

| Component | Detail | Cost/month |
|---|---|---|
| Storage | 1,224 TB compressed @ $0.023/GB (S3) | $28,822 |
| Indexing | 1,253,128 GB @ $0.10/GB (OpenSearch/Loki) | $125,313 |
| **Total** | | **~$154,135/month** |

---

## ROI

| | Cost/month |
|---|---|
| With hangtail (AWS) | ~$3,756 |
| Without hangtail | ~$154,135 |
| **Savings** | **~$150,000/month** |
| **Ratio** | **~40:1** |

The biggest savings come not from compute, but from **eliminating backend load**: 95% of logs that are never relevant to an incident don't need to be indexed, stored, or queried at all.

---

## Additional Notes

- **Asia Pacific regions** (Singapore, Jakarta) are typically 10–20% more expensive than us-east-1
- **Reserved Instances / Committed Use Discounts** can reduce compute costs by 30–40%
- **SaaS log platforms** amplify hangtail's savings further — at Datadog ($0.10/GB ingested), the no-hangtail ingestion cost alone reaches ~$122,000/month
- The AWS S3 PUT cost ($657/month) disappears if using an OTLP exporter directly to Loki or OpenSearch instead of S3
- This scenario assumes all traffic flows through hangtail. For further efficiency, filter out logs that don't need correlation (healthchecks, metrics scrapes) upstream before they reach the processor
