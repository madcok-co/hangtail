'use strict'
/**
 * notifier-js — notification sender for hangtail demo.
 *
 * Consumes jobs from Redis 'notifications:queue', simulates sending a
 * push notification (FAIL_RATE % chance of failure).
 *
 * Success path: emit log with flow.end=true → hangtail drops buffered logs.
 * Failure path: emit error log, no flow.end  → hangtail forwards all logs
 *               for this trace_id after segment_max_age.
 */

const { NodeTracerProvider, BatchSpanProcessor } = require('@opentelemetry/sdk-trace-node')
const { OTLPTraceExporter }   = require('@opentelemetry/exporter-trace-otlp-http')
const { LoggerProvider, BatchLogRecordProcessor } = require('@opentelemetry/sdk-logs')
const { OTLPLogExporter }     = require('@opentelemetry/exporter-logs-otlp-http')
const { Resource }            = require('@opentelemetry/resources')
const { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } = require('@opentelemetry/semantic-conventions')
const { trace, context, propagation } = require('@opentelemetry/api')
const { logs, SeverityNumber } = require('@opentelemetry/api-logs')
const { createClient }        = require('redis')

const FAIL_RATE    = parseInt(process.env.FAIL_RATE    || '10',  10)
const REDIS_HOST   = process.env.REDIS_HOST            || 'redis'
const OTLP         = process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://collector:4318'
const SERVICE_NAME = process.env.OTEL_SERVICE_NAME     || 'notifier-js'

// ── OTel setup ────────────────────────────────────────────────────────────────
const resource = new Resource({
  [ATTR_SERVICE_NAME]:    SERVICE_NAME,
  [ATTR_SERVICE_VERSION]: '0.1.0',
})

const traceProvider = new NodeTracerProvider({ resource })
traceProvider.addSpanProcessor(new BatchSpanProcessor(
  new OTLPTraceExporter({ url: `${OTLP}/v1/traces` })
))
// register() sets the global tracer provider AND the W3C propagator
traceProvider.register()

const loggerProvider = new LoggerProvider({ resource })
loggerProvider.addLogRecordProcessor(new BatchLogRecordProcessor(
  new OTLPLogExporter({ url: `${OTLP}/v1/logs` })
))
logs.setGlobalLoggerProvider(loggerProvider)

const tracer     = trace.getTracer(SERVICE_NAME)
const otelLogger = logs.getLogger(SERVICE_NAME)

// ── helpers ───────────────────────────────────────────────────────────────────
function emit(severityNumber, severityText, body, attributes) {
  otelLogger.emit({
    severityNumber,
    severityText,
    body,
    attributes,
    context: context.active(),   // attaches current trace_id + span_id
  })
}

// ── main ──────────────────────────────────────────────────────────────────────
async function main() {
  const redis = createClient({ socket: { host: REDIS_HOST, port: 6379 } })
  redis.on('error', err => console.error('[redis]', err.message))
  await redis.connect()

  emit(SeverityNumber.INFO, 'INFO', 'notifier-js ready', { fail_rate_pct: FAIL_RATE })

  while (true) {
    let result
    try {
      result = await redis.brPop('notifications:queue', 5)
    } catch (err) {
      console.error('redis brPop error:', err.message)
      await new Promise(r => setTimeout(r, 1000))
      continue
    }
    if (!result) continue

    const msg      = JSON.parse(result.element)
    const orderId  = msg.order_id
    const carrier  = msg.traceparent ? { traceparent: msg.traceparent } : {}

    // Restore trace context from upstream (worker-py)
    const upstreamCtx = propagation.extract(context.active(), carrier)

    await context.with(upstreamCtx, async () => {
      const span    = tracer.startSpan('send_notification', {}, context.active())
      const spanCtx = trace.setSpan(context.active(), span)

      await context.with(spanCtx, async () => {
        span.setAttribute('order_id', orderId)
        emit(SeverityNumber.INFO, 'INFO', 'sending notification', { order_id: orderId })

        await new Promise(r => setTimeout(r, Math.random() * 50))  // simulate I/O

        if (Math.random() * 100 < FAIL_RATE) {
          emit(SeverityNumber.ERROR, 'ERROR', 'notification failed', {
            order_id: orderId,
            error:    'delivery timeout',
          })
          span.setStatus({ code: 2, message: 'notification failed' })
          span.end()
          // No flow.end → hangtail forwards all logs for this trace_id
          return
        }

        // ✓ Success: signal hangtail to drop this flow's buffered logs
        emit(SeverityNumber.INFO, 'INFO', 'notification sent', {
          order_id:  orderId,
          'flow.end': 'true',
        })
        span.end()
      })
    })
  }
}

main().catch(err => {
  console.error('fatal:', err)
  process.exit(1)
})
