#!/usr/bin/env python3
"""worker-py — async order processor for hangtail demo.

Consumes jobs from Redis 'orders:queue', runs payment processing
(FAIL_RATE % chance of failure), and pushes successful jobs to
'notifications:queue'.  All telemetry goes to the OTel collector.

Failure path: log error, do NOT push to notifications → flow.end is
never sent → hangtail forwards all logs for that trace_id after
segment_max_age.
"""
import json
import logging
import os
import random
import time

import redis as redislib
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME as RES_SERVICE_NAME

# Logs bridge (underscore path is stable across 1.14–1.30+)
try:
    from opentelemetry.sdk.logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk.logs.export import BatchLogRecordProcessor
    from opentelemetry.exporter.otlp.proto.http.log_exporter import OTLPLogExporter
    from opentelemetry.sdk.logs import set_logger_provider
except ImportError:
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler  # type: ignore[no-redef]
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor   # type: ignore[no-redef]
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter  # type: ignore[no-redef]
    from opentelemetry._logs import set_logger_provider  # type: ignore[no-redef]

FAIL_RATE    = int(os.getenv("FAIL_RATE", "30"))
REDIS_HOST   = os.getenv("REDIS_HOST", "redis")
OTLP         = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "worker-py")

propagator = TraceContextTextMapPropagator()


def setup_otel() -> tuple:
    resource = Resource.create({RES_SERVICE_NAME: SERVICE_NAME, "service.version": "0.1.0"})

    tp = TracerProvider(resource=resource)
    tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTLP}/v1/traces")))
    trace.set_tracer_provider(tp)

    lp = LoggerProvider(resource=resource)
    lp.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter(endpoint=f"{OTLP}/v1/logs")))
    set_logger_provider(lp)

    # Bridge stdlib logging → OTel OTLP.
    # Use format="%(message)s" so the body contains only the message, not
    # the "INFO:logger_name:" prefix that basicConfig adds by default.
    handler = LoggingHandler(level=logging.INFO, logger_provider=lp)
    logging.basicConfig(level=logging.INFO, handlers=[handler], format="%(message)s")
    # Suppress chatty third-party loggers that would flood the tail output
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)
    logging.getLogger("opentelemetry").setLevel(logging.WARNING)

    return tp, lp


def current_traceparent() -> str:
    carrier: dict = {}
    propagator.inject(carrier)
    return carrier.get("traceparent", "")


def main() -> None:
    tp, lp = setup_otel()
    tracer = trace.get_tracer(SERVICE_NAME)
    logger = logging.getLogger(SERVICE_NAME)

    r = redislib.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    logger.info("worker-py ready", extra={"fail_rate_pct": FAIL_RATE})

    try:
        while True:
            item = r.brpop("orders:queue", timeout=5)
            if item is None:
                continue

            _, raw = item
            msg = json.loads(raw)
            order_id    = msg["order_id"]
            traceparent = msg.get("traceparent", "")

            # Restore upstream trace context so all spans share one trace_id
            carrier  = {"traceparent": traceparent} if traceparent else {}
            upstream = propagator.extract(carrier)

            with tracer.start_as_current_span("process_order", context=upstream) as span:
                span.set_attribute("order_id", order_id)
                logger.info("processing order", extra={"order_id": order_id})

                time.sleep(random.uniform(0.01, 0.05))

                if random.randint(0, 99) < FAIL_RATE:
                    logger.error(
                        "payment declined",
                        extra={"order_id": order_id, "error": "card declined"},
                    )
                    span.set_status(trace.StatusCode.ERROR, "payment declined")
                    # No push to notifications:queue → flow.end never sent
                    # → hangtail forwards all logs for this trace_id
                    continue

                logger.info("payment accepted", extra={"order_id": order_id})

                # Propagate context to next hop
                notify = {"order_id": order_id, "traceparent": current_traceparent()}
                r.lpush("notifications:queue", json.dumps(notify))
    finally:
        tp.shutdown()
        lp.shutdown()


if __name__ == "__main__":
    main()
