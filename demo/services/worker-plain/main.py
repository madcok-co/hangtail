import json
import os
import random
import time
from datetime import datetime, timezone

import redis

LOG_FILE  = os.environ.get("LOG_FILE", "/var/log/app/worker.log")
FAIL_RATE = int(os.environ.get("FAIL_RATE", "30"))
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
SERVICE = "worker-plain"

os.makedirs("/var/log/app", exist_ok=True)
log_fh = open(LOG_FILE, "a", buffering=1)  # line-buffered


def ts() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def write_log(level: str, request_id: str, msg: str, extra: dict = None):
    entry = {"ts": ts(), "level": level, "service": SERVICE,
             "request_id": request_id, "msg": msg}
    if extra:
        entry.update(extra)
    log_fh.write(json.dumps(entry) + "\n")


def process(request_id: str, item: str, amount: float):
    write_log("INFO", request_id, "processing order", {"item": item, "amount": amount})
    time.sleep(0.08)

    if random.randint(1, 100) <= FAIL_RATE:
        write_log("ERROR", request_id, "payment declined", {"error": "card declined"})
        return

    # Success — signal hangtail to drop this flow
    write_log("INFO", request_id, "order completed", {"flow.end": "true"})


def main():
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    print(f"worker-plain started  FAIL_RATE={FAIL_RATE}%", flush=True)

    while True:
        try:
            item = r.brpop("orders", timeout=2)
            if item is None:
                continue
            _, raw = item
            data = json.loads(raw)
            process(data["request_id"], data["item"], data["amount"])
        except redis.exceptions.ConnectionError:
            time.sleep(1)
        except Exception as exc:
            print(f"error: {exc}", flush=True)
            time.sleep(0.5)


if __name__ == "__main__":
    main()
