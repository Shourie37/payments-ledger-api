"""Structured JSON logs with request IDs, and in-process metric counters."""

from __future__ import annotations

import contextvars
import json
import logging
import threading
import time
from collections import Counter

request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("request_id", default=None)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts": round(record.created, 3),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        request_id = request_id_var.get()
        if request_id:
            doc["request_id"] = request_id
        doc.update(getattr(record, "fields", {}))
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger("ledger")
    root.handlers[:] = [handler]
    root.setLevel(level)
    root.propagate = False


def log_event(logger: logging.Logger, msg: str, level: int = logging.INFO, **fields: object) -> None:
    logger.log(level, msg, extra={"fields": fields})


class Metrics:
    """Thread-safe counters. Label sets are flattened into the key, Prometheus style."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Counter[str] = Counter()
        self.started_at = time.time()

    def incr(self, name: str, value: int = 1, **labels: object) -> None:
        if labels:
            name += "{" + ",".join(f'{k}="{v}"' for k, v in sorted(labels.items())) + "}"
        with self._lock:
            self._counters[name] += value

    def get(self, name: str) -> int:
        with self._lock:
            return self._counters.get(name, 0)

    def snapshot(self) -> dict:
        with self._lock:
            counters = dict(sorted(self._counters.items()))
        return {"uptime_seconds": round(time.time() - self.started_at, 1), "counters": counters}
