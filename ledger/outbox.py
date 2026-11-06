"""Transactional outbox and webhook delivery worker."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import random
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Callable

from .db import Database, new_id, write_txn
from .observability import Metrics, log_event

log = logging.getLogger("ledger.outbox")

Sender = Callable[[dict], None]


def enqueue(conn: sqlite3.Connection, event_type: str, data: dict, now: float | None = None) -> str:
    """Write an event. Call it inside the same transaction as the state change."""
    now = time.time() if now is None else now
    event_id = new_id("evt")
    payload = json.dumps({"id": event_id, "type": event_type, "created_at": now, "data": data})
    conn.execute(
        "INSERT INTO outbox_events (id, type, payload, status, next_attempt_at, created_at) VALUES (?, ?, ?, 'pending', ?, ?)",
        (event_id, event_type, payload, now, now),
    )
    return event_id


def backoff_delay(attempts: int, base: float, cap: float, rng: random.Random) -> float:
    # Full jitter: spread retries so a recovering receiver is not hit all at once.
    return rng.uniform(0, min(cap, base * (2 ** (attempts - 1))))


class HttpSender:
    def __init__(self, url: str, secret: bytes, timeout: float = 5.0):
        if urllib.parse.urlsplit(url).scheme not in ("http", "https"):
            raise ValueError("webhook URL must use http or https")
        self.url = url
        self.secret = secret
        self.timeout = timeout

    def __call__(self, event: dict) -> None:
        body = json.dumps(event).encode()
        ts = str(int(time.time()))
        sig = hmac.new(self.secret, f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
        req = urllib.request.Request(  # noqa: S310 - the scheme is checked in __init__
            self.url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "Webhook-Id": event["id"], "Webhook-Signature": f"t={ts},v1={sig}"},
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # noqa: S310
            if resp.status >= 300:
                raise RuntimeError(f"receiver returned {resp.status}")


def log_sender(event: dict) -> None:
    log_event(log, "webhook_delivered_to_log", event_id=event["id"], type=event["type"])


class OutboxWorker:
    def __init__(
        self,
        db: Database,
        sender: Sender,
        metrics: Metrics,
        max_attempts: int = 6,
        base_delay: float = 1.0,
        max_delay: float = 300.0,
        lease_seconds: float = 30.0,
        clock: Callable[[], float] = time.time,
        rng: random.Random | None = None,
    ):
        self.db = db
        self.sender = sender
        self.metrics = metrics
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.lease_seconds = lease_seconds
        self.clock = clock
        self.rng = rng or random.Random()
        self._stop = threading.Event()

    def run_once(self, batch: int = 50) -> int:
        """Deliver due events once. Returns the number of events tried."""
        with self.db.connect() as conn:
            tried = 0
            for event_id in self._due(conn, batch):
                if self._claim(conn, event_id):
                    self._deliver(conn, event_id)
                    tried += 1
            return tried

    def run_forever(self, interval: float = 1.0) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                log.exception("outbox_worker_error")
            self._stop.wait(interval)

    def stop(self) -> None:
        self._stop.set()

    def _due(self, conn: sqlite3.Connection, batch: int) -> list[str]:
        rows = conn.execute(
            "SELECT id FROM outbox_events WHERE status = 'pending' AND next_attempt_at <= ? ORDER BY next_attempt_at LIMIT ?",
            (self.clock(), batch),
        ).fetchall()
        return [r["id"] for r in rows]

    def _claim(self, conn: sqlite3.Connection, event_id: str) -> bool:
        # A lease lets several workers run without sending the same event at once.
        now = self.clock()
        cur = conn.execute(
            "UPDATE outbox_events SET locked_until = ? WHERE id = ? AND status = 'pending'"
            " AND (locked_until IS NULL OR locked_until <= ?)",
            (now + self.lease_seconds, event_id, now),
        )
        return cur.rowcount == 1

    def _deliver(self, conn: sqlite3.Connection, event_id: str) -> None:
        row = conn.execute("SELECT * FROM outbox_events WHERE id = ?", (event_id,)).fetchone()
        event = json.loads(row["payload"])
        try:
            self.sender(event)
        except Exception as exc:
            self._record_failure(conn, row, f"{type(exc).__name__}: {exc}")
            return
        conn.execute(
            "UPDATE outbox_events SET status = 'delivered', attempts = attempts + 1, locked_until = NULL WHERE id = ?",
            (event_id,),
        )
        self.metrics.incr("outbox_delivered_total")
        log_event(log, "webhook_delivered", event_id=event_id, attempts=row["attempts"] + 1)

    def _record_failure(self, conn: sqlite3.Connection, row: sqlite3.Row, error: str) -> None:
        attempts = row["attempts"] + 1
        now = self.clock()
        with write_txn(conn):
            if attempts >= self.max_attempts:
                conn.execute(
                    "UPDATE outbox_events SET status = 'dead', attempts = ?, last_error = ?, locked_until = NULL WHERE id = ?",
                    (attempts, error, row["id"]),
                )
                conn.execute(
                    "INSERT INTO webhook_dead_letters (event_id, type, payload, attempts, last_error, failed_at)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (row["id"], row["type"], row["payload"], attempts, error, now),
                )
            else:
                delay = backoff_delay(attempts, self.base_delay, self.max_delay, self.rng)
                conn.execute(
                    "UPDATE outbox_events SET attempts = ?, last_error = ?, next_attempt_at = ?, locked_until = NULL"
                    " WHERE id = ?",
                    (attempts, error, now + delay, row["id"]),
                )
        if attempts >= self.max_attempts:
            self.metrics.incr("outbox_dead_lettered_total")
            log_event(log, "webhook_dead_lettered", logging.ERROR, event_id=row["id"], attempts=attempts, error=error)
        else:
            self.metrics.incr("outbox_retry_scheduled_total")
            log_event(log, "webhook_retry_scheduled", logging.WARNING, event_id=row["id"], attempts=attempts, error=error)
