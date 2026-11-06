"""Idempotency keys for POST requests.

Each key has one row. The row starts in "processing" and moves to "completed"
in the same transaction that writes the business change. The primary key on
the row is what stops two concurrent requests with one key from both running.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import time
import uuid
from collections.abc import Callable

from .errors import ApiError
from .observability import Metrics, log_event
from .service import Finalizer

log = logging.getLogger("ledger.idempotency")

KEY_RE = re.compile(r"^[A-Za-z0-9_\-:.]{1,255}$")

# These errors can change on retry, so the key is released, not stored.
RETRYABLE_STATUSES = {401, 409, 429, 503}


def request_hash(method: str, path: str, body: dict) -> str:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{method} {path}\n{canonical}".encode()).hexdigest()


class IdempotencyStore:
    def __init__(self, metrics: Metrics, lock_seconds: float = 30.0, clock: Callable[[], float] = time.time):
        self.metrics = metrics
        self.lock_seconds = lock_seconds
        self.clock = clock

    def execute(
        self,
        conn: sqlite3.Connection,
        key: str,
        fingerprint: str,
        operation: Callable[[Finalizer], dict],
        success_status: int = 201,
    ) -> tuple[int, dict, bool]:
        """Run the operation once per key. Returns (status, body, replayed)."""
        owner = uuid.uuid4().hex
        stored = self._claim(conn, key, fingerprint, owner)
        if stored is not None:
            self.metrics.incr("idempotent_replays_total")
            log_event(log, "idempotent_replay", key=key, status=stored[0])
            return stored[0], stored[1], True

        def finalize(txn_conn: sqlite3.Connection, status: int, body: dict) -> None:
            # The owner check fences off a worker whose lock expired and was taken over.
            if not self._complete(txn_conn, key, owner, status, body):
                raise ApiError(409, "idempotency_in_progress", "another request now owns this idempotency key")

        try:
            body = operation(finalize)
        except ApiError as exc:
            if exc.status in RETRYABLE_STATUSES or exc.status >= 500:
                self._release(conn, key, owner)
            else:
                self._complete(conn, key, owner, exc.status, exc.body())
            raise
        except BaseException:
            self._release(conn, key, owner)
            raise
        return success_status, body, False

    def _claim(self, conn: sqlite3.Connection, key: str, fingerprint: str, owner: str) -> tuple[int, dict] | None:
        now = self.clock()
        try:
            conn.execute(
                "INSERT INTO idempotency_keys (key, request_hash, status, owner, locked_until, created_at)"
                " VALUES (?, ?, 'processing', ?, ?, ?)",
                (key, fingerprint, owner, now + self.lock_seconds, now),
            )
            return None
        except sqlite3.IntegrityError:
            pass
        row = conn.execute("SELECT * FROM idempotency_keys WHERE key = ?", (key,)).fetchone()
        if row is None:
            # The owner released the key between our insert and select.
            return self._claim(conn, key, fingerprint, owner)
        if row["request_hash"] != fingerprint:
            self.metrics.incr("idempotency_key_mismatch_total")
            raise ApiError(422, "idempotency_key_reused", "this idempotency key was used with a different request")
        if row["status"] == "completed":
            return row["response_code"], json.loads(row["response_body"])
        if row["locked_until"] > now:
            self.metrics.incr("idempotency_in_progress_total")
            raise ApiError(409, "idempotency_in_progress", "a request with this key is in progress", {"Retry-After": "1"})
        # The first owner seems to have died. Take the lock over with a conditional update.
        cur = conn.execute(
            "UPDATE idempotency_keys SET owner = ?, locked_until = ? WHERE key = ? AND status = 'processing' AND owner = ?",
            (owner, now + self.lock_seconds, key, row["owner"]),
        )
        if cur.rowcount != 1:
            raise ApiError(409, "idempotency_in_progress", "a request with this key is in progress", {"Retry-After": "1"})
        self.metrics.incr("idempotency_lock_takeover_total")
        log_event(log, "idempotency_lock_takeover", logging.WARNING, key=key)
        return None

    def _complete(self, conn: sqlite3.Connection, key: str, owner: str, status: int, body: dict) -> bool:
        cur = conn.execute(
            "UPDATE idempotency_keys SET status = 'completed', response_code = ?, response_body = ?"
            " WHERE key = ? AND owner = ? AND status = 'processing'",
            (status, json.dumps(body), key, owner),
        )
        return cur.rowcount == 1

    def _release(self, conn: sqlite3.Connection, key: str, owner: str) -> None:
        conn.execute("DELETE FROM idempotency_keys WHERE key = ? AND owner = ? AND status = 'processing'", (key, owner))


def validate_key(value: str | None, required: bool) -> str | None:
    if value is None:
        if required:
            raise ApiError(400, "idempotency_key_required", "send an Idempotency-Key header on this request")
        return None
    if not KEY_RE.match(value):
        raise ApiError(400, "invalid_idempotency_key", "Idempotency-Key must be 1-255 characters of [A-Za-z0-9_-:.]")
    return value
