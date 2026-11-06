"""Bearer token check with replay detection.

The token format is a stand-in for tokens from a real identity provider. The
replay guard is the point here: it counts uses of each token ID (jti) in a
sliding window and rejects a token once it goes over its use limit.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable
from typing import Protocol

from .errors import ApiError
from .observability import Metrics, log_event
from .signing import InvalidToken, derive_key, sign, verify

log = logging.getLogger("ledger.auth")


class ReplayStoreError(Exception):
    """The counter store cannot answer, for example a cache outage."""


class WindowStore(Protocol):
    def incr(self, key: str, bucket: int, ttl: float) -> int: ...
    def get(self, key: str, bucket: int) -> int: ...


class InMemoryWindowStore:
    """Bucket counters with TTL. A shared cache such as Redis takes this role in a fleet."""

    def __init__(self, clock: Callable[[], float] = time.time):
        self._lock = threading.Lock()
        self._data: dict[tuple[str, int], tuple[int, float]] = {}
        self._clock = clock

    def incr(self, key: str, bucket: int, ttl: float) -> int:
        now = self._clock()
        with self._lock:
            self._purge(now)
            count, _ = self._data.get((key, bucket), (0, 0.0))
            self._data[(key, bucket)] = (count + 1, now + ttl)
            return count + 1

    def get(self, key: str, bucket: int) -> int:
        with self._lock:
            count, expires = self._data.get((key, bucket), (0, 0.0))
            return count if expires > self._clock() else 0

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def _purge(self, now: float) -> None:
        for k in [k for k, (_, exp) in self._data.items() if exp <= now]:
            del self._data[k]


class SlidingWindowCounter:
    """Two-bucket sliding window: estimate = prev * (1 - elapsed fraction) + current."""

    def __init__(self, store: WindowStore, window: float, limit: int, clock: Callable[[], float] = time.time):
        self.store = store
        self.window = window
        self.limit = limit
        self.clock = clock

    def hit(self, key: str) -> bool:
        now = self.clock()
        bucket, offset = divmod(now, self.window)
        bucket = int(bucket)
        previous = self.store.get(key, bucket - 1)
        current = self.store.incr(key, bucket, ttl=2 * self.window)
        estimate = previous * (1 - offset / self.window) + current
        return estimate <= self.limit


class TokenAuthenticator:
    def __init__(
        self,
        secret: bytes,
        counter: SlidingWindowCounter,
        metrics: Metrics,
        fail_mode: str = "closed",
        clock: Callable[[], float] = time.time,
    ):
        self._key = derive_key(secret, "auth:v1")
        self.counter = counter
        self.metrics = metrics
        self.fail_mode = fail_mode
        self.clock = clock

    def issue(self, subject: str, ttl: int | None = None) -> str:
        now = int(self.clock())
        ttl = int(ttl or self.counter.window)
        return sign({"sub": subject, "jti": uuid.uuid4().hex, "iat": now, "exp": now + ttl}, self._key)

    def authenticate(self, header: str | None) -> str:
        if not header or not header.startswith("Bearer "):
            raise _unauthorized("missing_token", "send Authorization: Bearer <token>")
        try:
            claims = verify(header[len("Bearer ") :].strip(), self._key)
        except InvalidToken as exc:
            raise _unauthorized("invalid_token", "token signature is not valid") from exc
        now = self.clock()
        iat, exp, jti, sub = claims.get("iat"), claims.get("exp"), claims.get("jti"), claims.get("sub")
        if not all(isinstance(v, int) for v in (iat, exp)) or not jti or not sub:
            raise _unauthorized("invalid_token", "token claims are not valid")
        if exp <= now:
            raise _unauthorized("token_expired", "token has expired")
        # A lifetime longer than the window lets a replay slip past the counter.
        if exp - iat > self.counter.window:
            raise _unauthorized("invalid_token", "token lifetime is longer than the replay window")
        self._check_replay(jti, sub)
        return sub

    def _check_replay(self, jti: str, subject: str) -> None:
        try:
            allowed = self.counter.hit(f"jti:{jti}")
        except ReplayStoreError as exc:
            if self.fail_mode == "open":
                self.metrics.incr("replay_check_fail_open_total")
                log_event(log, "replay_store_unavailable_fail_open", logging.WARNING, error=str(exc))
                return
            self.metrics.incr("replay_check_fail_closed_total")
            log_event(log, "replay_store_unavailable_fail_closed", logging.ERROR, error=str(exc))
            raise ApiError(503, "auth_unavailable", "cannot verify token now, retry later", {"Retry-After": "1"}) from exc
        if not allowed:
            self.metrics.incr("token_replay_rejected_total")
            log_event(log, "token_replay_rejected", logging.WARNING, subject=subject, jti=jti)
            raise _unauthorized("token_replayed", "token was already used")


def _unauthorized(code: str, message: str) -> ApiError:
    return ApiError(401, code, message, {"WWW-Authenticate": 'Bearer realm="ledger"'})
