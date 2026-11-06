"""Opaque, tamper-evident pagination cursors."""

from __future__ import annotations

import time

from .errors import ApiError
from .signing import InvalidToken, derive_key, sign, verify

CURSOR_TTL_SECONDS = 24 * 3600


class CursorCodec:
    def __init__(self, secret: bytes, ttl: int = CURSOR_TTL_SECONDS):
        self._key = derive_key(secret, "cursor:v1")
        self._ttl = ttl

    def encode(self, last_seq: int, scope: str) -> str:
        # Scope binds the cursor to its filter, so it cannot page another account.
        return sign({"v": 1, "after": last_seq, "scope": scope, "exp": int(time.time()) + self._ttl}, self._key)

    def decode(self, token: str, scope: str) -> int:
        try:
            payload = verify(token, self._key)
        except InvalidToken as exc:
            raise ApiError(400, "invalid_cursor", "cursor is invalid or was modified") from exc
        if payload.get("scope") != scope:
            raise ApiError(400, "invalid_cursor", "cursor does not match this query")
        if payload.get("exp", 0) < time.time():
            raise ApiError(400, "invalid_cursor", "cursor has expired")
        after = payload.get("after")
        if not isinstance(after, int) or after < 0:
            raise ApiError(400, "invalid_cursor", "cursor is invalid or was modified")
        return after
