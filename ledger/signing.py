"""HMAC-SHA256 signed tokens: base64url(json payload) + "." + base64url(tag).

The payload is signed, not encrypted. The standard library has no AES, and a
hand-made cipher is a bigger risk than a readable payload. So callers must not
put secrets in the payload. Any change to the payload breaks the tag.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json


class InvalidToken(Exception):
    pass


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _b64decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def derive_key(secret: bytes, purpose: str) -> bytes:
    # One key per purpose, so a cursor can never pass as an auth token.
    return hmac.new(secret, purpose.encode(), hashlib.sha256).digest()


def sign(payload: dict, key: bytes) -> str:
    body = _b64encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode())
    tag = hmac.new(key, body.encode(), hashlib.sha256).digest()
    return f"{body}.{_b64encode(tag)}"


def verify(token: str, key: bytes) -> dict:
    try:
        body, tag = token.split(".")
        expected = hmac.new(key, body.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(expected, _b64decode(tag)):
            raise InvalidToken("bad signature")
        payload = json.loads(_b64decode(body))
    except InvalidToken:
        raise
    except (ValueError, TypeError) as exc:
        raise InvalidToken("malformed token") from exc
    if not isinstance(payload, dict):
        raise InvalidToken("malformed token")
    return payload
