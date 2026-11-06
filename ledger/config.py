"""Runtime settings, read from environment variables."""

from __future__ import annotations

import logging
import os
import secrets
from dataclasses import dataclass, field

log = logging.getLogger("ledger.config")


def _secret(name: str) -> bytes:
    value = os.environ.get(name)
    if value:
        return value.encode()
    # A random secret keeps local runs safe but breaks tokens on restart.
    log.warning("secret_not_set", extra={"fields": {"variable": name}})
    return secrets.token_bytes(32)


@dataclass
class Settings:
    db_path: str = "ledger.db"
    host: str = "127.0.0.1"
    port: int = 8080
    auth_secret: bytes = field(default_factory=lambda: secrets.token_bytes(32))
    cursor_secret: bytes = field(default_factory=lambda: secrets.token_bytes(32))
    require_auth: bool = True
    # "closed" rejects requests when the replay store fails; "open" lets them through.
    replay_fail_mode: str = "closed"
    replay_window_seconds: int = 300
    token_max_uses: int = 1
    idempotency_lock_seconds: int = 30
    max_conflict_retries: int = 8
    outbox_max_attempts: int = 6
    outbox_base_delay: float = 1.0
    outbox_max_delay: float = 300.0
    webhook_url: str | None = None
    webhook_secret: bytes = field(default_factory=lambda: secrets.token_bytes(32))

    def __post_init__(self) -> None:
        if self.replay_fail_mode not in ("open", "closed"):
            raise ValueError("replay_fail_mode must be 'open' or 'closed'")

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ.get
        return cls(
            db_path=env("LEDGER_DB_PATH", "ledger.db"),
            host=env("LEDGER_HOST", "127.0.0.1"),
            port=int(env("LEDGER_PORT", "8080")),
            auth_secret=_secret("LEDGER_AUTH_SECRET"),
            cursor_secret=_secret("LEDGER_CURSOR_SECRET"),
            require_auth=env("LEDGER_REQUIRE_AUTH", "true").lower() == "true",
            replay_fail_mode=env("LEDGER_REPLAY_FAIL_MODE", "closed"),
            replay_window_seconds=int(env("LEDGER_REPLAY_WINDOW_SECONDS", "300")),
            token_max_uses=int(env("LEDGER_TOKEN_MAX_USES", "1")),
            webhook_url=env("LEDGER_WEBHOOK_URL"),
            webhook_secret=_secret("LEDGER_WEBHOOK_SECRET"),
        )
