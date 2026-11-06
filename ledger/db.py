"""SQLite schema and connection helpers."""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    currency       TEXT NOT NULL,
    balance        INTEGER NOT NULL DEFAULT 0,
    allow_negative INTEGER NOT NULL DEFAULT 0,
    version        INTEGER NOT NULL DEFAULT 0,
    created_at     REAL NOT NULL,
    CHECK (allow_negative = 1 OR balance >= 0)
);

CREATE TABLE IF NOT EXISTS payments (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES accounts(id),
    destination_id  TEXT NOT NULL REFERENCES accounts(id),
    amount          INTEGER NOT NULL CHECK (amount > 0),
    currency        TEXT NOT NULL,
    refunded_amount INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL,
    description     TEXT,
    version         INTEGER NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL,
    CHECK (refunded_amount >= 0 AND refunded_amount <= amount)
);

CREATE TABLE IF NOT EXISTS refunds (
    id         TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL REFERENCES payments(id),
    amount     INTEGER NOT NULL CHECK (amount > 0),
    created_at REAL NOT NULL
);

-- seq is internal and only leaves the service inside a signed cursor.
CREATE TABLE IF NOT EXISTS ledger_entries (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    id         TEXT NOT NULL UNIQUE,
    txn_id     TEXT NOT NULL,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    direction  TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount     INTEGER NOT NULL CHECK (amount > 0),
    currency   TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_entries_account ON ledger_entries(account_id, seq);
CREATE INDEX IF NOT EXISTS ix_entries_txn ON ledger_entries(txn_id);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key           TEXT PRIMARY KEY,
    request_hash  TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('processing', 'completed')),
    owner         TEXT NOT NULL,
    response_code INTEGER,
    response_body TEXT,
    locked_until  REAL NOT NULL,
    created_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id              TEXT PRIMARY KEY,
    type            TEXT NOT NULL,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending', 'delivered', 'dead')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL,
    locked_until    REAL,
    last_error      TEXT,
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_outbox_due ON outbox_events(status, next_attempt_at);

CREATE TABLE IF NOT EXISTS webhook_dead_letters (
    event_id   TEXT PRIMARY KEY REFERENCES outbox_events(id),
    type       TEXT NOT NULL,
    payload    TEXT NOT NULL,
    attempts   INTEGER NOT NULL,
    last_error TEXT,
    failed_at  REAL NOT NULL
);
"""


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:24]}"


class Database:
    def __init__(self, path: str):
        self.path = path
        with self.connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        # Autocommit mode, so each write path controls its own BEGIN.
        conn = sqlite3.connect(self.path, timeout=15, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
        finally:
            conn.close()


@contextmanager
def write_txn(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run a block in one write transaction. IMMEDIATE takes the write lock up front."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
