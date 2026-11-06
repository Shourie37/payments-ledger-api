"""Accounts, payments, refunds and the double-entry ledger.

Money moves in two phases. The read phase loads account rows and checks the
business rules. The write phase applies the change only if each row still has
the version that was read. If another request changed a row first, the write
phase rolls back and the whole operation runs again with fresh data.
"""

from __future__ import annotations

import logging
import random
import re
import sqlite3
import time
from collections.abc import Callable

from . import outbox
from .cursor import CursorCodec
from .db import new_id, write_txn
from .errors import ApiError, bad_request, not_found
from .observability import Metrics, log_event

log = logging.getLogger("ledger.service")

MAX_AMOUNT = 10**12
CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

# Called inside the write transaction, so the stored response commits with the change.
Finalizer = Callable[[sqlite3.Connection, int, dict], None]


class VersionConflict(Exception):
    pass


def _amount(value: object, field: str = "amount") -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise bad_request(f"{field} must be an integer in minor units")
    if value <= 0 or value > MAX_AMOUNT:
        raise bad_request(f"{field} must be between 1 and {MAX_AMOUNT}")
    return value


def _string(body: dict, field: str, max_len: int = 200, required: bool = True) -> str | None:
    value = body.get(field)
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value or len(value) > max_len:
        raise bad_request(f"{field} must be a non-empty string up to {max_len} characters")
    return value


def account_view(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "object": "account",
        "name": row["name"],
        "currency": row["currency"],
        "balance": row["balance"],
        "version": row["version"],
        "created_at": row["created_at"],
    }


def payment_view(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "object": "payment",
        "source_account_id": row["source_id"],
        "destination_account_id": row["destination_id"],
        "amount": row["amount"],
        "currency": row["currency"],
        "status": row["status"],
        "refunded_amount": row["refunded_amount"],
        "description": row["description"],
        "created_at": row["created_at"],
    }


def entry_view(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "object": "ledger_entry",
        "transaction_id": row["txn_id"],
        "account_id": row["account_id"],
        "direction": row["direction"],
        "amount": row["amount"],
        "currency": row["currency"],
        "created_at": row["created_at"],
    }


class Ledger:
    def __init__(self, metrics: Metrics, cursors: CursorCodec, max_retries: int = 8):
        self.metrics = metrics
        self.cursors = cursors
        self.max_retries = max_retries

    # Accounts

    def create_account(self, conn: sqlite3.Connection, body: dict, finalize: Finalizer | None = None) -> dict:
        name = _string(body, "name")
        currency = body.get("currency")
        if not isinstance(currency, str) or not CURRENCY_RE.match(currency):
            raise bad_request("currency must be a 3-letter uppercase code such as USD")
        initial = body.get("initial_balance", 0)
        if initial != 0:
            initial = _amount(initial, "initial_balance")
        account_id = new_id("acct")
        now = time.time()
        with write_txn(conn):
            conn.execute(
                "INSERT INTO accounts (id, name, currency, created_at) VALUES (?, ?, ?, ?)",
                (account_id, name, currency, now),
            )
            if initial:
                # Outside money enters through a system account, so the books still balance.
                system_id = self._system_account(conn, currency, now)
                snapshot = self._read_accounts(conn, [system_id, account_id])
                self._post(conn, new_id("fund"), snapshot, [(system_id, "debit", initial), (account_id, "credit", initial)], now)
            result = account_view(self._get_account_row(conn, account_id))
            if finalize:
                finalize(conn, 201, result)
        self.metrics.incr("accounts_created_total")
        return result

    def get_account(self, conn: sqlite3.Connection, account_id: str) -> dict:
        return account_view(self._get_account_row(conn, account_id))

    # Payments

    def create_payment(self, conn: sqlite3.Connection, body: dict, finalize: Finalizer | None = None) -> dict:
        source_id = _string(body, "source_account_id", 64)
        destination_id = _string(body, "destination_account_id", 64)
        amount = _amount(body.get("amount"))
        currency = body.get("currency")
        description = _string(body, "description", 500, required=False)
        if source_id == destination_id:
            raise bad_request("source and destination must be different accounts")

        def attempt() -> dict:
            accounts = self._read_accounts(conn, [source_id, destination_id])
            src, dst = accounts.get(source_id), accounts.get(destination_id)
            if src is None or dst is None or src["allow_negative"] or dst["allow_negative"]:
                raise not_found("account")
            if src["currency"] != dst["currency"] or currency != src["currency"]:
                raise ApiError(422, "currency_mismatch", "both accounts and the payment must use the same currency")
            if src["balance"] < amount:
                raise ApiError(422, "insufficient_funds", "source account balance is too low")
            payment_id = new_id("pay")
            now = time.time()
            with write_txn(conn):
                self._post(conn, payment_id, accounts, [(source_id, "debit", amount), (destination_id, "credit", amount)], now)
                conn.execute(
                    "INSERT INTO payments (id, source_id, destination_id, amount, currency, status, description, created_at)"
                    " VALUES (?, ?, ?, ?, ?, 'succeeded', ?, ?)",
                    (payment_id, source_id, destination_id, amount, currency, description, now),
                )
                result = payment_view(self._get_payment_row(conn, payment_id))
                outbox.enqueue(conn, "payment.succeeded", result, now)
                if finalize:
                    finalize(conn, 201, result)
            return result

        result = self._with_retry(attempt)
        self.metrics.incr("payments_created_total")
        log_event(log, "payment_created", payment_id=result["id"], amount=amount, currency=currency)
        return result

    def get_payment(self, conn: sqlite3.Connection, payment_id: str) -> dict:
        return payment_view(self._get_payment_row(conn, payment_id))

    # Refunds

    def refund_payment(self, conn: sqlite3.Connection, payment_id: str, body: dict, finalize: Finalizer | None = None) -> dict:
        requested = body.get("amount")
        if requested is not None:
            requested = _amount(requested)

        def attempt() -> dict:
            payment = self._get_payment_row(conn, payment_id)
            remaining = payment["amount"] - payment["refunded_amount"]
            if remaining == 0:
                raise ApiError(422, "payment_already_refunded", "payment is already fully refunded")
            amount = remaining if requested is None else requested
            if amount > remaining:
                raise ApiError(422, "refund_exceeds_payment", f"refund amount is more than the {remaining} that remains")
            payer, payee = payment["source_id"], payment["destination_id"]
            accounts = self._read_accounts(conn, [payer, payee])
            if accounts[payee]["balance"] < amount:
                raise ApiError(422, "insufficient_funds", "destination account balance is too low to refund")
            refund_id = new_id("re")
            status = "refunded" if amount == remaining else "partially_refunded"
            now = time.time()
            with write_txn(conn):
                cur = conn.execute(
                    "UPDATE payments SET refunded_amount = refunded_amount + ?, status = ?, version = version + 1"
                    " WHERE id = ? AND version = ?",
                    (amount, status, payment_id, payment["version"]),
                )
                if cur.rowcount != 1:
                    raise VersionConflict(payment_id)
                self._post(conn, refund_id, accounts, [(payee, "debit", amount), (payer, "credit", amount)], now)
                conn.execute(
                    "INSERT INTO refunds (id, payment_id, amount, created_at) VALUES (?, ?, ?, ?)",
                    (refund_id, payment_id, amount, now),
                )
                result = {
                    "id": refund_id,
                    "object": "refund",
                    "payment_id": payment_id,
                    "amount": amount,
                    "currency": payment["currency"],
                    "payment_status": status,
                    "created_at": now,
                }
                outbox.enqueue(conn, "refund.succeeded", result, now)
                if finalize:
                    finalize(conn, 201, result)
            return result

        result = self._with_retry(attempt)
        self.metrics.incr("refunds_created_total")
        log_event(log, "refund_created", refund_id=result["id"], payment_id=payment_id, amount=result["amount"])
        return result

    # Ledger entries

    def list_entries(self, conn: sqlite3.Connection, account_id: str | None, limit: int, cursor: str | None) -> dict:
        if not 1 <= limit <= 100:
            raise bad_request("limit must be between 1 and 100")
        scope = f"account:{account_id or '*'}"
        after = self.cursors.decode(cursor, scope) if cursor else 0
        sql = "SELECT * FROM ledger_entries WHERE seq > ?"
        args: list[object] = [after]
        if account_id:
            self._get_account_row(conn, account_id)
            sql += " AND account_id = ?"
            args.append(account_id)
        rows = conn.execute(sql + " ORDER BY seq LIMIT ?", (*args, limit + 1)).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = self.cursors.encode(rows[-1]["seq"], scope) if has_more else None
        return {"object": "list", "data": [entry_view(r) for r in rows], "has_more": has_more, "next_cursor": next_cursor}

    # Invariants

    def check_invariants(self, conn: sqlite3.Connection) -> dict:
        signed = "SUM(CASE direction WHEN 'credit' THEN amount ELSE -amount END)"
        unbalanced = conn.execute(
            f"SELECT txn_id, currency, {signed} AS net FROM ledger_entries GROUP BY txn_id, currency HAVING net != 0"
        ).fetchall()
        drift = conn.execute(
            f"SELECT a.id, a.balance, COALESCE((SELECT {signed} FROM ledger_entries e WHERE e.account_id = a.id), 0) AS derived"
            " FROM accounts a WHERE a.balance != derived"
        ).fetchall()
        negative = conn.execute("SELECT id, balance FROM accounts WHERE allow_negative = 0 AND balance < 0").fetchall()
        over_refunded = conn.execute(
            "SELECT p.id FROM payments p WHERE p.refunded_amount !="
            " COALESCE((SELECT SUM(amount) FROM refunds r WHERE r.payment_id = p.id), 0)"
        ).fetchall()
        violations = (
            [f"unbalanced transaction {r['txn_id']} ({r['net']})" for r in unbalanced]
            + [f"account {r['id']} balance {r['balance']} != entries {r['derived']}" for r in drift]
            + [f"account {r['id']} is negative ({r['balance']})" for r in negative]
            + [f"payment {r['id']} refunded_amount does not match refunds" for r in over_refunded]
        )
        totals = conn.execute(f"SELECT currency, COALESCE({signed}, 0) AS net FROM ledger_entries GROUP BY currency").fetchall()
        return {"ok": not violations, "violations": violations, "net_by_currency": {r["currency"]: r["net"] for r in totals}}

    # Internals

    def _with_retry(self, attempt: Callable[[], dict]) -> dict:
        for n in range(self.max_retries + 1):
            try:
                return attempt()
            except VersionConflict as exc:
                self.metrics.incr("version_conflicts_total")
                log_event(log, "version_conflict_retry", logging.DEBUG, row=str(exc), attempt=n + 1)
                time.sleep(random.uniform(0, 0.002 * 2**n))
        raise ApiError(409, "concurrent_update", "too many concurrent updates, retry the request", {"Retry-After": "1"})

    def _read_accounts(self, conn: sqlite3.Connection, ids: list[str]) -> dict[str, sqlite3.Row]:
        marks = ",".join("?" * len(ids))
        rows = conn.execute(f"SELECT * FROM accounts WHERE id IN ({marks})", ids).fetchall()
        return {r["id"]: r for r in rows}

    def _post(
        self,
        conn: sqlite3.Connection,
        txn_id: str,
        snapshot: dict[str, sqlite3.Row],
        legs: list[tuple[str, str, int]],
        now: float,
    ) -> None:
        """Write balanced entries and apply them to balances with a version check."""
        net = sum(amount if direction == "credit" else -amount for _, direction, amount in legs)
        if net != 0:
            raise AssertionError(f"unbalanced journal {txn_id}")
        for account_id, direction, amount in legs:
            row = snapshot[account_id]
            delta = amount if direction == "credit" else -amount
            cur = conn.execute(
                "UPDATE accounts SET balance = ?, version = version + 1 WHERE id = ? AND version = ?",
                (row["balance"] + delta, account_id, row["version"]),
            )
            if cur.rowcount != 1:
                raise VersionConflict(account_id)
            conn.execute(
                "INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount, currency, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (new_id("ent"), txn_id, account_id, direction, amount, row["currency"], now),
            )

    def _system_account(self, conn: sqlite3.Connection, currency: str, now: float) -> str:
        system_id = f"acct_external_{currency.lower()}"
        conn.execute(
            "INSERT OR IGNORE INTO accounts (id, name, currency, allow_negative, created_at) VALUES (?, ?, ?, 1, ?)",
            (system_id, f"External funding ({currency})", currency, now),
        )
        return system_id

    def _get_account_row(self, conn: sqlite3.Connection, account_id: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
        if row is None:
            raise not_found("account")
        return row

    def _get_payment_row(self, conn: sqlite3.Connection, payment_id: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM payments WHERE id = ?", (payment_id,)).fetchone()
        if row is None:
            raise not_found("payment")
        return row
