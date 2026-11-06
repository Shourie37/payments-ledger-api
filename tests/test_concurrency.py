import random
import threading

from conftest import assert_invariants

from ledger import service


def run_threads(n, target):
    barrier = threading.Barrier(n)

    def wrapped(i):
        barrier.wait()
        target(i)

    threads = [threading.Thread(target=wrapped, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def test_concurrent_random_transfers_preserve_balances(client):
    accounts = [client.account(5_000) for _ in range(4)]
    statuses = []
    lock = threading.Lock()

    def worker(i):
        rng = random.Random(i)
        for _ in range(25):
            src, dst = rng.sample(accounts, 2)
            resp = client.pay(src, dst, rng.randint(1, 2_000))
            with lock:
                statuses.append((resp.status, resp.json.get("error", {}).get("code")))

    run_threads(8, worker)
    assert {s for s, _ in statuses} <= {201, 422}
    assert all(code == "insufficient_funds" for s, code in statuses if s == 422)
    balances = [client.balance(a) for a in accounts]
    assert sum(balances) == 20_000
    assert min(balances) >= 0
    assert_invariants(client)


def test_concurrent_overdraw_never_goes_negative(client):
    src, dst = client.account(100), client.account(0)
    results = []
    run_threads(20, lambda i: results.append(client.pay(src, dst, 10).status))
    assert results.count(201) == 10
    assert results.count(422) == 10
    assert client.balance(src) == 0 and client.balance(dst) == 100
    assert_invariants(client)


def test_version_conflict_is_retried_with_fresh_read(client, monkeypatch):
    src, dst, other = client.account(100), client.account(0), client.account(0)
    real_read = service.Ledger._read_accounts
    state = {"interfered": False}

    def read_then_interfere(self, conn, ids):
        rows = real_read(self, conn, ids)
        if not state["interfered"] and src in ids:
            # Another writer spends 80 between our read and our write.
            state["interfered"] = True
            client.pay(src, other, 80)
        return rows

    monkeypatch.setattr(service.Ledger, "_read_accounts", read_then_interfere)
    resp = client.pay(src, dst, 50)
    assert resp.status == 422 and resp.json["error"]["code"] == "insufficient_funds"
    assert client.app.metrics.get("version_conflicts_total") == 1
    assert client.balance(src) == 20
    assert_invariants(client)


def test_balance_check_constraint_is_last_line_of_defense(client):
    import sqlite3

    import pytest

    a = client.account(10)
    with client.app.db.connect() as conn, pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE accounts SET balance = -1 WHERE id = ?", (a,))
