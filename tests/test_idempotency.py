import threading
import time
import uuid

from conftest import assert_invariants

from ledger.idempotency import request_hash


def insert_processing_key(client, key, src, dst, locked_until):
    """Simulate another request that holds the key."""
    body = {"source_account_id": src, "destination_account_id": dst, "amount": 10, "currency": "USD"}
    with client.app.db.connect() as conn:
        conn.execute(
            "INSERT INTO idempotency_keys (key, request_hash, status, owner, locked_until, created_at)"
            " VALUES (?, ?, 'processing', 'other-worker', ?, ?)",
            (f"test-client:{key}", request_hash("POST", "/v1/payments", body), locked_until, time.time()),
        )


def test_retry_with_same_key_returns_stored_response(client):
    a, b = client.account(1000), client.account(0)
    first = client.pay(a, b, 300, key="order-42")
    second = client.pay(a, b, 300, key="order-42")
    assert first.status == second.status == 201
    assert second.json == first.json
    assert second.headers.get("Idempotent-Replayed") == "true"
    assert "Idempotent-Replayed" not in first.headers
    assert client.balance(a) == 700
    assert_invariants(client)


def test_same_key_different_body_is_422(client):
    a, b = client.account(1000), client.account(0)
    assert client.pay(a, b, 300, key="order-43").status == 201
    resp = client.pay(a, b, 301, key="order-43")
    assert resp.status == 422 and resp.json["error"]["code"] == "idempotency_key_reused"
    assert client.balance(a) == 700


def test_business_error_is_stored_and_replayed(client):
    a, b = client.account(100), client.account(0)
    first = client.pay(a, b, 500, key="too-big")
    assert first.status == 422 and first.json["error"]["code"] == "insufficient_funds"
    client.pay(client.account(1000), a, 900)
    second = client.pay(a, b, 500, key="too-big")
    assert second.status == 422 and second.headers.get("Idempotent-Replayed") == "true"
    assert client.balance(a) == 1000


def test_keys_are_scoped_per_caller(client):
    a, b = client.account(1000), client.account(0)
    t1 = client.app.auth.issue("client-one")
    t2 = client.app.auth.issue("client-two")
    body = {"source_account_id": a, "destination_account_id": b, "amount": 10, "currency": "USD"}
    r1 = client.post("/v1/payments", body, key="shared", token=t1)
    r2 = client.post("/v1/payments", body, key="shared", token=t2)
    assert r1.status == r2.status == 201 and r1.json["id"] != r2.json["id"]


def test_key_in_processing_state_returns_409(client):
    a, b = client.account(1000), client.account(0)
    insert_processing_key(client, "busy", a, b, locked_until=time.time() + 60)
    resp = client.pay(a, b, 10, key="busy")
    assert resp.status == 409 and resp.json["error"]["code"] == "idempotency_in_progress"
    assert resp.headers["Retry-After"] == "1"
    assert client.balance(a) == 1000


def test_stale_processing_lock_is_taken_over(client):
    a, b = client.account(1000), client.account(0)
    insert_processing_key(client, "stale", a, b, locked_until=time.time() - 1)
    resp = client.pay(a, b, 10, key="stale")
    assert resp.status == 201
    assert client.app.metrics.get("idempotency_lock_takeover_total") == 1


def test_concurrent_duplicates_create_one_payment(client):
    a, b = client.account(10_000), client.account(0)
    key = uuid.uuid4().hex
    barrier = threading.Barrier(12)
    results = []

    def worker():
        barrier.wait()
        results.append(client.pay(a, b, 100, key=key))

    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    created = {r.json["id"] for r in results if r.status == 201}
    assert len(created) == 1
    assert all(r.status in (201, 409) for r in results)
    with client.app.db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM payments").fetchone()[0] == 1
    assert client.balance(a) == 9_900
    assert_invariants(client)


def test_unexpected_failure_releases_key(client, monkeypatch):
    a, b = client.account(1000), client.account(0)
    real = client.app.ledger.create_payment
    calls = {"n": 0}

    def flaky(conn, body, finalize=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("disk on fire")
        return real(conn, body, finalize)

    monkeypatch.setattr(client.app.ledger, "create_payment", flaky)
    assert client.pay(a, b, 10, key="flaky").status == 500
    assert client.pay(a, b, 10, key="flaky").status == 201
    assert client.balance(a) == 990
