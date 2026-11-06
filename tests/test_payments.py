from conftest import assert_invariants


def test_payment_writes_balanced_entries(client):
    a, b = client.account(1000), client.account(0)
    resp = client.pay(a, b, 250)
    assert resp.status == 201
    pay = resp.json
    assert pay["status"] == "succeeded" and pay["amount"] == 250
    assert client.balance(a) == 750 and client.balance(b) == 250
    entries = [e for e in client.get("/v1/ledger_entries", query={"limit": 100}).json["data"] if e["transaction_id"] == pay["id"]]
    assert sorted((e["account_id"], e["direction"], e["amount"]) for e in entries) == sorted(
        [(a, "debit", 250), (b, "credit", 250)]
    )
    assert client.get(f"/v1/payments/{pay['id']}").json == pay
    assert_invariants(client)


def test_insufficient_funds_rejected_without_side_effects(client):
    a, b = client.account(100), client.account(0)
    resp = client.pay(a, b, 101)
    assert resp.status == 422 and resp.json["error"]["code"] == "insufficient_funds"
    assert client.balance(a) == 100 and client.balance(b) == 0
    with client.app.db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM outbox_events WHERE type='payment.succeeded'").fetchone()[0] == 0
    assert_invariants(client)


def test_validation_errors(client):
    a, b = client.account(100), client.account(0, currency="EUR")
    assert client.pay(a, a, 10).json["error"]["code"] == "invalid_request"
    assert client.pay(a, b, 10).json["error"]["code"] == "currency_mismatch"
    assert client.pay(a, "acct_missing", 10).status == 404
    for bad in (0, -5, 1.5, "10", True):
        resp = client.post(
            "/v1/payments",
            {"source_account_id": a, "destination_account_id": b, "amount": bad, "currency": "USD"},
            key="k" + str(bad),
        )
        assert resp.status == 400, bad


def test_payment_requires_idempotency_key(client):
    a, b = client.account(100), client.account(0)
    resp = client.post("/v1/payments", {"source_account_id": a, "destination_account_id": b, "amount": 1, "currency": "USD"})
    assert resp.status == 400 and resp.json["error"]["code"] == "idempotency_key_required"


def test_system_account_cannot_be_used_as_payment_source(client):
    b = client.account(0)
    assert client.pay("acct_external_usd", b, 10).status == 404


def test_responses_carry_request_id(client):
    resp = client.get("/v1/payments/pay_missing", headers={"X-Request-Id": "trace_abc12345"})
    assert resp.status == 404
    assert resp.headers["X-Request-Id"] == "trace_abc12345"
    assert resp.json["request_id"] == "trace_abc12345"


def test_metrics_endpoint_counts_requests(client):
    a, b = client.account(10), client.account(0)
    client.pay(a, b, 5)
    counters = client.get("/metrics", token=None).json["counters"]
    assert counters["payments_created_total"] == 1
    assert counters['http_requests_total{route="POST /v1/payments",status="201"}'] == 1
