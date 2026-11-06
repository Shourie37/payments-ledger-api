import threading

from conftest import assert_invariants


def setup_payment(client, amount=500):
    a, b = client.account(1000), client.account(0)
    pay = client.pay(a, b, amount).json
    return a, b, pay


def test_full_refund_restores_balances(client):
    a, b, pay = setup_payment(client)
    resp = client.post(f"/v1/payments/{pay['id']}/refunds", {}, key="r1")
    assert resp.status == 201 and resp.json["amount"] == 500
    assert resp.json["payment_status"] == "refunded"
    assert client.balance(a) == 1000 and client.balance(b) == 0
    assert client.get(f"/v1/payments/{pay['id']}").json["refunded_amount"] == 500
    assert_invariants(client)


def test_partial_refunds_up_to_payment_amount(client):
    _, _, pay = setup_payment(client)
    url = f"/v1/payments/{pay['id']}/refunds"
    assert client.post(url, {"amount": 200}, key="p1").json["payment_status"] == "partially_refunded"
    over = client.post(url, {"amount": 301}, key="p2")
    assert over.status == 422 and over.json["error"]["code"] == "refund_exceeds_payment"
    assert client.post(url, {"amount": 300}, key="p3").json["payment_status"] == "refunded"
    again = client.post(url, {"amount": 1}, key="p4")
    assert again.status == 422 and again.json["error"]["code"] == "payment_already_refunded"
    assert_invariants(client)


def test_refund_needs_funds_in_destination(client):
    a, b, pay = setup_payment(client)
    client.pay(b, client.account(0), 400)
    resp = client.post(f"/v1/payments/{pay['id']}/refunds", {}, key="r1")
    assert resp.status == 422 and resp.json["error"]["code"] == "insufficient_funds"
    assert client.balance(b) == 100
    assert_invariants(client)


def test_refund_unknown_payment_is_404(client):
    assert client.post("/v1/payments/pay_nope/refunds", {}, key="x").status == 404


def test_refund_is_idempotent(client):
    a, _, pay = setup_payment(client)
    url = f"/v1/payments/{pay['id']}/refunds"
    first = client.post(url, {"amount": 100}, key="same")
    second = client.post(url, {"amount": 100}, key="same")
    assert second.json == first.json and second.headers.get("Idempotent-Replayed") == "true"
    assert client.balance(a) == 600


def test_concurrent_full_refunds_only_one_wins(client):
    a, b, pay = setup_payment(client)
    url = f"/v1/payments/{pay['id']}/refunds"
    results = []
    barrier = threading.Barrier(8)

    def worker(i):
        barrier.wait()
        results.append(client.post(url, {}, key=f"race-{i}"))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(r.status == 201 for r in results) == 1
    assert all(r.json["error"]["code"] == "payment_already_refunded" for r in results if r.status != 201)
    assert client.balance(a) == 1000
    assert_invariants(client)
