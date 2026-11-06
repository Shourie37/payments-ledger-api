import json
import random
import time

from ledger.outbox import OutboxWorker, backoff_delay


class FakeClock:
    def __init__(self):
        self.now = time.time() + 1

    def __call__(self):
        return self.now


class FlakySender:
    def __init__(self, failures):
        self.failures = failures
        self.calls = []

    def __call__(self, event):
        self.calls.append(event["id"])
        if len(self.calls) <= self.failures:
            raise ConnectionError("receiver down")


def events(app):
    with app.db.connect() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM outbox_events ORDER BY created_at")]


def make_worker(app, sender, clock, **kw):
    return OutboxWorker(app.db, sender, app.metrics, clock=clock, rng=random.Random(7), base_delay=1, max_delay=60, **kw)


def test_payment_writes_event_in_same_transaction(client):
    a, b = client.account(100), client.account(0)
    pay = client.pay(a, b, 40).json
    evs = [e for e in events(client.app) if e["type"] == "payment.succeeded"]
    assert len(evs) == 1 and json.loads(evs[0]["payload"])["data"]["id"] == pay["id"]
    client.pay(a, b, 40, key="dup")
    client.pay(a, b, 40, key="dup")
    assert len([e for e in events(client.app) if e["type"] == "payment.succeeded"]) == 2


def test_failed_delivery_is_retried_with_backoff(client):
    a, b = client.account(100), client.account(0)
    client.pay(a, b, 10)
    clock = FakeClock()
    sender = FlakySender(failures=2)
    worker = make_worker(client.app, sender, clock)

    assert worker.run_once() == 1
    ev = events(client.app)[0]
    assert ev["status"] == "pending" and ev["attempts"] == 1
    assert clock.now <= ev["next_attempt_at"] <= clock.now + 1
    assert worker.run_once() == 0  # not due yet

    clock.now = ev["next_attempt_at"]
    worker.run_once()
    ev = events(client.app)[0]
    assert ev["attempts"] == 2 and ev["next_attempt_at"] <= clock.now + 2

    clock.now = ev["next_attempt_at"]
    worker.run_once()
    ev = events(client.app)[0]
    assert ev["status"] == "delivered" and ev["attempts"] == 3
    assert len(sender.calls) == 3
    assert client.app.metrics.get("outbox_delivered_total") == 1


def test_event_goes_to_dead_letter_after_max_attempts(client):
    a, b = client.account(100), client.account(0)
    client.pay(a, b, 10)
    clock = FakeClock()
    worker = make_worker(client.app, FlakySender(failures=99), clock, max_attempts=4)
    for _ in range(10):
        clock.now += 1_000
        worker.run_once()
    ev = events(client.app)[0]
    assert ev["status"] == "dead" and ev["attempts"] == 4
    with client.app.db.connect() as conn:
        dlq = conn.execute("SELECT * FROM webhook_dead_letters").fetchall()
    assert len(dlq) == 1 and dlq[0]["event_id"] == ev["id"] and "receiver down" in dlq[0]["last_error"]
    assert client.app.metrics.get("outbox_dead_lettered_total") == 1


def test_leased_event_is_not_sent_twice(client):
    a, b = client.account(100), client.account(0)
    client.pay(a, b, 10)
    clock = FakeClock()
    sender = FlakySender(failures=0)
    w1, w2 = make_worker(client.app, sender, clock), make_worker(client.app, sender, clock)
    with client.app.db.connect() as conn:
        event_id = events(client.app)[0]["id"]
        assert w1._claim(conn, event_id) is True
        assert w2._claim(conn, event_id) is False


def test_backoff_is_capped_and_jittered():
    rng = random.Random(1)
    delays = [backoff_delay(n, base=1, cap=30, rng=rng) for n in range(1, 12)]
    assert all(0 <= d <= min(30, 2 ** (n - 1)) for n, d in zip(range(1, 12), delays))
    assert len(set(delays)) == len(delays)
