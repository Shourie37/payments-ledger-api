import pytest
from conftest import Client, make_settings

from ledger.app import App
from ledger.auth import InMemoryWindowStore, ReplayStoreError, SlidingWindowCounter, TokenAuthenticator
from ledger.observability import Metrics


class FakeClock:
    def __init__(self, now=1_000_000.0):
        self.now = now

    def __call__(self):
        return self.now


class BrokenStore:
    def incr(self, key, bucket, ttl):
        raise ReplayStoreError("cache unreachable")

    def get(self, key, bucket):
        raise ReplayStoreError("cache unreachable")


def test_reused_token_is_rejected(client):
    token = client.app.auth.issue("c1")
    assert client.get("/v1/invariants", token=token).status == 200
    resp = client.get("/v1/invariants", token=token)
    assert resp.status == 401 and resp.json["error"]["code"] == "token_replayed"
    assert client.app.metrics.get("token_replay_rejected_total") == 1


def test_fresh_tokens_are_accepted(client):
    for _ in range(5):
        assert client.get("/v1/invariants").status == 200


@pytest.mark.parametrize("token,code", [(None, "missing_token"), ("abc.def", "invalid_token")])
def test_missing_or_forged_token(client, token, code):
    resp = client.get("/v1/invariants", token=token)
    assert resp.status == 401 and resp.json["error"]["code"] == code
    assert "WWW-Authenticate" in resp.headers


def test_expired_token_is_rejected(tmp_path):
    clock = FakeClock()
    app = App(make_settings(tmp_path), clock=clock)
    token = app.auth.issue("c1", ttl=60)
    clock.now += 61
    assert Client(app).get("/v1/invariants", token=token).json["error"]["code"] == "token_expired"


def test_token_signed_with_other_secret_is_rejected(client):
    other = TokenAuthenticator(b"z" * 32, SlidingWindowCounter(InMemoryWindowStore(), 300, 1), Metrics())
    resp = client.get("/v1/invariants", token=other.issue("c1"))
    assert resp.json["error"]["code"] == "invalid_token"


def test_fail_closed_rejects_when_store_is_down(tmp_path):
    app = App(make_settings(tmp_path, replay_fail_mode="closed"), replay_store=BrokenStore())
    resp = Client(app).get("/v1/invariants")
    assert resp.status == 503 and resp.json["error"]["code"] == "auth_unavailable"
    assert app.metrics.get("replay_check_fail_closed_total") == 1


def test_fail_open_allows_when_store_is_down(tmp_path):
    app = App(make_settings(tmp_path, replay_fail_mode="open"), replay_store=BrokenStore())
    token = app.auth.issue("c1")
    c = Client(app)
    assert c.get("/v1/invariants", token=token).status == 200
    # The cost of fail-open: a replay is not caught while the store is down.
    assert c.get("/v1/invariants", token=token).status == 200
    assert app.metrics.get("replay_check_fail_open_total") == 2


def test_bad_fail_mode_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        make_settings(tmp_path, replay_fail_mode="maybe")


def test_sliding_window_counter_limits_and_expires():
    clock = FakeClock(0.0)
    store = InMemoryWindowStore(clock)
    counter = SlidingWindowCounter(store, window=10, limit=3, clock=clock)
    assert [counter.hit("k") for _ in range(4)] == [True, True, True, False]
    # Early in the next window the previous count still weighs in.
    clock.now = 11.0
    assert counter.hit("k") is False
    # Two windows later the old buckets have expired.
    clock.now = 35.0
    assert counter.hit("k") is True
    assert len(store) == 1


def test_one_use_token_is_caught_across_bucket_boundary():
    clock = FakeClock(9.9)
    counter = SlidingWindowCounter(InMemoryWindowStore(clock), window=10, limit=1, clock=clock)
    assert counter.hit("jti") is True
    clock.now = 19.8
    assert counter.hit("jti") is False


def test_token_lifetime_longer_than_window_is_rejected(tmp_path):
    app = App(make_settings(tmp_path, replay_window_seconds=60))
    resp = Client(app).get("/v1/invariants", token=app.auth.issue("c1", ttl=3600))
    assert resp.json["error"]["code"] == "invalid_token"
