import base64
import json

from ledger.cursor import CursorCodec


def make_entries(client, n=5):
    a, b = client.account(1000), client.account(0)
    for _ in range(n):
        client.pay(a, b, 1)
    return a, b


def test_cursor_pages_through_all_entries_once(client):
    a, _ = make_entries(client, 5)
    seen, cursor = [], None
    while True:
        query = {"account_id": a, "limit": 2}
        if cursor:
            query["cursor"] = cursor
        page = client.get("/v1/ledger_entries", query=query).json
        seen += [e["id"] for e in page["data"]]
        cursor = page["next_cursor"]
        if not page["has_more"]:
            assert cursor is None
            break
    assert len(seen) == len(set(seen)) == 6  # 1 funding credit + 5 payment debits


def first_cursor(client, account_id):
    return client.get("/v1/ledger_entries", query={"account_id": account_id, "limit": 1}).json["next_cursor"]


def test_tampered_cursor_is_rejected(client):
    a, _ = make_entries(client)
    cursor = first_cursor(client, a)
    body, tag = cursor.split(".")
    payload = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    payload["after"] = 0
    forged_body = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    for bad in (f"{forged_body}.{tag}", cursor[:-2] + "AA", "garbage", cursor + "."):
        resp = client.get("/v1/ledger_entries", query={"account_id": a, "cursor": bad})
        assert resp.status == 400 and resp.json["error"]["code"] == "invalid_cursor", bad


def test_cursor_is_bound_to_its_query(client):
    a, b = make_entries(client)
    resp = client.get("/v1/ledger_entries", query={"account_id": b, "cursor": first_cursor(client, a)})
    assert resp.status == 400 and resp.json["error"]["code"] == "invalid_cursor"


def test_cursor_from_another_key_is_rejected(client):
    a, _ = make_entries(client)
    foreign = CursorCodec(b"other-secret").encode(1, f"account:{a}")
    assert client.get("/v1/ledger_entries", query={"account_id": a, "cursor": foreign}).status == 400


def test_expired_cursor_is_rejected():
    codec = CursorCodec(b"k" * 32, ttl=-1)
    import pytest

    from ledger.errors import ApiError

    with pytest.raises(ApiError) as exc:
        codec.decode(codec.encode(5, "account:*"), "account:*")
    assert exc.value.code == "invalid_cursor"


def test_cursor_does_not_expose_entry_ids(client):
    a, _ = make_entries(client)
    page = client.get("/v1/ledger_entries", query={"account_id": a, "limit": 1}).json
    assert page["data"][0]["id"] not in page["next_cursor"]
    assert "seq" not in page["data"][0]
