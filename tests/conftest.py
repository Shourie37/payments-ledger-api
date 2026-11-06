from __future__ import annotations

import io
import json
import uuid
from dataclasses import dataclass
from urllib.parse import urlencode
from wsgiref.util import setup_testing_defaults

import pytest

from ledger.app import App
from ledger.config import Settings

AUTO = object()


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    json: dict


class Client:
    def __init__(self, app: App):
        self.app = app

    def request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        headers: dict | None = None,
        query: dict | None = None,
        token: object = AUTO,
    ) -> Response:
        environ: dict = {}
        setup_testing_defaults(environ)
        raw = json.dumps(body).encode() if body is not None else b""
        environ.update(
            {
                "REQUEST_METHOD": method,
                "PATH_INFO": path,
                "QUERY_STRING": urlencode(query or {}),
                "wsgi.input": io.BytesIO(raw),
                "CONTENT_LENGTH": str(len(raw)),
                "CONTENT_TYPE": "application/json" if body is not None else "",
            }
        )
        if token is AUTO:
            token = self.app.auth.issue("test-client")
        if token is not None:
            environ["HTTP_AUTHORIZATION"] = f"Bearer {token}"
        for name, value in (headers or {}).items():
            environ["HTTP_" + name.upper().replace("-", "_")] = value
        captured: dict = {}

        def start_response(status: str, hdrs: list) -> None:
            captured["status"] = int(status.split()[0])
            captured["headers"] = dict(hdrs)

        out = b"".join(self.app(environ, start_response))
        return Response(captured["status"], captured["headers"], json.loads(out))

    def get(self, path: str, **kw) -> Response:
        return self.request("GET", path, **kw)

    def post(self, path: str, body: dict, key: str | None = None, **kw) -> Response:
        headers = kw.pop("headers", {}) or {}
        if key is not None:
            headers["Idempotency-Key"] = key
        return self.request("POST", path, body, headers=headers, **kw)

    def account(self, balance: int = 0, currency: str = "USD") -> str:
        resp = self.post("/v1/accounts", {"name": "acct", "currency": currency, "initial_balance": balance})
        assert resp.status == 201, resp.json
        return resp.json["id"]

    def pay(self, src: str, dst: str, amount: int, key: str | None = None, currency: str = "USD") -> Response:
        body = {"source_account_id": src, "destination_account_id": dst, "amount": amount, "currency": currency}
        return self.post("/v1/payments", body, key=key or uuid.uuid4().hex)

    def balance(self, account_id: str) -> int:
        return self.get(f"/v1/accounts/{account_id}").json["balance"]


def make_settings(tmp_path, **overrides) -> Settings:
    base = dict(db_path=str(tmp_path / "test.db"), auth_secret=b"a" * 32, cursor_secret=b"c" * 32)
    base.update(overrides)
    return Settings(**base)


@pytest.fixture
def app(tmp_path) -> App:
    return App(make_settings(tmp_path))


@pytest.fixture
def client(app) -> Client:
    return Client(app)


def assert_invariants(client: Client) -> None:
    report = client.get("/v1/invariants").json
    assert report["ok"], report["violations"]
    assert all(net == 0 for net in report["net_by_currency"].values()), report
