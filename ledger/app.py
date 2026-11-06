"""WSGI application: routing, auth, idempotency and JSON I/O."""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from collections.abc import Callable, Iterable
from urllib.parse import parse_qs

from .auth import InMemoryWindowStore, SlidingWindowCounter, TokenAuthenticator, WindowStore
from .config import Settings
from .cursor import CursorCodec
from .db import Database
from .errors import ApiError, bad_request
from .idempotency import IdempotencyStore, request_hash, validate_key
from .observability import Metrics, log_event, request_id_var
from .service import Ledger

log = logging.getLogger("ledger.http")

MAX_BODY_BYTES = 64 * 1024
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{8,64}$")
STATUS_TEXT = {
    200: "OK",
    201: "Created",
    400: "Bad Request",
    401: "Unauthorized",
    404: "Not Found",
    405: "Method Not Allowed",
    409: "Conflict",
    413: "Payload Too Large",
    415: "Unsupported Media Type",
    422: "Unprocessable Entity",
    500: "Internal Server Error",
    503: "Service Unavailable",
}

Handler = Callable[["Request"], tuple[int, dict]]


class Request:
    def __init__(self, environ: dict, params: dict[str, str]):
        self.environ = environ
        self.method = environ["REQUEST_METHOD"]
        self.path = environ.get("PATH_INFO", "/")
        self.params = params
        self.query = {k: v[0] for k, v in parse_qs(environ.get("QUERY_STRING", "")).items()}
        self.subject = "anonymous"
        self.replayed = False
        self._body: dict | None = None

    def header(self, name: str) -> str | None:
        return self.environ.get("HTTP_" + name.upper().replace("-", "_"))

    def json(self) -> dict:
        if self._body is None:
            if self.environ.get("CONTENT_TYPE", "").split(";")[0].strip() != "application/json":
                raise ApiError(415, "unsupported_media_type", "send Content-Type: application/json")
            length = int(self.environ.get("CONTENT_LENGTH") or 0)
            if length > MAX_BODY_BYTES:
                raise ApiError(413, "payload_too_large", "request body is too large")
            raw = self.environ["wsgi.input"].read(length) if length else b"{}"
            try:
                body = json.loads(raw)
            except ValueError as exc:
                raise bad_request("request body is not valid JSON") from exc
            if not isinstance(body, dict):
                raise bad_request("request body must be a JSON object")
            self._body = body
        return self._body


class App:
    def __init__(self, settings: Settings, replay_store: WindowStore | None = None, clock: Callable[[], float] = time.time):
        self.settings = settings
        self.metrics = Metrics()
        self.db = Database(settings.db_path)
        self.ledger = Ledger(self.metrics, CursorCodec(settings.cursor_secret), settings.max_conflict_retries)
        self.idempotency = IdempotencyStore(self.metrics, settings.idempotency_lock_seconds, clock)
        counter = SlidingWindowCounter(
            replay_store or InMemoryWindowStore(clock), settings.replay_window_seconds, settings.token_max_uses, clock
        )
        self.auth = TokenAuthenticator(settings.auth_secret, counter, self.metrics, settings.replay_fail_mode, clock)
        self.routes: list[tuple[str, re.Pattern[str], str, Handler, bool]] = []
        self._route("GET", "/health", self.health, auth=False)
        self._route("GET", "/metrics", self.get_metrics, auth=False)
        self._route("POST", "/v1/accounts", self.create_account)
        self._route("GET", "/v1/accounts/{id}", self.get_account)
        self._route("POST", "/v1/payments", self.create_payment)
        self._route("GET", "/v1/payments/{id}", self.get_payment)
        self._route("POST", "/v1/payments/{id}/refunds", self.create_refund)
        self._route("GET", "/v1/ledger_entries", self.list_entries)
        self._route("GET", "/v1/invariants", self.invariants)

    def _route(self, method: str, template: str, handler: Handler, auth: bool = True) -> None:
        pattern = re.compile("^" + re.sub(r"\{(\w+)\}", r"(?P<\1>[A-Za-z0-9_]+)", template) + "$")
        self.routes.append((method, pattern, template, handler, auth))

    # WSGI entry point

    def __call__(self, environ: dict, start_response: Callable) -> Iterable[bytes]:
        started = time.perf_counter()
        incoming = environ.get("HTTP_X_REQUEST_ID", "")
        request_id = incoming if REQUEST_ID_RE.match(incoming) else "req_" + uuid.uuid4().hex[:20]
        token = request_id_var.set(request_id)
        template = "unmatched"
        request: Request | None = None
        try:
            try:
                request, handler, template, needs_auth = self._match(environ)
                if needs_auth and self.settings.require_auth:
                    request.subject = self.auth.authenticate(environ.get("HTTP_AUTHORIZATION"))
                status, body = handler(request)
                headers: dict[str, str] = {}
            except ApiError as exc:
                status, body, headers = exc.status, exc.body(), exc.headers
            except Exception:
                log.exception("unhandled_error")
                status, body, headers = 500, {"error": {"code": "internal_error", "message": "internal error"}}, {}
            replayed = request is not None and request.replayed
            if "error" in body:
                body = {**body, "request_id": request_id}
            payload = json.dumps(body).encode()
            headers = {
                **headers,
                "Content-Type": "application/json",
                "Content-Length": str(len(payload)),
                "X-Request-Id": request_id,
            }
            if replayed:
                headers["Idempotent-Replayed"] = "true"
            start_response(f"{status} {STATUS_TEXT.get(status, 'Unknown')}", list(headers.items()))
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            self.metrics.incr("http_requests_total", route=f"{environ['REQUEST_METHOD']} {template}", status=status)
            log_event(
                log,
                "request",
                method=environ["REQUEST_METHOD"],
                route=template,
                status=status,
                duration_ms=duration_ms,
                replayed=replayed,
            )
            return [payload]
        finally:
            request_id_var.reset(token)

    def _match(self, environ: dict) -> tuple[Request, Handler, str, bool]:
        path = environ.get("PATH_INFO", "/")
        allowed = False
        for method, pattern, template, handler, needs_auth in self.routes:
            m = pattern.match(path)
            if not m:
                continue
            allowed = True
            if method == environ["REQUEST_METHOD"]:
                return Request(environ, m.groupdict()), handler, template, needs_auth
        if allowed:
            raise ApiError(405, "method_not_allowed", "method not allowed on this path")
        raise ApiError(404, "not_found", "route not found")

    # Handlers

    def health(self, req: Request) -> tuple[int, dict]:
        return 200, {"status": "ok"}

    def get_metrics(self, req: Request) -> tuple[int, dict]:
        return 200, self.metrics.snapshot()

    def create_account(self, req: Request) -> tuple[int, dict]:
        return self._idempotent(req, lambda conn, fin: self.ledger.create_account(conn, req.json(), fin), required=False)

    def get_account(self, req: Request) -> tuple[int, dict]:
        with self.db.connect() as conn:
            return 200, self.ledger.get_account(conn, req.params["id"])

    def create_payment(self, req: Request) -> tuple[int, dict]:
        return self._idempotent(req, lambda conn, fin: self.ledger.create_payment(conn, req.json(), fin), required=True)

    def get_payment(self, req: Request) -> tuple[int, dict]:
        with self.db.connect() as conn:
            return 200, self.ledger.get_payment(conn, req.params["id"])

    def create_refund(self, req: Request) -> tuple[int, dict]:
        payment_id = req.params["id"]
        return self._idempotent(
            req, lambda conn, fin: self.ledger.refund_payment(conn, payment_id, req.json(), fin), required=True
        )

    def list_entries(self, req: Request) -> tuple[int, dict]:
        try:
            limit = int(req.query.get("limit", "20"))
        except ValueError as exc:
            raise bad_request("limit must be an integer") from exc
        with self.db.connect() as conn:
            return 200, self.ledger.list_entries(conn, req.query.get("account_id"), limit, req.query.get("cursor"))

    def invariants(self, req: Request) -> tuple[int, dict]:
        with self.db.connect() as conn:
            return 200, self.ledger.check_invariants(conn)

    def _idempotent(self, req: Request, run: Callable, required: bool) -> tuple[int, dict]:
        body = req.json()
        key = validate_key(req.header("Idempotency-Key"), required)
        with self.db.connect() as conn:
            if key is None:
                return 201, run(conn, None)
            # Keys are per caller, so two clients cannot collide on one key.
            scoped_key = f"{req.subject}:{key}"
            status, result, replayed = self.idempotency.execute(
                conn, scoped_key, request_hash(req.method, req.path, body), lambda fin: run(conn, fin)
            )
        req.replayed = replayed
        if replayed and status >= 400:
            raise ApiError(status, result["error"]["code"], result["error"]["message"])
        return status, result
