"""API error type that maps to an HTTP status and a stable error code."""

from __future__ import annotations


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, headers: dict[str, str] | None = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.headers = headers or {}

    def body(self) -> dict:
        return {"error": {"code": self.code, "message": self.message}}


def bad_request(message: str, code: str = "invalid_request") -> ApiError:
    return ApiError(400, code, message)


def not_found(what: str) -> ApiError:
    return ApiError(404, "not_found", f"{what} not found")
