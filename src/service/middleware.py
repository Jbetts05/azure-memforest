"""HTTP middleware for service request boundaries."""

from __future__ import annotations

import re
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from src.service.trace_context import TRACEPARENT_HEADER, parse_traceparent

REQUEST_ID_HEADER = "x-request-id"
_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")

ASGIReceive = Callable[[], Awaitable[dict[str, Any]]]
ASGISend = Callable[[dict[str, Any]], Awaitable[None]]
ASGIApp = Callable[[dict[str, Any], ASGIReceive, ASGISend], Awaitable[None]]


class RequestIdMiddleware:
    """Attach a sanitized request id to request state and response headers."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        if scope.get("type") != "http":
            await self._app(scope, receive, send)
            return

        request_id = _request_id_from_scope(scope)
        state = scope.setdefault("state", {})
        state["request_id"] = request_id
        trace_context = _trace_context_from_scope(scope)
        if trace_context is not None:
            state["trace_id"] = trace_context.trace_id
            state["trace_parent_id"] = trace_context.parent_id
            state["trace_flags"] = trace_context.trace_flags

        async def send_with_request_id(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                headers = list(message.get("headers") or [])
                headers.append((REQUEST_ID_HEADER.encode("latin-1"), request_id.encode("latin-1")))
                message["headers"] = headers
            await send(message)

        await self._app(scope, receive, send_with_request_id)


def sanitize_request_id(value: str | None) -> str:
    """Return a safe request id, generating one when input is missing/unsafe."""

    candidate = str(value or "").strip()
    if _REQUEST_ID_PATTERN.fullmatch(candidate):
        return candidate
    return str(uuid.uuid4())


def _request_id_from_scope(scope: dict[str, Any]) -> str:
    for raw_name, raw_value in scope.get("headers") or []:
        if raw_name.decode("latin-1").lower() == REQUEST_ID_HEADER:
            return sanitize_request_id(raw_value.decode("latin-1"))
    return sanitize_request_id(None)


def _trace_context_from_scope(scope: dict[str, Any]):
    for raw_name, raw_value in scope.get("headers") or []:
        if raw_name.decode("latin-1").lower() == TRACEPARENT_HEADER:
            return parse_traceparent(raw_value.decode("latin-1"))
    return None
