"""Payload-safe JSON logging for the Azure-MemForest service."""

from __future__ import annotations

import datetime as dt
import json
import logging
import sys
from typing import Any


_CONFIGURED = False


class JsonLogFormatter(logging.Formatter):
    """Format LogRecords as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": dt.datetime.fromtimestamp(record.created, tz=dt.UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        request_id = getattr(record, "request_id", "")
        trace_id = getattr(record, "trace_id", "")
        if request_id:
            payload["request_id"] = str(request_id)
        if trace_id:
            payload["trace_id"] = str(trace_id)
        for key, value in getattr(record, "safe_fields", {}).items():
            payload[str(key)] = _json_safe(value)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def configure_service_logging(*, level: str | int = "INFO") -> None:
    """Configure process logging for service entrypoints.

    This intentionally lives outside ``create_app`` so tests and embedding
    applications can control global logging state themselves.
    """

    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLogFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(logger_name)
        logger.handlers = []
        logger.propagate = True
    _CONFIGURED = True


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return str(value)
