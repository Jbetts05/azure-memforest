from __future__ import annotations

import subprocess
import sys
import json
import logging

from fastapi.testclient import TestClient
from fastapi import HTTPException, Request

from src import __version__
from src.service.app import create_app
from src.service.logging import JsonLogFormatter
from src.service.readiness import ReadinessCheckResult, StaticReadinessCheck
from src.service.settings import ServiceSettings, load_service_settings
from src.service.trace_context import parse_traceparent


def test_service_app_import_is_side_effect_light() -> None:
    code = """
import sys
import src.service.app as app
import src.service.middleware as middleware
import src.service.logging as service_logging
import src.service.readiness as readiness
import src.service.settings as settings
import src.service.trace_context as trace_context
forbidden = [
    'src.forest.memforest',
    'src.api.client',
    'src.extraction.fact_manager',
    'faiss',
    'openai',
]
loaded = [name for name in forbidden if name in sys.modules]
assert hasattr(app, 'create_app')
assert hasattr(middleware, 'RequestIdMiddleware')
assert hasattr(service_logging, 'JsonLogFormatter')
assert hasattr(readiness, 'evaluate_readiness')
assert hasattr(settings, 'load_service_settings')
assert hasattr(trace_context, 'parse_traceparent')
assert not loaded, loaded
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_live_health_returns_ok() -> None:
    client = TestClient(create_app())

    response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["x-request-id"]


def test_request_id_header_is_echoed_when_safe() -> None:
    client = TestClient(create_app())

    response = client.get("/health/live", headers={"x-request-id": "request-123"})

    assert response.status_code == 200
    assert response.headers["x-request-id"] == "request-123"


def test_unsafe_request_id_header_is_replaced() -> None:
    client = TestClient(create_app())

    response = client.get("/health/live", headers={"x-request-id": "bad\r\nvalue"})

    assert response.status_code == 200
    assert response.headers["x-request-id"]
    assert response.headers["x-request-id"] != "bad\r\nvalue"


def test_ready_health_is_not_ready_until_dependencies_are_configured() -> None:
    client = TestClient(create_app())

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "checks": {
            "configuration": {"status": "not_configured"},
            "azure": {"status": "not_configured"},
        },
    }


def test_version_endpoint_returns_minimal_service_identity() -> None:
    client = TestClient(create_app())

    response = client.get("/version")

    assert response.status_code == 200
    assert response.json() == {
        "name": "azure-memforest-api",
        "version": __version__,
    }


def test_ready_health_returns_ok_when_all_checks_pass() -> None:
    client = TestClient(
        create_app(
            readiness_checks=[
                StaticReadinessCheck(name="configuration", status="ok"),
                StaticReadinessCheck(name="azure", status="ok"),
            ]
        )
    )

    response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "checks": {
            "configuration": {"status": "ok"},
            "azure": {"status": "ok"},
        },
    }


def test_ready_health_sanitizes_check_exceptions() -> None:
    class BrokenCheck:
        name = "broken"

        async def check(self) -> ReadinessCheckResult:
            raise RuntimeError("secret connection string")

    client = TestClient(create_app(readiness_checks=[BrokenCheck()]))

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "checks": {
            "broken": {
                "status": "failed",
                "detail": "check raised",
            }
        },
    }
    assert "secret" not in str(response.json())


def test_ready_health_times_out_slow_checks() -> None:
    class SlowCheck:
        name = "slow"

        async def check(self) -> ReadinessCheckResult:
            import asyncio

            await asyncio.sleep(0.1)
            return ReadinessCheckResult(name=self.name, status="ok")

    client = TestClient(
        create_app(
            settings=ServiceSettings(readiness_timeout_seconds=0.01),
            readiness_checks=[SlowCheck()],
        )
    )

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["slow"] == {
        "status": "failed",
        "detail": "timeout",
    }


def test_service_settings_loads_safe_env_values() -> None:
    settings = load_service_settings(
        {
            "AZURE_MEMFOREST_ENV": "dev",
            "AZURE_MEMFOREST_READINESS_TIMEOUT_SECONDS": "3.5",
        }
    )

    assert settings.environment == "dev"
    assert settings.readiness_timeout_seconds == 3.5


def test_unhandled_exception_response_is_sanitized_and_correlated() -> None:
    app = create_app()

    @app.get("/__boom")
    def boom() -> None:
        raise RuntimeError("super secret failure")

    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/__boom", headers={"x-request-id": "boom-1"})

    assert response.status_code == 500
    assert response.headers["x-request-id"] == "boom-1"
    assert response.json() == {
        "status": "error",
        "error": "internal_server_error",
        "request_id": "boom-1",
    }
    assert "super secret" not in response.text


def test_http_exception_keeps_fastapi_status_behavior() -> None:
    app = create_app()

    @app.get("/__missing")
    def missing() -> None:
        raise HTTPException(status_code=404, detail="not here")

    client = TestClient(app)

    response = client.get("/__missing", headers={"x-request-id": "not-found-1"})

    assert response.status_code == 404
    assert response.headers["x-request-id"] == "not-found-1"
    assert response.json() == {"detail": "not here"}


def test_traceparent_parser_accepts_valid_context() -> None:
    context = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

    assert context is not None
    assert context.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert context.parent_id == "00f067aa0ba902b7"
    assert context.trace_flags == "01"


def test_traceparent_parser_rejects_invalid_contexts() -> None:
    assert parse_traceparent(None) is None
    assert parse_traceparent("bad") is None
    assert parse_traceparent("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01") is None
    assert parse_traceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01") is None
    assert parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01") is None


def test_valid_traceparent_is_available_to_routes_but_not_echoed() -> None:
    app = create_app()

    @app.get("/__trace")
    def trace(request: Request):
        return {
            "trace_id": getattr(request.state, "trace_id", ""),
            "parent_id": getattr(request.state, "trace_parent_id", ""),
        }

    client = TestClient(app)
    traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

    response = client.get("/__trace", headers={"traceparent": traceparent})

    assert response.status_code == 200
    assert response.json() == {
        "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
        "parent_id": "00f067aa0ba902b7",
    }
    assert "traceparent" not in response.headers


def test_json_log_formatter_outputs_context_fields() -> None:
    record = logging.LogRecord(
        name="src.service.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="safe message",
        args=(),
        exc_info=None,
    )
    record.request_id = "request-1"
    record.trace_id = "trace-1"
    record.safe_fields = {"operation": "health"}

    payload = json.loads(JsonLogFormatter().format(record))

    assert payload["message"] == "safe message"
    assert payload["request_id"] == "request-1"
    assert payload["trace_id"] == "trace-1"
    assert payload["operation"] == "health"


def test_json_log_formatter_handles_records_without_context() -> None:
    record = logging.LogRecord(
        name="src.service.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="safe message",
        args=(),
        exc_info=None,
    )

    payload = json.loads(JsonLogFormatter().format(record))

    assert payload["message"] == "safe message"
    assert "request_id" not in payload
    assert "trace_id" not in payload
