from __future__ import annotations

import subprocess
import sys
import json
import logging

import pytest
from fastapi import Depends, HTTPException, Request
from fastapi.testclient import TestClient

from src import __version__
from src.service.app import create_app
from src.cloud.cosmos_jobs import JobNotFound
from src.service.auth import (
    ACTOR_ID_HEADER,
    ROLES_HEADER,
    SCOPES_HEADER,
    TENANT_ID_HEADER,
    AuthNotConfigured,
    DevHeaderAuthProvider,
    get_actor_context,
)
from src.service.logging import JsonLogFormatter
from src.service.readiness import ReadinessCheckResult, StaticReadinessCheck
from src.service.settings import ServiceSettings, load_service_settings
from src.service.trace_context import parse_traceparent


def test_service_app_import_is_side_effect_light() -> None:
    code = """
import sys
import src.service.app as app
import src.service.auth as auth
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
assert hasattr(auth, 'get_actor_context')
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
        "git_sha": "unknown",
        "image_dirty": "unknown",
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
            "AZURE_MEMFOREST_UI_ENABLED": "true",
            "AZURE_MEMFOREST_UI_ALLOWED_ORIGINS": "https://ui.example, http://localhost:5173",
        }
    )

    assert settings.environment == "dev"
    assert settings.readiness_timeout_seconds == 3.5
    assert settings.ui_enabled is True
    assert settings.ui_allowed_origins == ("https://ui.example", "http://localhost:5173")
    assert load_service_settings({}).ui_enabled is False
    assert load_service_settings({"AZURE_MEMFOREST_UI_ENABLED": "False"}).ui_enabled is False


def test_ui_cors_is_gated_by_feature_flag() -> None:
    app = create_app(
        settings=ServiceSettings(ui_enabled=True, ui_allowed_origins=("https://ui.example",)),
        auth_provider=DevHeaderAuthProvider(environment="test"),
    )
    client = TestClient(app)

    response = client.options(
        "/v1/users/user-1/sessions",
        headers={
            "Origin": "https://ui.example",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "Authorization",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://ui.example"


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


def test_dev_header_auth_provider_returns_actor_context() -> None:
    app = create_app(auth_provider=DevHeaderAuthProvider(environment="test"))

    @app.get("/__actor")
    async def actor(ctx=Depends(get_actor_context)):
        return {
            "tenant_id": ctx.tenant.tenant_id,
            "actor_id": ctx.actor_id,
            "scopes": list(ctx.scopes),
            "roles": list(ctx.roles),
            "auth_scheme": ctx.auth_scheme,
        }

    client = TestClient(app)
    response = client.get(
        "/__actor",
        headers={
            TENANT_ID_HEADER: "tenant-1",
            ACTOR_ID_HEADER: "actor@example.com",
            SCOPES_HEADER: "memory.read,memory.write",
            ROLES_HEADER: "admin",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "tenant_id": "tenant-1",
        "actor_id": "actor@example.com",
        "scopes": ["memory.read", "memory.write"],
        "roles": ["admin"],
        "auth_scheme": "dev-header",
    }


def test_dev_header_auth_provider_refuses_production_environment() -> None:
    with pytest.raises(ValueError, match="only allowed"):
        DevHeaderAuthProvider(environment="production")


def test_dev_header_auth_requires_explicit_environment() -> None:
    from src.service.app import _dev_auth_provider_from_env

    with pytest.raises(ValueError, match="AZURE_MEMFOREST_ENV"):
        _dev_auth_provider_from_env({"AZURE_MEMFOREST_DEV_AUTH": "true"})


def test_auth_dependency_fails_closed_when_not_configured() -> None:
    app = create_app()

    @app.get("/__actor")
    async def actor(ctx=Depends(get_actor_context)):
        return {"actor_id": ctx.actor_id}

    client = TestClient(app)
    response = client.get("/__actor", headers={"x-request-id": "auth-1"})

    assert response.status_code == 503
    assert response.headers["x-request-id"] == "auth-1"
    assert response.json() == {
        "status": "error",
        "error": "auth_not_configured",
        "request_id": "auth-1",
    }


def test_dev_header_auth_rejects_unsafe_values_without_echoing_them() -> None:
    app = create_app(auth_provider=DevHeaderAuthProvider(environment="test"))

    @app.get("/__actor")
    async def actor(ctx=Depends(get_actor_context)):
        return {"actor_id": ctx.actor_id}

    client = TestClient(app)
    response = client.get(
        "/__actor",
        headers={
            "x-request-id": "auth-2",
            TENANT_ID_HEADER: "tenant-1",
            ACTOR_ID_HEADER: "bad value with spaces",
        },
    )

    assert response.status_code == 401
    assert response.json() == {
        "status": "error",
        "error": "unauthorized",
        "request_id": "auth-2",
    }
    assert "bad value" not in response.text


def test_dev_header_auth_rejects_colon_in_partition_ids() -> None:
    app = create_app(auth_provider=DevHeaderAuthProvider(environment="test"))

    @app.get("/__actor")
    async def actor(ctx=Depends(get_actor_context)):
        return {"actor_id": ctx.actor_id}

    client = TestClient(app)
    response = client.get(
        "/__actor",
        headers={
            "x-request-id": "auth-3",
            TENANT_ID_HEADER: "tenant:1",
            ACTOR_ID_HEADER: "user-1",
        },
    )

    assert response.status_code == 401
    assert response.json()["error"] == "unauthorized"


def test_ingest_route_requires_auth_provider() -> None:
    client = TestClient(create_app())

    response = client.post(
        "/v1/users/user-1/sessions/session-1:ingest",
        headers={"x-request-id": "api-1", "Idempotency-Key": "idem-1"},
        json={"turns": [{"role": "user", "content": "hello"}]},
    )

    assert response.status_code == 503
    assert response.json() == {
        "status": "error",
        "error": "auth_not_configured",
        "request_id": "api-1",
    }


def test_ingest_route_requires_idempotency_key() -> None:
    client = TestClient(create_app(auth_provider=DevHeaderAuthProvider(environment="test")))

    response = client.post(
        "/v1/users/user-1/sessions/session-1:ingest",
        headers={TENANT_ID_HEADER: "tenant-1", ACTOR_ID_HEADER: "user-1", "x-request-id": "api-2"},
        json={"turns": [{"role": "user", "content": "hello"}]},
    )

    assert response.status_code == 400
    assert response.json() == {
        "status": "error",
        "error": "idempotency_key_required",
        "request_id": "api-2",
    }


def test_ingest_route_rejects_unsafe_idempotency_key() -> None:
    client = TestClient(create_app(auth_provider=DevHeaderAuthProvider(environment="test")))

    response = client.post(
        "/v1/users/user-1/sessions/session-1:ingest",
        headers={
            TENANT_ID_HEADER: "tenant-1",
            ACTOR_ID_HEADER: "user-1",
            "x-request-id": "api-2b",
            "Idempotency-Key": "bad value",
        },
        json={"turns": [{"role": "user", "content": "hello"}]},
    )

    assert response.status_code == 400
    assert response.json()["error"] == "idempotency_key_required"


def test_query_route_rejects_user_mismatch_without_admin_role() -> None:
    client = TestClient(create_app(auth_provider=DevHeaderAuthProvider(environment="test")))

    response = client.post(
        "/v1/users/other-user/query",
        headers={TENANT_ID_HEADER: "tenant-1", ACTOR_ID_HEADER: "user-1", "x-request-id": "api-3"},
        json={"question": "Where is Bob?"},
    )

    assert response.status_code == 403
    assert response.json() == {
        "status": "error",
        "error": "forbidden",
        "request_id": "api-3",
    }


def test_query_route_returns_service_not_configured_when_auth_succeeds() -> None:
    client = TestClient(create_app(auth_provider=DevHeaderAuthProvider(environment="test")))

    response = client.post(
        "/v1/users/user-1/query",
        headers={TENANT_ID_HEADER: "tenant-1", ACTOR_ID_HEADER: "user-1", "x-request-id": "api-4"},
        json={"question": "Where is Bob?"},
    )

    assert response.status_code == 503
    assert response.json() == {
        "status": "error",
        "error": "service_not_configured",
        "request_id": "api-4",
    }


def test_chat_route_returns_service_not_configured_when_auth_succeeds() -> None:
    client = TestClient(create_app(auth_provider=DevHeaderAuthProvider(environment="test")))

    response = client.post(
        "/v1/users/user-1/chat",
        headers={TENANT_ID_HEADER: "tenant-1", ACTOR_ID_HEADER: "user-1", "x-request-id": "api-chat-1"},
        json={"message": "What do you remember?"},
    )

    assert response.status_code == 503
    assert response.json() == {
        "status": "error",
        "error": "service_not_configured",
        "request_id": "api-chat-1",
    }


def test_memory_routes_call_injected_services_with_actor_context() -> None:
    class FakeCommandService:
        async def submit_ingest_session(self, actor, *, user_id, session_id, request, idempotency_key):
            self.actor = actor
            self.idempotency_key = idempotency_key
            return {
                "job_id": "job-1",
                "status": "accepted",
                "tenant_id": actor.tenant.tenant_id,
                "user_id": user_id,
                "session_id": session_id,
            }

        async def submit_delete_session(self, actor, *, user_id, session_id, idempotency_key):
            return {
                "job_id": "job-delete",
                "status": "accepted",
                "tenant_id": actor.tenant.tenant_id,
                "user_id": user_id,
                "session_id": session_id,
            }

    class FakeQueryService:
        async def query(self, actor, *, user_id, request):
            return {
                "query_id": "query-1",
                "status": "succeeded",
                "facts": [
                    {
                        "fact_id": "fact-1",
                        "text": "Bob moved to Davis.",
                    }
                ],
            }

    class FakeChatService:
        async def chat(self, actor, *, user_id, request):
            self.actor = actor
            self.message = request.message
            return {
                "reply": "I remember Bob moved to Davis.",
                "query_id": "query-chat-1",
                "memory_status": "grounded",
                "facts": [{"fact_id": "fact-1", "text": "Bob moved to Davis."}],
            }

    class FakeJobService:
        async def get_job(self, actor, *, user_id, job_id):
            return {
                "job_id": job_id,
                "status": "running",
                "tenant_id": actor.tenant.tenant_id,
                "user_id": user_id,
            }

    class FakeTraceService:
        async def get_trace(self, actor, *, user_id, query_id):
            return {
                "query_id": query_id,
                "tenant_id": actor.tenant.tenant_id,
                "user_id": user_id,
                "forest_version": "v1",
                "event_count": 1,
                "events": [{"event_type": "query_completed"}],
            }

    class FakeUiReadService:
        async def list_sessions(self, actor, *, user_id, limit=100):
            return [{"session_id": "session-1", "cell_ids": [], "metadata": {}}]

        async def get_session(self, actor, *, user_id, session_id):
            return {"session_id": session_id, "cell_ids": [], "metadata": {}}

        async def list_turns(self, actor, *, user_id, session_id, limit=100):
            return [{"turn_id": "t1", "session_id": session_id, "turn_index": 0, "role": "user", "content": "hello"}]

        async def latest_manifest(self, actor, *, user_id):
            return {"manifest_id": "m1", "forest_version": "v1", "generation": 1, "status": "committed", "created_at_unix": 1.0}

        async def list_tree_roots(self, actor, *, user_id, forest_version, limit=100):
            return {"forest_version": forest_version, "roots": [{"tree_id": "tree-1", "node_id": "root", "level": "L1", "summary": "root"}]}

        async def get_tree_node(self, actor, *, user_id, forest_version, tree_id, node_id):
            return {"tree_id": tree_id, "node_id": node_id, "level": "L1", "summary": "root"}

        async def get_subtree(self, actor, *, user_id, forest_version, tree_id, root_node_id, max_depth=2, max_nodes=250):
            return {"forest_version": forest_version, "tree_id": tree_id, "root_node_id": root_node_id, "nodes": [{"tree_id": tree_id, "node_id": root_node_id, "level": "L1", "summary": "root"}]}

        async def get_fact(self, actor, *, user_id, forest_version, fact_id):
            return {"fact_id": fact_id, "text": "fact", "locations": []}

    command_service = FakeCommandService()
    chat_service = FakeChatService()
    app = create_app(auth_provider=DevHeaderAuthProvider(environment="test"))
    app.state.memory_command_service = command_service
    app.state.memory_query_service = FakeQueryService()
    app.state.memory_chat_service = chat_service
    app.state.job_status_service = FakeJobService()
    app.state.query_trace_service = FakeTraceService()
    app.state.ui_read_service = FakeUiReadService()
    client = TestClient(app)
    headers = {
        TENANT_ID_HEADER: "tenant-1",
        ACTOR_ID_HEADER: "user-1",
        ROLES_HEADER: "memory.admin",
        "Idempotency-Key": "idem-1",
    }

    ingest_response = client.post(
        "/v1/users/other-user/sessions/session-1:ingest",
        headers=headers,
        json={"turns": [{"role": "user", "content": "hello"}]},
    )
    query_response = client.post(
        "/v1/users/other-user/query",
        headers=headers,
        json={"question": "Where is Bob?"},
    )
    chat_response = client.post(
        "/v1/users/other-user/chat",
        headers=headers,
        json={"message": "Where did Bob move?", "turns": [{"role": "user", "content": "Where did Bob move?"}]},
    )
    job_response = client.get(
        "/v1/users/other-user/jobs/job-1",
        headers=headers,
    )
    job_events_response = client.get(
        "/v1/users/other-user/jobs/job-1/events",
        headers=headers,
    )
    trace_response = client.get(
        "/v1/users/other-user/queries/query-1/trace",
        headers=headers,
    )
    sessions_response = client.get("/v1/users/other-user/sessions", headers=headers)
    turns_response = client.get("/v1/users/other-user/sessions/session-1/turns", headers=headers)
    manifest_response = client.get("/v1/users/other-user/forest/latest", headers=headers)
    roots_response = client.get("/v1/users/other-user/forest/v1/trees", headers=headers)
    node_response = client.get("/v1/users/other-user/forest/v1/trees/tree-1/nodes/root", headers=headers)
    subtree_response = client.get("/v1/users/other-user/forest/v1/trees/tree-1/subtree?root_node_id=root", headers=headers)
    fact_response = client.get("/v1/users/other-user/forest/v1/facts/fact-1", headers=headers)

    assert ingest_response.status_code == 202
    assert ingest_response.json()["tenant_id"] == "tenant-1"
    assert command_service.actor.actor_id == "user-1"
    assert command_service.idempotency_key == "idem-1"
    assert query_response.status_code == 200
    assert query_response.json()["facts"][0]["fact_id"] == "fact-1"
    assert chat_response.status_code == 200
    assert chat_response.json()["reply"] == "I remember Bob moved to Davis."
    assert chat_service.actor.actor_id == "user-1"
    assert chat_service.message == "Where did Bob move?"
    assert job_response.status_code == 200
    assert job_response.json()["job_id"] == "job-1"
    assert job_events_response.status_code == 200
    assert job_events_response.json()[0]["event_type"] == "accepted"
    assert trace_response.status_code == 200
    assert trace_response.json()["query_id"] == "query-1"
    assert sessions_response.status_code == 200
    assert sessions_response.json()[0]["session_id"] == "session-1"
    assert turns_response.status_code == 200
    assert manifest_response.status_code == 200
    assert roots_response.status_code == 200
    assert node_response.status_code == 200
    assert subtree_response.status_code == 200
    assert fact_response.status_code == 200


def test_ingest_route_preserves_session_id_from_recording_service() -> None:
    from src.cloud.cosmos_jobs import CosmosJobStore
    from src.service.jobs import JobRecordingCommandService

    class FakeContainer:
        def __init__(self):
            self.items = {}

        def create_item(self, item, *, partition_key):
            self.items[(partition_key, item["id"])] = item

        def read_item(self, *, item, partition_key):
            return self.items[(partition_key, item)]

    class InnerCommandService:
        async def submit_ingest_session(self, actor, *, user_id, session_id, request, idempotency_key):
            return {
                "job_id": "unused",
                "status": "accepted",
                "tenant_id": actor.tenant.tenant_id,
                "user_id": user_id,
                "session_id": session_id,
            }

        async def submit_delete_session(self, actor, *, user_id, session_id, idempotency_key):
            raise AssertionError("not used")

    app = create_app(auth_provider=DevHeaderAuthProvider(environment="test"))
    app.state.memory_command_service = JobRecordingCommandService(
        InnerCommandService(),
        CosmosJobStore(container=FakeContainer()),
    )
    client = TestClient(app)

    response = client.post(
        "/v1/users/user-1/sessions/session-abc:ingest",
        headers={
            TENANT_ID_HEADER: "tenant-1",
            ACTOR_ID_HEADER: "user-1",
            "Idempotency-Key": "idem-route",
        },
        json={"turns": [{"role": "user", "content": "hello"}]},
    )

    assert response.status_code == 202
    assert response.json()["session_id"] == "session-abc"


def test_job_status_route_returns_404_for_missing_job() -> None:
    class MissingJobService:
        async def get_job(self, actor, *, user_id, job_id):
            raise JobNotFound()

    app = create_app(auth_provider=DevHeaderAuthProvider(environment="test"))
    app.state.job_status_service = MissingJobService()
    client = TestClient(app)

    response = client.get(
        "/v1/users/user-1/jobs/missing",
        headers={
            TENANT_ID_HEADER: "tenant-1",
            ACTOR_ID_HEADER: "user-1",
            "x-request-id": "job-404",
        },
    )

    assert response.status_code == 404
    assert response.json() == {
        "status": "error",
        "error": "job_not_found",
        "request_id": "job-404",
    }
