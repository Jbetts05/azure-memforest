from __future__ import annotations

import subprocess
import sys

from fastapi.testclient import TestClient

from src import __version__
from src.service.app import create_app
from src.service.readiness import ReadinessCheckResult, StaticReadinessCheck
from src.service.settings import ServiceSettings, load_service_settings


def test_service_app_import_is_side_effect_light() -> None:
    code = """
import sys
import src.service.app as app
import src.service.readiness as readiness
import src.service.settings as settings
forbidden = [
    'src.forest.memforest',
    'src.api.client',
    'src.extraction.fact_manager',
    'faiss',
    'openai',
]
loaded = [name for name in forbidden if name in sys.modules]
assert hasattr(app, 'create_app')
assert hasattr(readiness, 'evaluate_readiness')
assert hasattr(settings, 'load_service_settings')
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
