from __future__ import annotations

import subprocess
import sys

from fastapi.testclient import TestClient

from src import __version__
from src.service.app import create_app


def test_service_app_import_is_side_effect_light() -> None:
    code = """
import sys
import src.service.app as app
forbidden = [
    'src.forest.memforest',
    'src.api.client',
    'src.extraction.fact_manager',
    'faiss',
    'openai',
]
loaded = [name for name in forbidden if name in sys.modules]
assert hasattr(app, 'create_app')
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
            "configuration": "not_configured",
            "azure": "not_configured",
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
