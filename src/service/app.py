"""FastAPI app factory for Azure-MemForest."""

from __future__ import annotations

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from src import __version__
from src.service.readiness import ReadinessCheck, default_not_configured_checks, evaluate_readiness
from src.service.settings import ServiceSettings, load_service_settings


SERVICE_NAME = "azure-memforest-api"


def create_app(
    *,
    settings: ServiceSettings | None = None,
    readiness_checks: list[ReadinessCheck] | None = None,
) -> FastAPI:
    """Create the HTTP API without loading model clients or user state."""

    settings = settings or load_service_settings()
    configured_readiness_checks = readiness_checks or default_not_configured_checks()

    app = FastAPI(
        title="Azure-MemForest API",
        version=__version__,
    )

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready() -> JSONResponse:
        overall_status, checks = await evaluate_readiness(
            configured_readiness_checks,
            timeout_seconds=settings.readiness_timeout_seconds,
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK if overall_status == "ok" else status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": overall_status, "checks": checks},
        )

    @app.get("/version")
    def version() -> dict[str, str]:
        return {
            "name": SERVICE_NAME,
            "version": __version__,
        }

    return app
