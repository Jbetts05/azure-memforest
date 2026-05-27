"""FastAPI app factory for Azure-MemForest."""

from __future__ import annotations

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from src import __version__


SERVICE_NAME = "azure-memforest-api"


def create_app() -> FastAPI:
    """Create the HTTP API without loading model clients or user state."""

    app = FastAPI(
        title="Azure-MemForest API",
        version=__version__,
    )

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    def ready() -> JSONResponse:
        checks = {
            "configuration": "not_configured",
            "azure": "not_configured",
        }
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "not_ready", "checks": checks},
        )

    @app.get("/version")
    def version() -> dict[str, str]:
        return {
            "name": SERVICE_NAME,
            "version": __version__,
        }

    return app
