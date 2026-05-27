"""FastAPI app factory for Azure-MemForest."""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from src import __version__
from src.service.auth import AuthError, AuthProvider
from src.service.azure_readiness import build_azure_readiness_checks
from src.service.middleware import REQUEST_ID_HEADER, RequestIdMiddleware
from src.service.readiness import ReadinessCheck, default_not_configured_checks, evaluate_readiness
from src.service.routes import _RouteError, route_error_handler, router as memory_router
from src.service.settings import ServiceSettings, load_service_settings


SERVICE_NAME = "azure-memforest-api"
log = logging.getLogger(__name__)


def create_app(
    *,
    settings: ServiceSettings | None = None,
    readiness_checks: list[ReadinessCheck] | None = None,
    auth_provider: AuthProvider | None = None,
) -> FastAPI:
    """Create the HTTP API without loading model clients or user state."""

    settings = settings or load_service_settings()
    configured_readiness_checks = readiness_checks or _default_readiness_checks()

    app = FastAPI(
        title="Azure-MemForest API",
        version=__version__,
    )
    app.add_middleware(RequestIdMiddleware)
    app.state.auth_provider = auth_provider
    app.include_router(memory_router)
    app.add_exception_handler(_RouteError, route_error_handler)

    @app.exception_handler(AuthError)
    async def auth_exception(request: Request, exc: AuthError) -> JSONResponse:
        request_id = getattr(request.state, "request_id", "")
        return JSONResponse(
            status_code=exc.status_code,
            headers={REQUEST_ID_HEADER: request_id} if request_id else None,
            content={
                "status": "error",
                "error": exc.error_code,
                "request_id": request_id,
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_exception(request: Request, exc: Exception) -> JSONResponse:
        request_id = getattr(request.state, "request_id", "")
        trace_id = getattr(request.state, "trace_id", "")
        log.error(
            "Unhandled service exception request_id=%s",
            request_id,
            extra={"request_id": request_id, "trace_id": trace_id},
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            headers={REQUEST_ID_HEADER: request_id} if request_id else None,
            content={
                "status": "error",
                "error": "internal_server_error",
                "request_id": request_id,
            },
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


def _default_readiness_checks() -> list[ReadinessCheck]:
    checks = build_azure_readiness_checks()
    return checks if checks else default_not_configured_checks()
