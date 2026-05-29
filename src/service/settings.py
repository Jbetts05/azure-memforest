"""Service-level settings for the Azure-MemForest API."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class ServiceSettings:
    environment: str = "local"
    readiness_timeout_seconds: float = 10.0
    ui_enabled: bool = False
    ui_allowed_origins: tuple[str, ...] = ()


def load_service_settings(environ: Mapping[str, str] | None = None) -> ServiceSettings:
    """Load safe, non-secret service settings from environment variables."""

    values = os.environ if environ is None else environ
    return ServiceSettings(
        environment=str(values.get("AZURE_MEMFOREST_ENV", "local") or "local"),
        readiness_timeout_seconds=_positive_float(
            values.get("AZURE_MEMFOREST_READINESS_TIMEOUT_SECONDS"),
            default=10.0,
        ),
        ui_enabled=_truthy(values.get("AZURE_MEMFOREST_UI_ENABLED")),
        ui_allowed_origins=_csv(values.get("AZURE_MEMFOREST_UI_ALLOWED_ORIGINS")),
    )


def _positive_float(value: str | None, *, default: float) -> float:
    if value is None or value == "":
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _csv(value: str | None) -> tuple[str, ...]:
    return tuple(part.strip() for part in str(value or "").split(",") if part.strip())
