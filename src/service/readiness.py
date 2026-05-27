"""Pluggable readiness checks for the Azure-MemForest service."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Literal, Protocol

ReadinessStatus = Literal["ok", "not_configured", "failed"]

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReadinessCheckResult:
    name: str
    status: ReadinessStatus
    detail: str = ""


class ReadinessCheck(Protocol):
    name: str

    async def check(self) -> ReadinessCheckResult:
        ...


@dataclass(frozen=True)
class StaticReadinessCheck:
    name: str
    status: ReadinessStatus = "ok"
    detail: str = ""

    async def check(self) -> ReadinessCheckResult:
        return ReadinessCheckResult(
            name=self.name,
            status=self.status,
            detail=self.detail,
        )


async def evaluate_readiness(
    checks: list[ReadinessCheck],
    *,
    timeout_seconds: float = 10.0,
) -> tuple[str, dict[str, dict[str, str]]]:
    """Evaluate readiness checks with timeout and sanitized failure details."""

    results = await asyncio.gather(
        *[_evaluate_one(check, timeout_seconds=timeout_seconds) for check in checks]
    )
    response_checks = {
        result.name: _result_to_response(result)
        for result in results
    }
    overall = "ok" if results and all(result.status == "ok" for result in results) else "not_ready"
    return overall, response_checks


async def _evaluate_one(check: ReadinessCheck, *, timeout_seconds: float) -> ReadinessCheckResult:
    try:
        return await asyncio.wait_for(
            check.check(),
            timeout=timeout_seconds,
        )
    except TimeoutError:
        return ReadinessCheckResult(
            name=getattr(check, "name", "unknown"),
            status="failed",
            detail="timeout",
        )
    except Exception:
        name = getattr(check, "name", "unknown")
        log.exception("Readiness check failed: %s", name)
        return ReadinessCheckResult(
            name=name,
            status="failed",
            detail="check raised",
        )


async def evaluate_readiness_sequential(
    checks: list[ReadinessCheck],
    *,
    timeout_seconds: float = 10.0,
) -> tuple[str, dict[str, dict[str, str]]]:
    """Sequential evaluator kept for tests/debugging when ordering matters."""

    results = []
    for check in checks:
        try:
            results.append(
                await asyncio.wait_for(
                    check.check(),
                    timeout=timeout_seconds,
                )
            )
        except TimeoutError:
            results.append(
                ReadinessCheckResult(
                    name=getattr(check, "name", "unknown"),
                    status="failed",
                    detail="timeout",
                )
            )
        except Exception:
            name = getattr(check, "name", "unknown")
            log.exception("Readiness check failed: %s", name)
            results.append(
                ReadinessCheckResult(
                    name=name,
                    status="failed",
                    detail="check raised",
                )
            )

    response_checks = {
        result.name: _result_to_response(result)
        for result in results
    }
    overall = "ok" if results and all(result.status == "ok" for result in results) else "not_ready"
    return overall, response_checks


def default_not_configured_checks() -> list[ReadinessCheck]:
    return [
        StaticReadinessCheck(name="configuration", status="not_configured"),
        StaticReadinessCheck(name="azure", status="not_configured"),
    ]


def _result_to_response(result: ReadinessCheckResult) -> dict[str, str]:
    payload = {"status": result.status}
    if result.detail:
        payload["detail"] = result.detail
    return payload
