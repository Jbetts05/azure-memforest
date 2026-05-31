"""Authentication context contracts for Azure-MemForest service routes."""

from __future__ import annotations

import re
import hmac
from dataclasses import dataclass, field
from typing import Protocol

from fastapi import Request

TENANT_ID_HEADER = "x-tenant-id"
ACTOR_ID_HEADER = "x-actor-id"
SCOPES_HEADER = "x-auth-scopes"
ROLES_HEADER = "x-auth-roles"
BENCHMARK_AUTH_HEADER = "x-benchmark-auth"

_SAFE_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]{1,128}$")
_SAFE_PARTITION_ID_PATTERN = re.compile(r"^[A-Za-z0-9._@-]{1,128}$")
_DEV_ENVIRONMENTS = {"local", "test", "dev"}


@dataclass(frozen=True, slots=True)
class TenantContext:
    tenant_id: str


@dataclass(frozen=True, slots=True)
class ActorContext:
    actor_id: str
    tenant: TenantContext
    scopes: tuple[str, ...] = field(default_factory=tuple)
    roles: tuple[str, ...] = field(default_factory=tuple)
    auth_scheme: str = "unknown"


class AuthProvider(Protocol):
    async def authenticate(self, request: Request) -> ActorContext:
        ...


class AuthError(Exception):
    error_code = "unauthorized"
    status_code = 401

    def __init__(self, message: str = "") -> None:
        self.message = message or self.error_code
        super().__init__(self.message)


class Unauthorized(AuthError):
    error_code = "unauthorized"
    status_code = 401


class Forbidden(AuthError):
    error_code = "forbidden"
    status_code = 403


class AuthNotConfigured(AuthError):
    error_code = "auth_not_configured"
    status_code = 503


class DevHeaderAuthProvider:
    """Local/test-only auth provider that trusts explicit dev headers."""

    def __init__(self, *, environment: str) -> None:
        normalized = str(environment or "").lower()
        if normalized not in _DEV_ENVIRONMENTS:
            raise ValueError("DevHeaderAuthProvider is only allowed in local, test, or dev environments.")
        self.environment = normalized

    async def authenticate(self, request: Request) -> ActorContext:
        tenant_id = _require_safe_header(request, TENANT_ID_HEADER)
        actor_id = _require_safe_header(request, ACTOR_ID_HEADER)
        return ActorContext(
            actor_id=actor_id,
            tenant=TenantContext(tenant_id=tenant_id),
            scopes=_parse_csv_header(request, SCOPES_HEADER),
            roles=_parse_csv_header(request, ROLES_HEADER),
            auth_scheme="dev-header",
        )


class BenchmarkHeaderAuthProvider:
    """Temporary benchmark-only auth override guarded by a shared secret header."""

    def __init__(self, *, secret: str, environment: str) -> None:
        normalized = str(environment or "").lower()
        if normalized not in _DEV_ENVIRONMENTS:
            raise ValueError("BenchmarkHeaderAuthProvider is only allowed in local, test, or dev environments.")
        if len(str(secret or "")) < 32:
            raise ValueError("benchmark auth secret must be at least 32 characters.")
        self.secret = str(secret)
        self.environment = normalized

    async def authenticate(self, request: Request) -> ActorContext:
        if not hmac.compare_digest(request.headers.get(BENCHMARK_AUTH_HEADER, ""), self.secret):
            raise Unauthorized()
        tenant_id = _require_safe_header(request, TENANT_ID_HEADER)
        actor_id = _require_safe_header(request, ACTOR_ID_HEADER)
        return ActorContext(
            actor_id=actor_id,
            tenant=TenantContext(tenant_id),
            scopes=_parse_csv_header(request, SCOPES_HEADER),
            roles=tuple(sorted(set(_parse_csv_header(request, ROLES_HEADER) + ("memory.benchmark",)))),
            auth_scheme="benchmark-header",
        )


class BenchmarkAuthWrapper:
    """Use benchmark header auth when present; otherwise delegate to normal auth."""

    def __init__(self, *, benchmark: BenchmarkHeaderAuthProvider, delegate: AuthProvider) -> None:
        self.benchmark = benchmark
        self.delegate = delegate

    async def authenticate(self, request: Request) -> ActorContext:
        if request.headers.get(BENCHMARK_AUTH_HEADER):
            return await self.benchmark.authenticate(request)
        return await self.delegate.authenticate(request)


async def get_actor_context(request: Request) -> ActorContext:
    provider = getattr(request.app.state, "auth_provider", None)
    if provider is None:
        raise AuthNotConfigured()
    return await provider.authenticate(request)


def _require_safe_header(request: Request, header_name: str) -> str:
    value = request.headers.get(header_name, "")
    if header_name in (TENANT_ID_HEADER, ACTOR_ID_HEADER):
        safe = _is_safe_partition_id(value)
    else:
        safe = _is_safe_id(value)
    if not safe:
        raise Unauthorized()
    return value


def _parse_csv_header(request: Request, header_name: str) -> tuple[str, ...]:
    raw = request.headers.get(header_name, "")
    values = []
    for item in raw.split(","):
        cleaned = item.strip()
        if cleaned and _is_safe_id(cleaned):
            values.append(cleaned)
    return tuple(values)


def _is_safe_id(value: str) -> bool:
    return bool(_SAFE_ID_PATTERN.fullmatch(str(value or "").strip()))


def _is_safe_partition_id(value: str) -> bool:
    return bool(_SAFE_PARTITION_ID_PATTERN.fullmatch(str(value or "").strip()))
