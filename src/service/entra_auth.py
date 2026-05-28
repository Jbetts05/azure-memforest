"""Microsoft Entra ID JWT auth provider."""

from __future__ import annotations

import time
from dataclasses import dataclass
from threading import RLock
from typing import Any, Mapping

import httpx
import jwt
from fastapi import Request
from jwt import PyJWKClient

from src.service.auth import ActorContext, AuthProvider, TenantContext, Unauthorized


@dataclass(frozen=True, slots=True)
class EntraJwtAuthConfig:
    tenant_id: str
    audiences: tuple[str, ...]
    metadata_url: str | None = None
    require_version: str = "2.0"
    leeway_seconds: int = 60


class EntraJwtAuthProvider(AuthProvider):
    def __init__(
        self,
        config: EntraJwtAuthConfig,
        *,
        metadata: Mapping[str, Any] | None = None,
        jwk_client: PyJWKClient | None = None,
    ) -> None:
        if not config.tenant_id or not config.audiences:
            raise ValueError("tenant_id and audiences are required for Entra JWT auth.")
        self.config = config
        self._metadata = dict(metadata) if metadata is not None else None
        self._jwk_client = jwk_client
        self._lock = RLock()

    async def authenticate(self, request: Request) -> ActorContext:
        token = _bearer_token(request)
        if not token:
            raise Unauthorized()
        claims = await self.validate_token(token)
        actor_id = str(claims.get("oid") or claims.get("sub") or "")
        if not actor_id:
            raise Unauthorized()
        scopes = tuple(str(claims.get("scp", "")).split()) if claims.get("scp") else ()
        roles_claim = claims.get("roles") or []
        roles = tuple(str(role) for role in roles_claim) if isinstance(roles_claim, list) else ()
        return ActorContext(
            actor_id=actor_id,
            tenant=TenantContext(tenant_id=self.config.tenant_id),
            scopes=scopes,
            roles=roles,
            auth_scheme="entra-jwt",
        )

    async def validate_token(self, token: str) -> dict[str, Any]:
        try:
            metadata = await self._metadata_document()
            signing_key = self._jwk_client_for(metadata).get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                audience=list(self.config.audiences),
                issuer=str(metadata["issuer"]),
                leeway=self.config.leeway_seconds,
                options={"require": ["exp", "nbf", "iat", "aud", "iss"]},
            )
        except Exception as exc:
            raise Unauthorized() from exc
        if str(claims.get("tid", "")) != self.config.tenant_id:
            raise Unauthorized()
        if str(claims.get("ver", "")) != self.config.require_version:
            raise Unauthorized()
        return claims

    async def _metadata_document(self) -> dict[str, Any]:
        if self._metadata is not None:
            return dict(self._metadata)
        metadata_url = self.config.metadata_url or (
            f"https://login.microsoftonline.com/{self.config.tenant_id}/v2.0/.well-known/openid-configuration"
        )
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(metadata_url)
            response.raise_for_status()
            metadata = response.json()
        with self._lock:
            self._metadata = dict(metadata)
        return dict(metadata)

    def _jwk_client_for(self, metadata: Mapping[str, Any]) -> PyJWKClient:
        with self._lock:
            if self._jwk_client is None:
                self._jwk_client = PyJWKClient(str(metadata["jwks_uri"]))
            return self._jwk_client


def entra_auth_provider_from_env(environ: Mapping[str, str]) -> EntraJwtAuthProvider | None:
    tenant_id = str(environ.get("ENTRA_TENANT_ID", "")).strip()
    audiences = tuple(
        item.strip()
        for item in str(environ.get("ENTRA_AUDIENCES", "")).split(",")
        if item.strip()
    )
    if not tenant_id or not audiences:
        return None
    return EntraJwtAuthProvider(
        EntraJwtAuthConfig(
            tenant_id=tenant_id,
            audiences=audiences,
        )
    )


def _bearer_token(request: Request) -> str:
    authorization = request.headers.get("authorization", "")
    if not authorization.lower().startswith("bearer "):
        return ""
    return authorization[7:].strip()
