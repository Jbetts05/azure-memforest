from __future__ import annotations

import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Request

from src.service.auth import Unauthorized
from src.service.entra_auth import (
    EntraJwtAuthConfig,
    EntraJwtAuthProvider,
    entra_auth_provider_from_env,
)


class FakeKey:
    def __init__(self, key):
        self.key = key


class FakeJwkClient:
    def __init__(self, key):
        self.key = key

    def get_signing_key_from_jwt(self, token):
        return FakeKey(self.key)


class FakeRequest:
    def __init__(self, token: str):
        self.headers = {"authorization": f"Bearer {token}"}


def _keys():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


def _token(private_key, *, tenant_id="tenant-1", audience="api://test", version="2.0", expires_delta=3600):
    now = int(time.time())
    return jwt.encode(
        {
            "iss": f"https://login.microsoftonline.com/{tenant_id}/v2.0",
            "aud": audience,
            "tid": tenant_id,
            "ver": version,
            "oid": "actor-1",
            "scp": "memory.read memory.write",
            "roles": ["memory.admin"],
            "iat": now,
            "nbf": now - 10,
            "exp": now + expires_delta,
        },
        private_key,
        algorithm="RS256",
        headers={"kid": "kid-1"},
    )


def _provider(public_key, *, tenant_id="tenant-1", audiences=("api://test",)):
    return EntraJwtAuthProvider(
        EntraJwtAuthConfig(tenant_id=tenant_id, audiences=audiences),
        metadata={
            "issuer": f"https://login.microsoftonline.com/{tenant_id}/v2.0",
            "jwks_uri": "https://example/jwks",
        },
        jwk_client=FakeJwkClient(public_key),
    )


@pytest.mark.asyncio
async def test_entra_provider_validates_token_and_returns_actor_context() -> None:
    private_key, public_key = _keys()
    provider = _provider(public_key)

    actor = await provider.authenticate(FakeRequest(_token(private_key)))

    assert actor.actor_id == "actor-1"
    assert actor.tenant.tenant_id == "tenant-1"
    assert actor.scopes == ("memory.read", "memory.write")
    assert actor.roles == ("memory.admin",)
    assert actor.auth_scheme == "entra-jwt"


@pytest.mark.asyncio
async def test_entra_provider_rejects_wrong_tenant_audience_and_version() -> None:
    private_key, public_key = _keys()
    provider = _provider(public_key)

    with pytest.raises(Unauthorized):
        await provider.validate_token(_token(private_key, tenant_id="other-tenant"))
    with pytest.raises(Unauthorized):
        await provider.validate_token(_token(private_key, audience="api://other"))
    with pytest.raises(Unauthorized):
        await provider.validate_token(_token(private_key, version="1.0"))


@pytest.mark.asyncio
async def test_entra_provider_rejects_expired_token_beyond_leeway() -> None:
    private_key, public_key = _keys()
    provider = _provider(public_key)

    with pytest.raises(Unauthorized):
        await provider.validate_token(_token(private_key, expires_delta=-120))


@pytest.mark.asyncio
async def test_entra_provider_rejects_missing_bearer_header() -> None:
    _, public_key = _keys()
    provider = _provider(public_key)

    class RequestWithoutAuth:
        headers = {}

    with pytest.raises(Unauthorized):
        await provider.authenticate(RequestWithoutAuth())


def test_entra_auth_provider_from_env_requires_tenant_and_audience() -> None:
    assert entra_auth_provider_from_env({}) is None
    assert entra_auth_provider_from_env(
        {
            "ENTRA_TENANT_ID": "tenant-1",
            "ENTRA_AUDIENCES": "api://test,api://other",
        }
    ) is not None
