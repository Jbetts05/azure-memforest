"""Cosmos-backed tenant/user write leases."""

from __future__ import annotations

import hashlib
import time
from typing import Any

from src.cloud.contracts import Lease
from src.cloud.schema import SCHEMA_VERSION, partition_key


class LeaseConflict(Exception):
    pass


class CosmosUserLeaseStore:
    def __init__(self, container: Any, *, tenant_id: str, user_id: str) -> None:
        self.container = container
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.tenant_user_key = partition_key(tenant_id=tenant_id, user_id=user_id)
        self.resource_id = self.tenant_user_key
        self.lease_id = _lease_id(tenant_id, user_id)

    async def acquire(self, *, owner_id: str, ttl_seconds: int) -> Lease:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive.")
        now = time.time()
        item = await self._read_raw()
        if item is None:
            document = _lease_document(
                lease_id=self.lease_id,
                tenant_id=self.tenant_id,
                user_id=self.user_id,
                tenant_user_key=self.tenant_user_key,
                resource_id=self.resource_id,
                owner_id=owner_id,
                fencing_token=1,
                now=now,
                ttl_seconds=ttl_seconds,
            )
            try:
                await _maybe_await(_create_item(self.container, document, partition_key=self.tenant_user_key))
                return _lease_from_doc(document)
            except Exception as exc:
                if not _is_conflict(exc):
                    raise
                item = await self._read_raw()
                if item is None:
                    raise LeaseConflict("lease create conflicted but no lease document exists") from exc

        return await self._replace_for_acquire(item, owner_id=owner_id, ttl_seconds=ttl_seconds, now=now)

    async def renew(self, lease: Lease, *, ttl_seconds: int) -> Lease:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive.")
        item = await self._required_current_item(lease)
        item["expires_at_unix"] = time.time() + ttl_seconds
        item["updated_at_unix"] = time.time()
        await _conditional_replace(self.container, item, partition_key=self.tenant_user_key, etag=item.get("_etag"))
        return _lease_from_doc(item)

    async def release(self, lease: Lease) -> None:
        item = await self._read_raw()
        if item is None:
            return
        self._assert_matches(item, lease)
        item["owner_id"] = ""
        item["expires_at_unix"] = 0.0
        item["released_at_unix"] = time.time()
        item["updated_at_unix"] = time.time()
        await _conditional_replace(self.container, item, partition_key=self.tenant_user_key, etag=item.get("_etag"))

    async def assert_current(self, lease: Lease) -> None:
        item = await self._required_current_item(lease)
        if float(item.get("expires_at_unix") or 0.0) <= time.time():
            raise LeaseConflict("lease is expired")

    async def _replace_for_acquire(self, item: dict[str, Any], *, owner_id: str, ttl_seconds: int, now: float) -> Lease:
        current_owner = str(item.get("owner_id") or "")
        expires_at = float(item.get("expires_at_unix") or 0.0)
        current_token = int(item.get("fencing_token") or 0)
        if current_owner and current_owner != owner_id and expires_at > now:
            raise LeaseConflict("lease is held by another owner")
        next_token = current_token
        if current_owner != owner_id or expires_at <= now:
            next_token = current_token + 1
        item["owner_id"] = owner_id
        item["fencing_token"] = next_token
        item["expires_at_unix"] = now + ttl_seconds
        item["updated_at_unix"] = now
        item["released_at_unix"] = None
        try:
            await _conditional_replace(self.container, item, partition_key=self.tenant_user_key, etag=item.get("_etag"))
        except Exception as exc:
            if _is_precondition_failed(exc):
                raise LeaseConflict("lease changed before acquire") from exc
            raise
        return _lease_from_doc(item)

    async def _required_current_item(self, lease: Lease) -> dict[str, Any]:
        item = await self._read_raw()
        if item is None:
            raise LeaseConflict("lease not found")
        self._assert_matches(item, lease)
        return item

    async def _read_raw(self) -> dict[str, Any] | None:
        try:
            item = await _maybe_await(self.container.read_item(item=self.lease_id, partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id:
            return None
        return item

    def _assert_matches(self, item: dict[str, Any], lease: Lease) -> None:
        if (
            item.get("owner_id") != lease.owner_id
            or str(item.get("fencing_token")) != str(lease.fencing_token)
            or item.get("resource_id") != lease.resource_id
        ):
            raise LeaseConflict("lease owner or fencing token does not match")


def _lease_document(
    *,
    lease_id: str,
    tenant_id: str,
    user_id: str,
    tenant_user_key: str,
    resource_id: str,
    owner_id: str,
    fencing_token: int,
    now: float,
    ttl_seconds: int,
) -> dict[str, Any]:
    return {
        "id": lease_id,
        "schema_version": SCHEMA_VERSION,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "tenant_user_key": tenant_user_key,
        "resource_id": resource_id,
        "owner_id": owner_id,
        "fencing_token": fencing_token,
        "acquired_at_unix": now,
        "expires_at_unix": now + ttl_seconds,
        "updated_at_unix": now,
        "released_at_unix": None,
    }


def _lease_from_doc(item: dict[str, Any]) -> Lease:
    return Lease(
        lease_id=item["id"],
        owner_id=str(item.get("owner_id") or ""),
        resource_id=str(item.get("resource_id") or ""),
        fencing_token=str(item.get("fencing_token") or ""),
        expires_at_unix=float(item.get("expires_at_unix") or 0.0),
    )


def _lease_id(tenant_id: str, user_id: str) -> str:
    digest = hashlib.sha256(f"{tenant_id}\x1f{user_id}".encode("utf-8")).hexdigest()
    return f"lease_{digest}"


async def _conditional_replace(container: Any, item: dict[str, Any], *, partition_key: str, etag: str | None):
    if hasattr(container, "replace_item"):
        from azure.core import MatchConditions

        return await _maybe_await(
            container.replace_item(
                item=item["id"],
                body=item,
                partition_key=partition_key,
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        )
    return await _maybe_await(container.upsert_item(item))


async def _maybe_await(value: Any):
    if hasattr(value, "__await__"):
        return await value
    return value


def _create_item(container: Any, document: dict[str, Any], *, partition_key: str):
    try:
        return container.create_item(document)
    except TypeError:
        return container.create_item(document, partition_key=partition_key)


def _is_conflict(exc: Exception) -> bool:
    return exc.__class__.__name__ == "CosmosResourceExistsError" or getattr(exc, "status_code", None) == 409


def _is_not_found(exc: Exception) -> bool:
    return exc.__class__.__name__ == "CosmosResourceNotFoundError" or getattr(exc, "status_code", None) == 404


def _is_precondition_failed(exc: Exception) -> bool:
    return getattr(exc, "status_code", None) == 412 or exc.__class__.__name__ == "CosmosAccessConditionFailedError"
