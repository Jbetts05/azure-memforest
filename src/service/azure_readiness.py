"""Azure dependency readiness checks."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Mapping

from src.service.readiness import ReadinessCheck, ReadinessCheckResult, StaticReadinessCheck


def build_azure_readiness_checks(environ: Mapping[str, str] | None = None) -> list[ReadinessCheck]:
    values = os.environ if environ is None else environ
    if not _has_any_azure_readiness_config(values):
        return []
    missing_base = _missing(values, ["AZURE_CLIENT_ID"])
    checks: list[ReadinessCheck] = []

    cosmos_missing = missing_base + _missing(values, ["AZURE_COSMOS_ENDPOINT", "AZURE_COSMOS_DATABASE_NAME"])
    if cosmos_missing:
        checks.append(_missing_check("cosmos", cosmos_missing))
    else:
        checks.append(CosmosReadinessCheck(
            endpoint=values["AZURE_COSMOS_ENDPOINT"],
            database_name=values["AZURE_COSMOS_DATABASE_NAME"],
            client_id=values["AZURE_CLIENT_ID"],
        ))

    search_missing = missing_base + _missing(values, ["AZURE_SEARCH_ENDPOINT"])
    if search_missing:
        checks.append(_missing_check("search", search_missing))
    else:
        checks.append(SearchReadinessCheck(
            endpoint=values["AZURE_SEARCH_ENDPOINT"],
            client_id=values["AZURE_CLIENT_ID"],
        ))

    storage_missing = missing_base + _missing(values, ["AZURE_STORAGE_BLOB_ENDPOINT", "AZURE_STORAGE_SNAPSHOT_CONTAINER"])
    if storage_missing:
        checks.append(_missing_check("storage", storage_missing))
    else:
        checks.append(StorageReadinessCheck(
            blob_endpoint=values["AZURE_STORAGE_BLOB_ENDPOINT"],
            container_name=values["AZURE_STORAGE_SNAPSHOT_CONTAINER"],
            client_id=values["AZURE_CLIENT_ID"],
        ))

    servicebus_missing = _missing(values, ["AZURE_SERVICEBUS_FULLY_QUALIFIED_NAMESPACE", "AZURE_SERVICEBUS_WRITES_QUEUE_NAME"])
    if servicebus_missing:
        checks.append(_missing_check("servicebus", servicebus_missing))
    else:
        checks.append(StaticReadinessCheck(name="servicebus", status="ok", detail="configured"))

    return checks


@dataclass(frozen=True)
class CosmosReadinessCheck:
    endpoint: str
    database_name: str
    client_id: str
    name: str = "cosmos"

    async def check(self) -> ReadinessCheckResult:
        def _read_database() -> None:
            _cosmos_client(self.endpoint, self.client_id).get_database_client(self.database_name).read()

        await asyncio.to_thread(_read_database)
        return ReadinessCheckResult(name=self.name, status="ok")


@dataclass(frozen=True)
class SearchReadinessCheck:
    endpoint: str
    client_id: str
    name: str = "search"

    async def check(self) -> ReadinessCheckResult:
        def _list_indexes() -> None:
            next(iter(_search_index_client(self.endpoint, self.client_id).list_index_names()), None)

        await asyncio.to_thread(_list_indexes)
        return ReadinessCheckResult(name=self.name, status="ok")


@dataclass(frozen=True)
class StorageReadinessCheck:
    blob_endpoint: str
    container_name: str
    client_id: str
    name: str = "storage"

    async def check(self) -> ReadinessCheckResult:
        def _read_container() -> None:
            _blob_service_client(self.blob_endpoint, self.client_id).get_container_client(self.container_name).get_container_properties()

        await asyncio.to_thread(_read_container)
        return ReadinessCheckResult(name=self.name, status="ok")


def _missing(values: Mapping[str, str], names: list[str]) -> list[str]:
    return [name for name in names if not str(values.get(name, "")).strip()]


@lru_cache(maxsize=8)
def _managed_identity_credential(client_id: str):
    from azure.identity import ManagedIdentityCredential

    return ManagedIdentityCredential(client_id=client_id)


@lru_cache(maxsize=8)
def _cosmos_client(endpoint: str, client_id: str):
    from azure.cosmos import CosmosClient

    return CosmosClient(endpoint, credential=_managed_identity_credential(client_id))


@lru_cache(maxsize=8)
def _search_index_client(endpoint: str, client_id: str):
    from azure.search.documents.indexes import SearchIndexClient

    return SearchIndexClient(endpoint=endpoint, credential=_managed_identity_credential(client_id))


@lru_cache(maxsize=8)
def _blob_service_client(blob_endpoint: str, client_id: str):
    from azure.storage.blob import BlobServiceClient

    return BlobServiceClient(account_url=blob_endpoint, credential=_managed_identity_credential(client_id))


def _has_any_azure_readiness_config(values: Mapping[str, str]) -> bool:
    prefixes = (
        "AZURE_COSMOS_",
        "AZURE_SEARCH_",
        "AZURE_STORAGE_",
        "AZURE_SERVICEBUS_",
    )
    return bool(str(values.get("AZURE_CLIENT_ID", "")).strip()) or any(
        str(values.get(name, "")).strip()
        for name in values
        if name.startswith(prefixes)
    )


def _missing_check(name: str, missing: list[str]) -> StaticReadinessCheck:
    return StaticReadinessCheck(
        name=name,
        status="not_configured",
        detail=f"missing: {','.join(missing)}",
    )
