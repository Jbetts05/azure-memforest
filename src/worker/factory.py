"""Factory helpers for scoped worker materialization handlers."""

from __future__ import annotations

import os
import hashlib
from pathlib import Path
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.cloud.azure_models import AzureOpenAIModelClient
from src.cloud.azure_search import AzureSearchRecallIndex
from src.cloud.blob_snapshots import blob_snapshot_store_from_env
from src.cloud.config import load_cloud_config
from src.cloud.cosmos_browse_embeddings import CosmosBrowseEmbeddingStore
from src.cloud.cosmos_facts import CosmosFactStore
from src.cloud.cosmos_jobs import CosmosJobStore
from src.cloud.cosmos_leases import CosmosUserLeaseStore
from src.cloud.cosmos_manifests import CosmosDeletionLog, CosmosManifestStore
from src.cloud.cosmos_sessions import CosmosSessionStore, CosmosTurnStore
from src.cloud.cosmos_source import CosmosFactLocationIndex, CosmosTreeNodeStore
from src.config.api import APISettings, ChatCompletionConfig, EmbeddingConfig
from src.config.config import load_default_config
from src.service.jobs import WriteJobEnvelope
from src.worker.consumer import ServiceBusWriteWorker, WriteJobHandler
from src.worker.materialization import (
    MaterializationHandlerConfig,
    MaterializationStores,
    MemForestMaterializationHandler,
    SyncIngestCore,
)
from src.worker.receiver import ServiceBusSessionReceiverLoop, WorkerReceiverConfig, create_azure_lock_renewer
from src.worker.snapshot_hydration import ArchiveHydratingUserForestCore


StoreFactory = Callable[[WriteJobEnvelope], MaterializationStores]
CoreFactory = Callable[[WriteJobEnvelope], SyncIngestCore]

REQUIRED_WORKER_ENV = (
    "AZURE_CLIENT_ID",
    "AZURE_COSMOS_ENDPOINT",
    "AZURE_COSMOS_DATABASE_NAME",
    "AZURE_SEARCH_ENDPOINT",
    "AZURE_SEARCH_RECALL_INDEX_NAME",
    "AZURE_OPENAI_ENDPOINT",
    "AZURE_OPENAI_API_VERSION",
    "AZURE_OPENAI_CHAT_DEPLOYMENT",
    "AZURE_OPENAI_EMBEDDING_DEPLOYMENT",
    "AZURE_OPENAI_EMBEDDING_DIMENSION",
    "AZURE_OPENAI_EMBEDDING_MODEL_VERSION",
    "AZURE_STORAGE_BLOB_ENDPOINT",
    "AZURE_STORAGE_SNAPSHOT_CONTAINER",
    "AZURE_SERVICEBUS_FULLY_QUALIFIED_NAMESPACE",
    "AZURE_SERVICEBUS_WRITES_QUEUE_NAME",
)


@dataclass(slots=True)
class WorkerRuntime:
    receiver_loop: ServiceBusSessionReceiverLoop
    closeables: list[Any]

    async def run_forever(self) -> None:
        await self.receiver_loop.run_forever()

    async def close(self) -> None:
        await self.receiver_loop.close()
        for closeable in self.closeables:
            close = getattr(closeable, "close", None)
            if close is not None:
                value = close()
                if hasattr(value, "__await__"):
                    await value


@dataclass(frozen=True, slots=True)
class ScopedMaterializationHandler(WriteJobHandler):
    """Build a tenant/user-scoped materialization handler per envelope."""

    store_factory: StoreFactory
    core_factory: CoreFactory
    config_factory: Callable[[WriteJobEnvelope], MaterializationHandlerConfig]

    async def handle(self, envelope: WriteJobEnvelope, claimed_job: dict[str, Any]) -> None:
        handler = MemForestMaterializationHandler(
            stores=self.store_factory(envelope),
            ingest_core=self.core_factory(envelope),
            config=self.config_factory(envelope),
        )
        await handler.handle(envelope, claimed_job)


def cosmos_materialization_store_factory(
    *,
    database_client: Any,
    recall_index: Any,
    embedding_model_version: str = "",
    snapshot_store: Any | None = None,
    browse_embedding_store: Any | None = None,
    summary_cache: Any | None = None,
) -> StoreFactory:
    """Create per-envelope Cosmos-backed MaterializationStores."""

    def build(envelope: WriteJobEnvelope) -> MaterializationStores:
        tenant_id = envelope.tenant_id
        user_id = envelope.user_id
        return MaterializationStores(
            lease_store=CosmosUserLeaseStore(
                database_client.get_container_client("leases"),
                tenant_id=tenant_id,
                user_id=user_id,
            ),
            source_session_store=CosmosSessionStore(
                database_client.get_container_client("sessions"),
                tenant_id=tenant_id,
                user_id=user_id,
            ),
            source_turn_store=CosmosTurnStore(
                database_client.get_container_client("turns"),
                tenant_id=tenant_id,
                user_id=user_id,
            ),
            fact_store=CosmosFactStore(
                database_client.get_container_client("facts"),
                tenant_id=tenant_id,
                user_id=user_id,
            ),
            manifest_store=CosmosManifestStore(
                database_client.get_container_client("manifests"),
                tenant_id=tenant_id,
                user_id=user_id,
            ),
            recall_index=recall_index,
            browse_embedding_store=browse_embedding_store,
            summary_cache=summary_cache,
            snapshot_store=snapshot_store,
            deletion_log=CosmosDeletionLog(database_client.get_container_client("deletions"), tenant_id=tenant_id),
            browse_embedding_store_factory=lambda forest_version: CosmosBrowseEmbeddingStore(
                database_client.get_container_client("browse_embeddings"),
                tenant_id=tenant_id,
                user_id=user_id,
                forest_version=forest_version,
                embedding_model_version=embedding_model_version,
            ) if browse_embedding_store is None else browse_embedding_store,
            tree_node_store_factory=lambda forest_version: CosmosTreeNodeStore(
                database_client.get_container_client("tree_nodes"),
                tenant_id=tenant_id,
                user_id=user_id,
                forest_version=forest_version,
            ),
            fact_location_index_factory=lambda forest_version: CosmosFactLocationIndex(
                database_client.get_container_client("fact_locations"),
                tenant_id=tenant_id,
                user_id=user_id,
                forest_version=forest_version,
            ),
        )

    return build


def fixed_materialization_config_factory(
    *,
    owner_id: str,
    embedding_model_version: str,
    lease_ttl_seconds: int = 300,
    lease_renewal_seconds: float = 60.0,
) -> Callable[[WriteJobEnvelope], MaterializationHandlerConfig]:
    def build(envelope: WriteJobEnvelope) -> MaterializationHandlerConfig:
        return MaterializationHandlerConfig(
            owner_id=owner_id,
            embedding_model_version=embedding_model_version,
            lease_ttl_seconds=lease_ttl_seconds,
            lease_renewal_seconds=lease_renewal_seconds,
        )

    return build


def build_worker_runtime_from_env(environ: dict[str, str] | None = None) -> WorkerRuntime:
    values = dict(os.environ if environ is None else environ)
    missing = missing_worker_env(values)
    if missing:
        raise RuntimeError(f"worker environment is missing required settings: {', '.join(missing)}")

    from azure.cosmos.aio import CosmosClient
    from azure.identity import ManagedIdentityCredential, get_bearer_token_provider
    from azure.identity.aio import ManagedIdentityCredential as AsyncManagedIdentityCredential
    from azure.servicebus.aio import ServiceBusClient

    client_id = values["AZURE_CLIENT_ID"]
    async_credential = AsyncManagedIdentityCredential(client_id=client_id)
    sync_credential = ManagedIdentityCredential(client_id=client_id)
    cosmos_client = CosmosClient(values["AZURE_COSMOS_ENDPOINT"], credential=async_credential)
    database = cosmos_client.get_database_client(values["AZURE_COSMOS_DATABASE_NAME"])
    cloud_config = load_cloud_config(values)
    if cloud_config.azure_openai is None:
        raise RuntimeError("Azure OpenAI configuration is required for the worker")
    token_provider = get_bearer_token_provider(sync_credential, "https://cognitiveservices.azure.com/.default")
    model_client = AzureOpenAIModelClient(cloud_config.azure_openai, azure_ad_token_provider=token_provider)
    memforest_config = _worker_memforest_config(values)

    recall_index = AzureSearchRecallIndex(
        endpoint=values["AZURE_SEARCH_ENDPOINT"],
        index_name=values["AZURE_SEARCH_RECALL_INDEX_NAME"],
        vector_dimensions=int(values["AZURE_OPENAI_EMBEDDING_DIMENSION"]),
        credential=sync_credential,
    )
    snapshot_store = blob_snapshot_store_from_env(values, credential=sync_credential)
    if snapshot_store is None:
        raise RuntimeError("snapshot store configuration is required for the worker")

    store_factory = cosmos_materialization_store_factory(
        database_client=database,
        recall_index=recall_index,
        embedding_model_version=values["AZURE_OPENAI_EMBEDDING_MODEL_VERSION"],
        snapshot_store=snapshot_store,
    )
    core_factory = hydrating_user_forest_core_factory(
        config=memforest_config,
        chat_client=model_client,
        embedding_client=model_client,
        snapshot_store=snapshot_store,
        scratch_root=Path(values.get("AZURE_MEMFOREST_WORKER_SCRATCH_ROOT", "/tmp/azure-memforest-worker")),
    )
    owner_id = values.get("AZURE_MEMFOREST_WORKER_OWNER_ID") or values.get("HOSTNAME") or "worker"
    handler = ScopedMaterializationHandler(
        store_factory=store_factory,
        core_factory=core_factory,
        config_factory=fixed_materialization_config_factory(
            owner_id=owner_id,
            embedding_model_version=values["AZURE_OPENAI_EMBEDDING_MODEL_VERSION"],
        ),
    )
    worker = ServiceBusWriteWorker(
        job_store=CosmosJobStore(database.get_container_client("jobs")),
        handler=handler,
        owner_id=owner_id,
    )
    servicebus_client = ServiceBusClient(
        fully_qualified_namespace=values["AZURE_SERVICEBUS_FULLY_QUALIFIED_NAMESPACE"],
        credential=async_credential,
    )
    receiver_loop = ServiceBusSessionReceiverLoop(
        servicebus_client=servicebus_client,
        worker=worker,
        config=WorkerReceiverConfig(
            queue_name=values["AZURE_SERVICEBUS_WRITES_QUEUE_NAME"],
            message_wait_seconds=float(values.get("AZURE_MEMFOREST_WORKER_MESSAGE_WAIT_SECONDS", "5")),
            session_lock_renewal_seconds=float(values.get("AZURE_MEMFOREST_WORKER_SESSION_LOCK_RENEWAL_SECONDS", "300")),
        ),
        lock_renewer=None,
    )
    return WorkerRuntime(receiver_loop=receiver_loop, closeables=[cosmos_client, servicebus_client, async_credential, sync_credential])


def hydrating_user_forest_core_factory(
    *,
    config: Any,
    chat_client: Any,
    embedding_client: Any,
    snapshot_store: Any,
    scratch_root: Path,
) -> CoreFactory:
    def build(envelope: WriteJobEnvelope) -> ArchiveHydratingUserForestCore:
        from src.forest.user_forest import UserForest

        user_dir = scratch_root / _safe_scope_dir(envelope.tenant_id, envelope.user_id)
        user_forest = UserForest(
            user_id=envelope.user_id,
            user_dir=user_dir,
            config=config,
            chat_client=chat_client,
            embedding_client=embedding_client,
        )
        return ArchiveHydratingUserForestCore(user_forest=user_forest, snapshot_store=snapshot_store)

    return build


def missing_worker_env(environ: dict[str, str]) -> list[str]:
    return [name for name in REQUIRED_WORKER_ENV if not str(environ.get(name, "")).strip()]


def _worker_memforest_config(environ: dict[str, str]):
    from dataclasses import replace

    config_path = environ.get("AZURE_MEMFOREST_CONFIG_PATH", "src/config/default.yaml")
    base = load_default_config(config_path)
    api = APISettings(
        llm=ChatCompletionConfig(
            url=environ["AZURE_OPENAI_ENDPOINT"],
            model_name="default",
            key="",
            top_p=1.0,
            max_token=1024,
            temperature=0.0,
        ),
        embedding=EmbeddingConfig(
            url=environ["AZURE_OPENAI_ENDPOINT"],
            model_name=environ["AZURE_OPENAI_EMBEDDING_DEPLOYMENT"],
            key="",
            dimension=int(environ["AZURE_OPENAI_EMBEDDING_DIMENSION"]),
        ),
    )
    return replace(base, api=api)


def _safe_scope_dir(tenant_id: str, user_id: str) -> str:
    digest = hashlib.sha256(f"{tenant_id}\x1f{user_id}".encode("utf-8")).hexdigest()
    return f"user-{digest}"
