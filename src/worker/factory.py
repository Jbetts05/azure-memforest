"""Factory helpers for scoped worker materialization handlers."""

from __future__ import annotations

import os
import hashlib
import json
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
from src.cloud.cosmos_summary_cache import CosmosSummaryCache
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

REQUIRED_MATERIALIZATION_ENV = (
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
)

REQUIRED_WORKER_ENV = (
    *REQUIRED_MATERIALIZATION_ENV,
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


@dataclass(slots=True)
class MaterializationRuntime:
    handler: WriteJobHandler
    job_store: CosmosJobStore
    closeables: list[Any]


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
    baseline_provider: Any | None = None,
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
            summary_cache=summary_cache
            or CosmosSummaryCache(
                database_client.get_container_client("summary_cache"),
                tenant_id=tenant_id,
                user_id=user_id,
            ),
            snapshot_store=snapshot_store,
            deletion_log=CosmosDeletionLog(database_client.get_container_client("deletions"), tenant_id=tenant_id),
            job_store=CosmosJobStore(database_client.get_container_client("jobs")),
            baseline_provider=baseline_provider,
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
    code_version: str = "unknown",
    prompt_config_hash: str = "unknown",
    lease_ttl_seconds: int = 300,
    lease_renewal_seconds: float = 60.0,
    ingest_timeout_seconds: float = 1800.0,
    extract_heartbeat_seconds: float = 60.0,
    extract_stack_dump_seconds: float = 0.0,
    exit_on_ingest_timeout: bool = False,
    snapshot_policy: str = "always",
    cloud_checkpoint_hydration_supported: bool = False,
    enable_tree_scoped_projection_writes: bool = False,
    enable_partial_hydration_shadow: bool = False,
    enable_partial_hydration_equivalence_shadow: bool = False,
    enable_partial_hydration_guarded_switch: bool = False,
    enable_partial_hydration_authoritative_switch: bool = False,
    partial_hydration_authority_preflight_report_path: str | None = None,
    enable_pre_ingest_route_prediction_prepass: bool = False,
    enable_partial_hydration_dual_compute_shadow: bool = False,
) -> Callable[[WriteJobEnvelope], MaterializationHandlerConfig]:
    if enable_partial_hydration_guarded_switch and not enable_tree_scoped_projection_writes:
        raise ValueError("partial hydration guarded switch requires tree-scoped projection writes")
    if enable_partial_hydration_authoritative_switch and not enable_partial_hydration_guarded_switch:
        raise ValueError("partial hydration authoritative switch requires guarded switch")
    if enable_partial_hydration_authoritative_switch and not str(partial_hydration_authority_preflight_report_path or "").strip():
        raise ValueError("partial hydration authoritative switch requires authority preflight report")

    def build(envelope: WriteJobEnvelope) -> MaterializationHandlerConfig:
        return MaterializationHandlerConfig(
            owner_id=owner_id,
            embedding_model_version=embedding_model_version,
            code_version=code_version,
            prompt_config_hash=prompt_config_hash,
            lease_ttl_seconds=lease_ttl_seconds,
            lease_renewal_seconds=lease_renewal_seconds,
            ingest_timeout_seconds=ingest_timeout_seconds,
            extract_heartbeat_seconds=extract_heartbeat_seconds,
            extract_stack_dump_seconds=extract_stack_dump_seconds,
            exit_on_ingest_timeout=exit_on_ingest_timeout,
            snapshot_policy=snapshot_policy,
            cloud_checkpoint_hydration_supported=cloud_checkpoint_hydration_supported,
            enable_tree_scoped_projection_writes=enable_tree_scoped_projection_writes,
            enable_partial_hydration_shadow=enable_partial_hydration_shadow,
            enable_partial_hydration_equivalence_shadow=enable_partial_hydration_equivalence_shadow,
            enable_partial_hydration_guarded_switch=enable_partial_hydration_guarded_switch,
            enable_partial_hydration_authoritative_switch=enable_partial_hydration_authoritative_switch,
            partial_hydration_authority_preflight_report_path=partial_hydration_authority_preflight_report_path,
            enable_pre_ingest_route_prediction_prepass=enable_pre_ingest_route_prediction_prepass,
            enable_partial_hydration_dual_compute_shadow=enable_partial_hydration_dual_compute_shadow,
        )

    return build


def build_worker_runtime_from_env(environ: dict[str, str] | None = None) -> WorkerRuntime:
    values = dict(os.environ if environ is None else environ)
    missing = missing_worker_env(values)
    if missing:
        raise RuntimeError(f"worker environment is missing required settings: {', '.join(missing)}")
    owner_id = values.get("AZURE_MEMFOREST_WORKER_OWNER_ID") or values.get("HOSTNAME") or "worker"
    materialization = build_scoped_materialization_handler_from_env(values, owner_id=owner_id)

    from azure.identity.aio import ManagedIdentityCredential as AsyncManagedIdentityCredential
    from azure.servicebus.aio import ServiceBusClient

    client_id = values["AZURE_CLIENT_ID"]
    async_credential = AsyncManagedIdentityCredential(client_id=client_id)
    worker = ServiceBusWriteWorker(
        job_store=materialization.job_store,
        handler=materialization.handler,
        owner_id=owner_id,
        lease_ttl_seconds=int(values.get("AZURE_MEMFOREST_JOB_LEASE_TTL_SECONDS", "300")),
        settle_before_handle=str(values.get("AZURE_MEMFOREST_WORKER_SETTLE_BEFORE_HANDLE", "")).strip().lower() in {"1", "true", "yes", "on"},
    )
    servicebus_client = ServiceBusClient(
        fully_qualified_namespace=values["AZURE_SERVICEBUS_FULLY_QUALIFIED_NAMESPACE"],
        credential=async_credential,
    )
    lock_renewer = create_azure_lock_renewer()
    receiver_loop = ServiceBusSessionReceiverLoop(
        servicebus_client=servicebus_client,
        worker=worker,
        config=WorkerReceiverConfig(
            queue_name=values["AZURE_SERVICEBUS_WRITES_QUEUE_NAME"],
            message_wait_seconds=float(values.get("AZURE_MEMFOREST_WORKER_MESSAGE_WAIT_SECONDS", "5")),
            session_lock_renewal_seconds=float(values.get("AZURE_MEMFOREST_WORKER_SESSION_LOCK_RENEWAL_SECONDS", "300")),
            stalled_job_reap_interval_seconds=_reap_interval_seconds(values.get("AZURE_MEMFOREST_STALLED_JOB_REAP_INTERVAL_SECONDS", "60")),
        ),
        lock_renewer=lock_renewer,
    )
    return WorkerRuntime(receiver_loop=receiver_loop, closeables=[*materialization.closeables, servicebus_client, async_credential])


def _validate_authority_preflight_report_from_env(values: dict[str, str]) -> None:
    path = str(values.get("AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_REPORT") or "").strip()
    if not path:
        raise ValueError("partial hydration authoritative switch requires authority preflight report")
    expected_code_version = str(values.get("AZURE_MEMFOREST_GIT_SHA") or "").strip()
    if not expected_code_version or expected_code_version.lower() == "unknown":
        raise ValueError("partial hydration authoritative switch requires AZURE_MEMFOREST_GIT_SHA")
    report_path = Path(path)
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError("partial hydration authority preflight report is unreadable") from exc
    if not isinstance(report, dict) or report.get("authority_allowed") is not True:
        raise ValueError("partial hydration authority preflight report is not approved")
    if str(report.get("expected_code_version") or "").strip() != expected_code_version:
        raise ValueError("partial hydration authority preflight report code version mismatch")
    if str(report.get("artifact_code_version") or "").strip() != expected_code_version:
        raise ValueError("partial hydration authority preflight artifact code version mismatch")


def build_scoped_materialization_handler_from_env(
    environ: dict[str, str] | None = None,
    *,
    owner_id: str | None = None,
) -> MaterializationRuntime:
    values = dict(os.environ if environ is None else environ)
    missing = [name for name in REQUIRED_MATERIALIZATION_ENV if not str(values.get(name, "")).strip()]
    if missing:
        raise RuntimeError(f"materialization environment is missing required settings: {', '.join(missing)}")
    if (
        _env_flag(values.get("AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_AUTHORITATIVE_SWITCH", ""))
        and not _env_flag(values.get("AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_GUARDED_SWITCH", ""))
    ):
        raise ValueError("partial hydration authoritative switch requires guarded switch")
    if _env_flag(values.get("AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_AUTHORITATIVE_SWITCH", "")):
        _validate_authority_preflight_report_from_env(values)

    from azure.cosmos.aio import CosmosClient
    from azure.identity import ManagedIdentityCredential, get_bearer_token_provider
    from azure.identity.aio import ManagedIdentityCredential as AsyncManagedIdentityCredential

    client_id = values["AZURE_CLIENT_ID"]
    async_credential = AsyncManagedIdentityCredential(client_id=client_id)
    sync_credential = ManagedIdentityCredential(client_id=client_id)
    cosmos_client = CosmosClient(values["AZURE_COSMOS_ENDPOINT"], credential=async_credential)
    database = cosmos_client.get_database_client(values["AZURE_COSMOS_DATABASE_NAME"])
    cloud_config = load_cloud_config(values)
    if cloud_config.azure_openai is None:
        raise RuntimeError("Azure OpenAI configuration is required for materialization")
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
        raise RuntimeError("snapshot store configuration is required for materialization")

    store_factory = cosmos_materialization_store_factory(
        database_client=database,
        recall_index=recall_index,
        embedding_model_version=values["AZURE_OPENAI_EMBEDDING_MODEL_VERSION"],
        snapshot_store=snapshot_store,
    )
    cloud_checkpoint_hydration_supported = _env_flag(values.get("AZURE_MEMFOREST_CLOUD_CHECKPOINT_HYDRATION_SUPPORTED", ""))
    core_factory = hydrating_user_forest_core_factory(
        config=memforest_config,
        chat_client=model_client,
        embedding_client=model_client,
        snapshot_store=snapshot_store,
        scratch_root=Path(values.get("AZURE_MEMFOREST_WORKER_SCRATCH_ROOT", "/tmp/azure-memforest-worker")),
        store_factory=store_factory,
        enable_cloud_state_hydration=cloud_checkpoint_hydration_supported,
    )
    resolved_owner_id = owner_id or values.get("AZURE_MEMFOREST_WORKER_OWNER_ID") or values.get("HOSTNAME") or "worker"
    handler = ScopedMaterializationHandler(
        store_factory=store_factory,
        core_factory=core_factory,
        config_factory=fixed_materialization_config_factory(
            owner_id=resolved_owner_id,
            embedding_model_version=values["AZURE_OPENAI_EMBEDDING_MODEL_VERSION"],
            code_version=values.get("AZURE_MEMFOREST_GIT_SHA", "unknown"),
            prompt_config_hash=_prompt_config_hash(values),
            lease_ttl_seconds=int(values.get("AZURE_MEMFOREST_USER_LEASE_TTL_SECONDS", "300")),
            lease_renewal_seconds=float(values.get("AZURE_MEMFOREST_USER_LEASE_RENEWAL_SECONDS", "60")),
            ingest_timeout_seconds=float(values.get("AZURE_MEMFOREST_INGEST_TIMEOUT_SECONDS", "1800")),
            extract_heartbeat_seconds=float(values.get("AZURE_MEMFOREST_EXTRACT_HEARTBEAT_SECONDS", "60")),
            extract_stack_dump_seconds=float(values.get("AZURE_MEMFOREST_EXTRACT_STACK_DUMP_SECONDS", "0")),
            exit_on_ingest_timeout=_env_flag(values.get("AZURE_MEMFOREST_EXIT_ON_INGEST_TIMEOUT", "")),
            snapshot_policy=values.get("AZURE_MEMFOREST_SNAPSHOT_POLICY", "always"),
            cloud_checkpoint_hydration_supported=cloud_checkpoint_hydration_supported,
            enable_tree_scoped_projection_writes=_env_flag(values.get("AZURE_MEMFOREST_ENABLE_TREE_SCOPED_PROJECTION_WRITES", "")),
            enable_partial_hydration_shadow=_env_flag(values.get("AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_SHADOW", "")),
            enable_partial_hydration_equivalence_shadow=_env_flag(values.get("AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_EQUIVALENCE_SHADOW", "")),
            enable_partial_hydration_guarded_switch=_env_flag(values.get("AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_GUARDED_SWITCH", "")),
            enable_partial_hydration_authoritative_switch=_env_flag(values.get("AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_AUTHORITATIVE_SWITCH", "")),
            partial_hydration_authority_preflight_report_path=values.get("AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_REPORT"),
            enable_pre_ingest_route_prediction_prepass=_env_flag(values.get("AZURE_MEMFOREST_ENABLE_PRE_INGEST_ROUTE_PREDICTION_PREPASS", "")),
            enable_partial_hydration_dual_compute_shadow=_env_flag(values.get("AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_DUAL_COMPUTE_SHADOW", "")),
        ),
    )
    return MaterializationRuntime(
        handler=handler,
        job_store=CosmosJobStore(database.get_container_client("jobs")),
        closeables=[cosmos_client, async_credential, sync_credential],
    )


def hydrating_user_forest_core_factory(
    *,
    config: Any,
    chat_client: Any,
    embedding_client: Any,
    snapshot_store: Any,
    scratch_root: Path,
    store_factory: StoreFactory | None = None,
    enable_cloud_state_hydration: bool = False,
) -> CoreFactory:
    def build(envelope: WriteJobEnvelope) -> SyncIngestCore:
        from src.forest.user_forest import UserForest
        from src.worker.cloud_hydration import CloudStateHydratingUserForestCore

        user_dir = scratch_root / _safe_scope_dir(envelope.tenant_id, envelope.user_id)
        user_forest = UserForest(
            user_id=envelope.user_id,
            user_dir=user_dir,
            config=config,
            chat_client=chat_client,
            embedding_client=embedding_client,
        )
        if enable_cloud_state_hydration:
            if store_factory is None:
                raise RuntimeError("store_factory is required for cloud state hydration")
            stores = store_factory(envelope)
            return CloudStateHydratingUserForestCore(
                user_forest=user_forest,
                tree_store_factory=stores.tree_node_store_factory,
                fact_store=stores.fact_store,
                summary_cache=stores.summary_cache,
                browse_embedding_store_factory=stores.browse_embedding_store_factory,
                snapshot_store=snapshot_store,
                enable_cloud_state_hydration=True,
                fallback_to_snapshot=True,
            )
        return ArchiveHydratingUserForestCore(user_forest=user_forest, snapshot_store=snapshot_store)

    return build


def missing_worker_env(environ: dict[str, str]) -> list[str]:
    return [name for name in REQUIRED_WORKER_ENV if not str(environ.get(name, "")).strip()]


def _reap_interval_seconds(value: str | float | int) -> float:
    interval = float(value)
    if interval <= 0:
        return 0.0
    return max(10.0, interval)


def _env_flag(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


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


def _prompt_config_hash(environ: dict[str, str]) -> str:
    keys = [
        "AZURE_OPENAI_CHAT_DEPLOYMENT",
        "AZURE_OPENAI_EXTRACTION_DEPLOYMENT",
        "AZURE_OPENAI_DEDUPE_DEPLOYMENT",
        "AZURE_OPENAI_SUMMARIZATION_DEPLOYMENT",
        "AZURE_OPENAI_PLANNER_DEPLOYMENT",
        "AZURE_OPENAI_BROWSE_DEPLOYMENT",
        "AZURE_OPENAI_ANSWER_DEPLOYMENT",
        "AZURE_OPENAI_CHAT_MODEL_VERSION",
        "AZURE_OPENAI_EMBEDDING_DEPLOYMENT",
        "AZURE_OPENAI_EMBEDDING_MODEL_VERSION",
        "AZURE_OPENAI_EMBEDDING_DIMENSION",
        "AZURE_MEMFOREST_CONFIG_PATH",
    ]
    payload = {key: str(environ.get(key, "")) for key in keys}
    return hashlib.sha256(
        __import__("json").dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
