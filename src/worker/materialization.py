"""Projection helpers for cloud write materialization."""

from __future__ import annotations

import json
import asyncio
import contextlib
import faulthandler
import hashlib
import inspect
import logging
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

from src.cloud.async_batch import run_bounded
from src.cloud.cosmos_browse_embeddings import BrowseEmbeddingRecord
from src.cloud.contracts import FactLocation, RecallDocument, StoredTreeNode
from src.cloud.cosmos_leases import LeaseConflict
from src.cloud.projection_baseline import (
    CommittedProjectionBaseline,
    ProjectionBaselineScope,
    build_committed_projection_baseline,
    summary_cache_refs_from_entries,
)
from src.cloud.projection_plan import ProjectionRecordRef, ProjectionSkipPlan, browse_embedding_content_hash, filter_projection_refs_to_tree_scope, plan_affected_trees, plan_projection_skip, plan_projection_tree_scope, tree_node_body_content_id
from src.cloud.schema import SCHEMA_VERSION
from src.forest.memforest import _content_key
from src.forest.user_forest import ForestMaterializationSnapshot
from src.service.jobs import WriteJobEnvelope
from src.service.schemas import IngestSessionRequest
from src.worker.consumer import PermanentHandlerError, TerminalHandlerError, TransientHandlerError
from src.observability.model_telemetry import collect_model_telemetry, snapshot_model_telemetry, update_model_telemetry_fields
from src.worker.progress import JobProgressBudget, budget_for_envelope, budget_from_document

log = logging.getLogger(__name__)


SNAPSHOT_POLICY_ALWAYS = "always"
SNAPSHOT_POLICY_CLOUD_CHECKPOINT = "cloud_checkpoint"
SUPPORTED_SNAPSHOT_POLICIES = frozenset({SNAPSHOT_POLICY_ALWAYS, SNAPSHOT_POLICY_CLOUD_CHECKPOINT})


@dataclass(frozen=True, slots=True)
class SummaryCacheEntry:
    content_hash: str
    summary: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MaterializationProjection:
    stored_nodes: list[StoredTreeNode]
    fact_locations: list[FactLocation]
    recall_documents: list[RecallDocument]
    browse_embeddings: dict[str, list[float]]
    browse_embedding_records: list[BrowseEmbeddingRecord]
    summary_cache_entries: list[SummaryCacheEntry]
    snapshot_payload: bytes
    changed_tree_ids: frozenset[str] | None = None
    deleted_tree_ids: frozenset[str] = frozenset()
    baseline_forest_version: str | None = None
    projection_skip_plans: Mapping[str, ProjectionSkipPlan] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MaterializationStores:
    lease_store: Any
    source_session_store: Any
    source_turn_store: Any
    fact_store: Any
    manifest_store: Any
    recall_index: Any
    tree_node_store: Any | None = None
    fact_location_index: Any | None = None
    browse_embedding_store: Any | None = None
    summary_cache: Any | None = None
    snapshot_store: Any | None = None
    deletion_log: Any | None = None
    job_store: Any | None = None
    tree_node_store_factory: Any | None = None
    fact_location_index_factory: Any | None = None
    browse_embedding_store_factory: Any | None = None
    baseline_provider: Any | None = None


class SyncIngestCore(Protocol):
    def ingest_session(self, *, session_id: str, turns: list[dict[str, Any]], metadata: dict[str, Any]) -> ForestMaterializationSnapshot:
        ...

    def delete_session(self, *, session_id: str) -> ForestMaterializationSnapshot:
        ...

    def snapshot_for_materialization(self) -> ForestMaterializationSnapshot:
        ...

    def snapshot_payload(self) -> bytes:
        ...

    def hydrate_from_manifest(self, manifest: Any) -> None:
        ...


@dataclass(frozen=True, slots=True)
class MaterializationHandlerConfig:
    owner_id: str
    embedding_model_version: str
    code_version: str = "unknown"
    prompt_config_hash: str = "unknown"
    lease_ttl_seconds: int = 300
    lease_renewal_seconds: float = 60.0
    clear_phase_timeout_seconds: float = 180.0
    clear_optional_timeout_seconds: float = 30.0
    ingest_timeout_seconds: float = 1800.0
    extract_heartbeat_seconds: float = 60.0
    extract_stack_dump_seconds: float = 0.0
    exit_on_ingest_timeout: bool = False
    snapshot_policy: str = SNAPSHOT_POLICY_ALWAYS
    cloud_checkpoint_hydration_supported: bool = False

    def __post_init__(self) -> None:
        policy = _normalize_snapshot_policy(self.snapshot_policy)
        object.__setattr__(self, "snapshot_policy", policy)
        if policy not in SUPPORTED_SNAPSHOT_POLICIES:
            raise ValueError(
                f"unsupported snapshot_policy {self.snapshot_policy!r}; "
                f"expected one of {sorted(SUPPORTED_SNAPSHOT_POLICIES)}"
            )
        if policy != SNAPSHOT_POLICY_ALWAYS and not self.cloud_checkpoint_hydration_supported:
            raise ValueError(
                f"snapshot_policy {policy!r} requires explicit cloud checkpoint hydration support"
            )


class MemForestMaterializationHandler:
    def __init__(
        self,
        *,
        stores: MaterializationStores,
        ingest_core: SyncIngestCore,
        config: MaterializationHandlerConfig,
    ) -> None:
        if not config.embedding_model_version:
            raise ValueError("embedding_model_version is required.")
        self._stores = stores
        self._ingest_core = ingest_core
        self._config = config

    async def handle(self, envelope: WriteJobEnvelope, claimed_job: dict[str, Any]) -> None:
        if envelope.job_type == "delete_session":
            await self._handle_delete(envelope)
            return
        if envelope.job_type == "clear_memory":
            await self._handle_clear_memory(envelope)
            return
        if envelope.job_type != "ingest_session":
            raise PermanentHandlerError(f"unsupported job_type {envelope.job_type!r}")
        await self._handle_ingest(envelope, claimed_job)

    async def _handle_ingest(self, envelope: WriteJobEnvelope, claimed_job: dict[str, Any]) -> None:
        request = _parse_ingest_request(envelope)
        if await self._latest_committed_matches_job(envelope):
            return
        telemetry: dict[str, Any] | None = None
        fencing_token = int(claimed_job["fencing_token"])
        log.info(
            "materialization_ingest_start",
            extra={
                "job_id": envelope.job_id,
                "session_id": envelope.session_id,
                "turn_count": len(request.turns),
                "ingest_timeout_seconds": self._config.ingest_timeout_seconds,
                "extract_heartbeat_seconds": self._config.extract_heartbeat_seconds,
                "extract_stack_dump_seconds": self._config.extract_stack_dump_seconds,
                "exit_on_ingest_timeout": self._config.exit_on_ingest_timeout,
                "code_version": self._config.code_version,
            },
        )

        try:
            lease = await self._stores.lease_store.acquire(
                owner_id=self._config.owner_id,
                ttl_seconds=self._config.lease_ttl_seconds,
            )
        except LeaseConflict as exc:
            raise TransientHandlerError("tenant/user write lease is held by another worker") from exc

        stop_renewal = asyncio.Event()
        renewal_task = asyncio.create_task(self._renew_lease_until_stopped(lease, stop_renewal))
        try:
            await self._update_job_stage(envelope, fencing_token=fencing_token, stage="hydrate", detail="Loading latest committed forest.", telemetry=telemetry)
            if await self._latest_committed_matches_job(envelope):
                return
            latest = await _maybe_await(self._stores.manifest_store.latest_committed())
            await self._hydrate_core_from_manifest(latest)
            generation = 1 if latest is None else int(latest.generation) + 1
            forest_version = _forest_version(generation=generation, job_id=envelope.job_id)
            turns = [turn.model_dump(mode="json") for turn in request.turns]
            await _maybe_await(
                self._stores.source_session_store.upsert_session(
                    envelope.session_id,
                    metadata={
                        **request.metadata,
                        "job_id": envelope.job_id,
                        "forest_version": forest_version,
                    },
                )
            )
            await _maybe_await(self._stores.source_turn_store.upsert_turns(envelope.session_id, turns))

            await self._update_job_stage(envelope, fencing_token=fencing_token, stage="extract_build", detail="Extracting facts, deduping, and rebuilding MemTrees.", telemetry=telemetry)
            try:
                with collect_model_telemetry() as telemetry:
                    snapshot = await self._run_ingest_session(
                        envelope,
                        fencing_token=fencing_token,
                        budget=budget_from_document(claimed_job) or budget_for_envelope(envelope),
                        turns=_extractable_turns(turns),
                        metadata=request.metadata,
                        telemetry=telemetry,
                        session_id=envelope.session_id,
                    )
            except asyncio.TimeoutError as exc:
                raise TransientHandlerError(
                    f"ingest_session timed out after {self._config.ingest_timeout_seconds:.0f}s",
                    exit_code=75 if self._config.exit_on_ingest_timeout else None,
                ) from exc
            except Exception as exc:
                if _is_content_filter_error(exc):
                    raise TerminalHandlerError(str(exc), reason="content_filter") from exc
                raise
            if _is_factless_snapshot(snapshot):
                await self._update_job_stage(envelope, fencing_token=fencing_token, stage="factless_noop", detail="No extractable facts found.", telemetry=telemetry)
                return
            await self._update_job_stage(envelope, fencing_token=fencing_token, stage="manifest_pending", detail="Creating pending forest manifest.", telemetry=telemetry)
            manifest = await _maybe_await(
                self._stores.manifest_store.create_pending(
                    forest_version=forest_version,
                    generation=generation,
                    metadata={
                        "job_id": envelope.job_id,
                        "job_type": envelope.job_type,
                        "session_id": envelope.session_id,
                        "idempotency_key": envelope.idempotency_key,
                        "embedding_model_version": self._config.embedding_model_version,
                        "code_version": self._config.code_version,
                        "prompt_config_hash": self._config.prompt_config_hash,
                        "snapshot_policy": self._config.snapshot_policy,
                        "fencing_token": lease.fencing_token,
                    },
                )
            )
            await self._update_job_stage(envelope, fencing_token=fencing_token, stage="project", detail="Projecting forest snapshot to durable stores.", telemetry=telemetry)
            projection = project_forest_snapshot(
                snapshot,
                tenant_id=envelope.tenant_id,
                user_id=envelope.user_id,
                forest_version=forest_version,
                generation=generation,
                embedding_model_version=self._config.embedding_model_version,
            )
            projection = await self._apply_committed_projection_baseline(latest, projection)
            await self._update_job_stage(envelope, fencing_token=fencing_token, stage="write_projection", detail="Writing facts, trees, embeddings, recall documents, and snapshot.", telemetry=telemetry)
            projection_metadata = await self._write_projection(
                envelope=envelope,
                manifest_id=manifest.manifest_id,
                forest_version=forest_version,
                generation=generation,
                snapshot=snapshot,
                projection=projection,
            )
            try:
                await self._stores.lease_store.assert_current(lease)
            except LeaseConflict as exc:
                raise TransientHandlerError("tenant/user write lease expired before manifest commit") from exc
            await self._update_job_stage(envelope, fencing_token=fencing_token, stage="commit", detail="Committing latest forest manifest.", telemetry=telemetry)
            await _maybe_await(
                self._stores.manifest_store.commit_manifest(
                    manifest.manifest_id,
                    metadata_updates=projection_metadata,
                )
            )
        finally:
            stop_renewal.set()
            renewal_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renewal_task
            try:
                await _maybe_await(self._stores.lease_store.release(lease))
            except Exception:
                log.exception("failed to release materialization lease")

    async def _update_job_stage(
        self,
        envelope: WriteJobEnvelope,
        *,
        fencing_token: int,
        stage: str,
        detail: str = "",
        telemetry: dict[str, Any] | None = None,
    ) -> None:
        if self._stores.job_store is None:
            return
        try:
            await self._stores.job_store.update_stage(
                tenant_id=envelope.tenant_id,
                user_id=envelope.user_id,
                job_id=envelope.job_id,
                owner_id=self._config.owner_id,
                fencing_token=fencing_token,
                stage=stage,
                detail=detail,
                telemetry=telemetry,
            )
        except Exception:
            log.exception("failed to update materialization job stage", extra={"job_id": envelope.job_id, "stage": stage})

    async def _run_ingest_session(
        self,
        envelope: WriteJobEnvelope,
        *,
        fencing_token: int,
        budget: JobProgressBudget,
        session_id: str,
        turns: list[dict[str, Any]],
        metadata: dict[str, Any],
        telemetry: dict[str, Any],
    ) -> ForestMaterializationSnapshot:
        stop_monitor = asyncio.Event()
        ingest_task = asyncio.create_task(
            asyncio.to_thread(
                self._ingest_core.ingest_session,
                session_id=session_id,
                turns=turns,
                metadata=metadata,
            )
        )
        monitor_task = asyncio.create_task(
            self._monitor_extract_build(
                envelope,
                fencing_token=fencing_token,
                budget=budget,
                telemetry=telemetry,
                stop_event=stop_monitor,
            )
        )
        try:
            done, pending = await asyncio.wait(
                {ingest_task, monitor_task},
                timeout=self._config.ingest_timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                ingest_task.cancel()
                monitor_task.cancel()
                raise asyncio.TimeoutError()
            if monitor_task in done:
                # Cancelling asyncio.to_thread does not stop the underlying OS
                # thread. Deployments that enable active budget aborts should
                # also enable exit_on_ingest_timeout so the process is replaced
                # before a ghost thread can write stale projections.
                ingest_task.cancel()
                await monitor_task
            return await ingest_task
        finally:
            stop_monitor.set()
            monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await monitor_task

    async def _monitor_extract_build(
        self,
        envelope: WriteJobEnvelope,
        *,
        fencing_token: int,
        budget: JobProgressBudget,
        telemetry: dict[str, Any],
        stop_event: asyncio.Event,
    ) -> None:
        interval = max(0.01, float(self._config.extract_heartbeat_seconds))
        stack_dump_interval = max(0.0, float(self._config.extract_stack_dump_seconds))
        next_stack_dump = time.monotonic() + stack_dump_interval if stack_dump_interval > 0 else 0.0
        last_signature = _telemetry_progress_signature(telemetry)
        last_progress_at = time.monotonic()
        await self._update_job_stage(
            envelope,
            fencing_token=fencing_token,
            stage="extract_build",
            detail="Extracting facts, deduping, and rebuilding MemTrees.",
            telemetry=_record_extract_heartbeat(telemetry),
        )
        while True:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                signature = _telemetry_progress_signature(telemetry)
                if signature != last_signature:
                    last_signature = signature
                    last_progress_at = time.monotonic()
                completed_calls = int(signature.get("completed_model_calls") or 0)
                if completed_calls > budget.max_completed_model_calls:
                    await self._record_extract_budget_stop(
                        envelope,
                        fencing_token=fencing_token,
                        telemetry=telemetry,
                    )
                    raise TransientHandlerError(
                        f"ingest_session exceeded model-call budget: {completed_calls}>{budget.max_completed_model_calls}",
                        exit_code=75 if self._config.exit_on_ingest_timeout else None,
                    )
                if time.monotonic() - last_progress_at >= budget.stall_timeout_seconds:
                    await self._record_extract_budget_stop(
                        envelope,
                        fencing_token=fencing_token,
                        telemetry=telemetry,
                    )
                    raise TransientHandlerError(
                        f"ingest_session stalled with no real progress for {budget.stall_timeout_seconds:.0f}s",
                        exit_code=75 if self._config.exit_on_ingest_timeout else None,
                    )
                if stack_dump_interval > 0 and time.monotonic() >= next_stack_dump:
                    log.warning(
                        "materialization_extract_build_stack_dump",
                        extra={"job_id": envelope.job_id, "session_id": envelope.session_id},
                    )
                    faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
                    next_stack_dump = time.monotonic() + stack_dump_interval
                await self._update_job_stage(
                    envelope,
                    fencing_token=fencing_token,
                    stage="extract_build",
                    detail="Extracting facts, deduping, and rebuilding MemTrees.",
                    telemetry=_record_extract_heartbeat(telemetry),
                )

    async def _record_extract_budget_stop(
        self,
        envelope: WriteJobEnvelope,
        *,
        fencing_token: int,
        telemetry: dict[str, Any],
    ) -> None:
        try:
            await self._update_job_stage(
                envelope,
                fencing_token=fencing_token,
                stage="extract_build",
                detail="Extracting facts, deduping, and rebuilding MemTrees.",
                telemetry=_record_extract_heartbeat(telemetry),
            )
        except Exception:
            log.exception("failed to record extract-build budget stop telemetry", extra={"job_id": envelope.job_id})

    async def _handle_delete(self, envelope: WriteJobEnvelope) -> None:
        if not envelope.session_id:
            raise PermanentHandlerError("delete_session requires session_id")
        latest = await self._matching_latest_committed(envelope)
        if latest is not None:
            await self._record_deletion(envelope=envelope, manifest=latest)
            return

        try:
            lease = await self._stores.lease_store.acquire(
                owner_id=self._config.owner_id,
                ttl_seconds=self._config.lease_ttl_seconds,
            )
        except LeaseConflict as exc:
            raise TransientHandlerError("tenant/user write lease is held by another worker") from exc

        stop_renewal = asyncio.Event()
        renewal_task = asyncio.create_task(self._renew_lease_until_stopped(lease, stop_renewal))
        try:
            latest = await self._matching_latest_committed(envelope)
            if latest is not None:
                await self._record_deletion(envelope=envelope, manifest=latest)
                return
            current = await _maybe_await(self._stores.manifest_store.latest_committed())
            await self._hydrate_core_from_manifest(current)
            generation = 1 if current is None else int(current.generation) + 1
            forest_version = _forest_version(generation=generation, job_id=envelope.job_id)
            manifest = await _maybe_await(
                self._stores.manifest_store.create_pending(
                    forest_version=forest_version,
                    generation=generation,
                    metadata={
                        "job_id": envelope.job_id,
                        "job_type": envelope.job_type,
                        "session_id": envelope.session_id,
                        "deleted_session_id": envelope.session_id,
                        "idempotency_key": envelope.idempotency_key,
                        "embedding_model_version": self._config.embedding_model_version,
                        "code_version": self._config.code_version,
                        "prompt_config_hash": self._config.prompt_config_hash,
                        "snapshot_policy": self._config.snapshot_policy,
                        "fencing_token": lease.fencing_token,
                    },
                )
            )
            try:
                snapshot = await asyncio.to_thread(self._ingest_core.delete_session, session_id=envelope.session_id)
            except KeyError:
                snapshot = await asyncio.to_thread(self._ingest_core.snapshot_for_materialization)
            projection = project_forest_snapshot(
                snapshot,
                tenant_id=envelope.tenant_id,
                user_id=envelope.user_id,
                forest_version=forest_version,
                generation=generation,
                embedding_model_version=self._config.embedding_model_version,
            )
            projection = await self._apply_committed_projection_baseline(current, projection)
            projection_metadata = await self._write_projection(
                envelope=envelope,
                manifest_id=manifest.manifest_id,
                forest_version=forest_version,
                generation=generation,
                snapshot=snapshot,
                projection=projection,
            )
            await _maybe_await(self._stores.source_session_store.tombstone_session(envelope.session_id, reason="delete_session", scrub=True))
            await _maybe_await(self._stores.source_turn_store.tombstone_session_turns(envelope.session_id, reason="delete_session", scrub_content=True))
            try:
                await self._stores.lease_store.assert_current(lease)
            except LeaseConflict as exc:
                raise TransientHandlerError("tenant/user write lease expired before manifest commit") from exc
            committed = await _maybe_await(
                self._stores.manifest_store.commit_manifest(
                    manifest.manifest_id,
                    metadata_updates=projection_metadata,
                )
            )
            await self._record_deletion(envelope=envelope, manifest=committed)
        finally:
            stop_renewal.set()
            renewal_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renewal_task
            try:
                await _maybe_await(self._stores.lease_store.release(lease))
            except Exception:
                log.exception("failed to release materialization lease")

    async def _handle_clear_memory(self, envelope: WriteJobEnvelope) -> None:
        try:
            lease = await self._stores.lease_store.acquire(
                owner_id=self._config.owner_id,
                ttl_seconds=self._config.lease_ttl_seconds,
            )
        except LeaseConflict as exc:
            raise TransientHandlerError("tenant/user write lease is held by another worker") from exc

        stop_renewal = asyncio.Event()
        renewal_task = asyncio.create_task(self._renew_lease_until_stopped(lease, stop_renewal))
        try:
            await self._run_clear_phase("source_sessions", _purge_container_by_tenant_user_key(self._stores.source_session_store, envelope))
            await self._run_clear_phase("source_turns", _purge_container_by_tenant_user_key(self._stores.source_turn_store, envelope))
            await self._run_clear_phase("facts", _purge_container_by_tenant_user_key(self._stores.fact_store, envelope))
            await self._run_clear_phase("tree_nodes", _purge_container_by_tenant_user_key(self._stores.tree_node_store, envelope))
            await self._run_clear_phase("fact_locations", _purge_container_by_tenant_user_key(self._stores.fact_location_index, envelope))
            await self._run_clear_phase("browse_embeddings", _purge_container_by_tenant_user_key(self._stores.browse_embedding_store, envelope))
            await self._run_clear_phase("summary_cache", _purge_container_by_tenant_user_key(self._stores.summary_cache, envelope))
            await self._run_clear_phase("tree_node_factory", _purge_factory_container(self._stores.tree_node_store_factory, envelope))
            await self._run_clear_phase("fact_location_factory", _purge_factory_container(self._stores.fact_location_index_factory, envelope))
            await self._run_clear_phase("browse_embedding_factory", _purge_factory_container(self._stores.browse_embedding_store_factory, envelope))
            await self._run_clear_phase("manifests", _purge_container_by_tenant_user(self._stores.manifest_store, envelope))
            if self._stores.deletion_log is not None:
                await self._run_clear_phase("deletion_log", _purge_container_by_tenant_user(self._stores.deletion_log, envelope))
            if hasattr(self._stores.recall_index, "delete_by_user"):
                await self._run_optional_clear_phase("recall_index", asyncio.to_thread(self._stores.recall_index.delete_by_user, tenant_id=envelope.tenant_id, user_id=envelope.user_id))
            if hasattr(self._stores.snapshot_store, "delete_prefix"):
                await self._run_optional_clear_phase("snapshot_store", asyncio.to_thread(self._stores.snapshot_store.delete_prefix, f"{envelope.tenant_id}/{envelope.user_id}/"))
            await self._stores.lease_store.assert_current(lease)
        except LeaseConflict as exc:
            raise TransientHandlerError("tenant/user write lease expired during clear_memory") from exc
        finally:
            stop_renewal.set()
            renewal_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renewal_task
            try:
                await _maybe_await(self._stores.lease_store.release(lease))
            except Exception:
                log.exception("failed to release materialization lease")

    async def _run_clear_phase(self, name: str, awaitable: Any) -> Any:
        log.info("clear_memory phase started: %s", name)
        try:
            result = await asyncio.wait_for(awaitable, timeout=self._config.clear_phase_timeout_seconds)
        except asyncio.TimeoutError as exc:
            raise TransientHandlerError(f"clear_memory phase timed out: {name}") from exc
        log.info("clear_memory phase completed: %s", name)
        return result

    async def _run_optional_clear_phase(self, name: str, awaitable: Any) -> Any:
        log.info("clear_memory optional phase started: %s", name)
        try:
            result = await asyncio.wait_for(awaitable, timeout=self._config.clear_optional_timeout_seconds)
        except Exception:
            log.warning("clear_memory optional phase failed or timed out: %s", name, exc_info=True)
            return None
        log.info("clear_memory optional phase completed: %s", name)
        return result

    async def _write_projection(
        self,
        *,
        envelope: WriteJobEnvelope,
        manifest_id: str,
        forest_version: str,
        generation: int,
        snapshot: ForestMaterializationSnapshot,
        projection: MaterializationProjection,
    ) -> dict[str, Any]:
        await asyncio.gather(
            self._call_store_method(self._stores.fact_store.upsert_facts, snapshot.facts),
            self._write_tree_nodes(forest_version, projection),
            self._write_fact_locations(forest_version, projection),
            self._write_browse_embeddings(forest_version, projection),
            self._write_summary_cache_entries(projection),
            self._write_recall_documents(forest_version, projection),
        )
        fingerprint_metadata, fingerprint_payload = _materialization_fingerprint_metadata(
            projection=projection,
            snapshot=snapshot,
            generation=generation,
            forest_version=forest_version,
            embedding_model_version=self._config.embedding_model_version,
            code_version=self._config.code_version,
            prompt_config_hash=self._config.prompt_config_hash,
        )
        metadata: dict[str, Any] = dict(fingerprint_metadata)
        if self._stores.snapshot_store is not None and self._should_write_archive_snapshot():
            snapshot_payload = await self._snapshot_payload(projection)
            snapshot_id = _snapshot_id(
                tenant_id=envelope.tenant_id,
                user_id=envelope.user_id,
                forest_version=forest_version,
            )
            snapshot_hash = _sha256(snapshot_payload)
            await _write_snapshot_idempotent(
                self._stores.snapshot_store,
                snapshot_id=snapshot_id,
                payload=snapshot_payload,
                metadata={
                    "tenant_id": envelope.tenant_id,
                    "user_id": envelope.user_id,
                    "forest_version": forest_version,
                    "manifest_id": manifest_id,
                    "generation": str(generation),
                    "sha256": snapshot_hash,
                },
            )
            metadata = {
                **metadata,
                "snapshot_id": snapshot_id,
                "snapshot_content_sha256": snapshot_hash,
                "snapshot_size_bytes": len(snapshot_payload),
            }
        if self._stores.snapshot_store is not None:
            metadata = {
                **metadata,
                "fingerprint_inputs_id": _fingerprint_inputs_id(
                    tenant_id=envelope.tenant_id,
                    user_id=envelope.user_id,
                    forest_version=forest_version,
                ),
            }
            await _write_snapshot_idempotent(
                self._stores.snapshot_store,
                snapshot_id=metadata["fingerprint_inputs_id"],
                payload=json.dumps(fingerprint_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
                metadata={
                    "tenant_id": envelope.tenant_id,
                    "user_id": envelope.user_id,
                    "forest_version": forest_version,
                    "manifest_id": manifest_id,
                    "sha256": metadata["materialization_fingerprint"],
                    "artifact_kind": "fingerprint_inputs",
                },
            )
        return metadata

    def _should_write_archive_snapshot(self) -> bool:
        return self._config.snapshot_policy == SNAPSHOT_POLICY_ALWAYS

    async def _apply_committed_projection_baseline(self, manifest: Any | None, projection: MaterializationProjection) -> MaterializationProjection:
        if not _projection_baseline_is_authoritative(manifest, projection):
            return projection
        try:
            baseline = await self._build_committed_projection_baseline(manifest, projection)
        except Exception:
            log.warning("failed to build committed projection baseline; falling back to full live writes", exc_info=True)
            return _without_projection_skip_plans(projection)
        return _with_projection_skip_plans(
            projection,
            embedding_model_version=self._config.embedding_model_version,
            committed_baseline=baseline,
        )

    async def _build_committed_projection_baseline(self, manifest: Any, projection: MaterializationProjection) -> CommittedProjectionBaseline:
        scope = ProjectionBaselineScope.from_manifest(manifest, embedding_model_version=self._config.embedding_model_version)
        if self._stores.baseline_provider is not None:
            return await _call_baseline_provider(
                self._stores.baseline_provider,
                scope=scope,
                manifest=manifest,
                projection=projection,
                embedding_model_version=self._config.embedding_model_version,
            )
        baseline_tree_store = _baseline_store_for_version(
            self._stores.tree_node_store,
            self._stores.tree_node_store_factory,
            scope.forest_version,
        )
        baseline_fact_location_store = _baseline_store_for_version(
            self._stores.fact_location_index,
            self._stores.fact_location_index_factory,
            scope.forest_version,
        )
        baseline_browse_store = _baseline_store_for_version(
            self._stores.browse_embedding_store,
            self._stores.browse_embedding_store_factory,
            scope.forest_version,
        )
        baseline = await build_committed_projection_baseline(
            scope,
            tree_nodes_container=_container_from_store(baseline_tree_store),
            fact_locations_container=_container_from_store(baseline_fact_location_store),
            browse_embeddings_container=_container_from_store(baseline_browse_store),
        )
        summary_entries = await self._read_committed_summary_cache_entries(projection)
        if summary_entries:
            baseline = baseline.with_records("summary_cache", summary_cache_refs_from_entries(summary_entries))
        return baseline

    async def _write_tree_nodes(self, forest_version: str, projection: MaterializationProjection) -> None:
        tree_store = self._tree_node_store(forest_version)
        plan = projection.projection_skip_plans.get("tree_node_membership")
        if plan is not None and hasattr(tree_store, "upsert_nodes_delta"):
            await self._call_store_method(
                tree_store.upsert_nodes_delta,
                _safe_upsert_tree_nodes(projection),
                delete_keys=_tree_node_delete_keys(projection),
                unchanged_keys=_tree_node_unchanged_keys(projection),
            )
            return
        await self._call_store_method(tree_store.upsert_nodes, projection.stored_nodes)

    async def _write_fact_locations(self, forest_version: str, projection: MaterializationProjection) -> None:
        location_index = self._fact_location_index(forest_version)
        plan = projection.projection_skip_plans.get("fact_location")
        if plan is not None and hasattr(location_index, "upsert_locations_delta"):
            await self._call_store_method(
                location_index.upsert_locations_delta,
                _safe_upsert_fact_locations(projection),
                delete_keys=_fact_location_delete_keys(projection),
                unchanged_keys=_fact_location_unchanged_keys(projection),
            )
            return
        await self._call_store_method(location_index.upsert_locations, projection.fact_locations)

    async def _read_committed_summary_cache_entries(self, projection: MaterializationProjection) -> dict[str, str]:
        if self._stores.summary_cache is None:
            return {}
        keys = [entry.content_hash for entry in projection.summary_cache_entries]
        if not keys:
            return {}
        cache = self._stores.summary_cache
        if hasattr(cache, "get_many"):
            values = await _maybe_await(cache.get_many(keys))
            return {
                str(key): str(value)
                for key, value in dict(values or {}).items()
                if key in set(keys) and value is not None
            }
        if not hasattr(cache, "get"):
            return {}
        entries: dict[str, str] = {}
        for key in keys:
            value = await _maybe_await(cache.get(key))
            if value is not None:
                entries[key] = str(value)
        return entries

    async def _write_browse_embeddings(self, forest_version: str, projection: MaterializationProjection) -> None:
        browse_store = self._browse_embedding_store(forest_version)
        if browse_store is None:
            return
        records = _safe_upsert_browse_embedding_records(projection)
        if not records:
            return
        if hasattr(browse_store, "upsert_records"):
            await self._call_store_method(
                browse_store.upsert_records,
                records,
                model_version=self._config.embedding_model_version,
            )
            return
        embeddings = {record.node_id: list(record.embedding) for record in records}
        await self._call_store_method(
            browse_store.upsert_embeddings,
            embeddings,
            model_version=self._config.embedding_model_version,
        )

    async def _write_summary_cache_entries(self, projection: MaterializationProjection) -> None:
        if self._stores.summary_cache is None:
            return
        entries = _safe_upsert_summary_cache_entries(projection)
        if not entries:
            return

        async def _put(entry: SummaryCacheEntry) -> None:
            await self._call_store_method(
                self._stores.summary_cache.put,
                entry.content_hash,
                entry.summary,
                metadata=entry.metadata,
            )

        await run_bounded(entries, _put)

    async def _write_recall_documents(self, forest_version: str, projection: MaterializationProjection) -> None:
        plan = projection.projection_skip_plans.get("recall_document")
        if plan is not None and hasattr(self._stores.recall_index, "upsert_documents_delta"):
            await self._call_store_method(
                self._stores.recall_index.upsert_documents_delta,
                _safe_upsert_recall_documents(projection),
                close_refs=_recall_close_refs(projection),
                forest_version=forest_version,
            )
            return
        await self._call_store_method(self._stores.recall_index.upsert_documents, projection.recall_documents)

    async def _call_store_method(self, method: Any, *args: Any, **kwargs: Any) -> Any:
        if inspect.iscoroutinefunction(method):
            return await method(*args, **kwargs)
        result = await asyncio.to_thread(method, *args, **kwargs)
        if hasattr(result, "__await__"):
            return await result
        return result

    async def _latest_committed_matches_job(self, envelope: WriteJobEnvelope) -> bool:
        return await self._matching_latest_committed(envelope) is not None

    async def _snapshot_payload(self, projection: MaterializationProjection) -> bytes:
        snapshot_payload = getattr(self._ingest_core, "snapshot_payload", None)
        if snapshot_payload is None:
            return projection.snapshot_payload
        return await asyncio.to_thread(snapshot_payload)

    async def _hydrate_core_from_manifest(self, manifest: Any | None) -> None:
        if manifest is None:
            return
        manifest_embedding_version = str((getattr(manifest, "metadata", {}) or {}).get("embedding_model_version") or "")
        if manifest_embedding_version and manifest_embedding_version != self._config.embedding_model_version:
            raise PermanentHandlerError("latest committed manifest embedding_model_version does not match worker configuration")
        hydrate = getattr(self._ingest_core, "hydrate_from_manifest", None)
        if hydrate is not None:
            if inspect.iscoroutinefunction(hydrate):
                await hydrate(manifest)
            else:
                await asyncio.to_thread(hydrate, manifest)

    def _tree_node_store(self, forest_version: str) -> Any:
        if self._stores.tree_node_store_factory is not None:
            return self._stores.tree_node_store_factory(forest_version)
        if self._stores.tree_node_store is None:
            raise RuntimeError("tree_node_store or tree_node_store_factory is required")
        return self._stores.tree_node_store

    def _fact_location_index(self, forest_version: str) -> Any:
        if self._stores.fact_location_index_factory is not None:
            return self._stores.fact_location_index_factory(forest_version)
        if self._stores.fact_location_index is None:
            raise RuntimeError("fact_location_index or fact_location_index_factory is required")
        return self._stores.fact_location_index

    def _browse_embedding_store(self, forest_version: str) -> Any | None:
        if self._stores.browse_embedding_store_factory is not None:
            return self._stores.browse_embedding_store_factory(forest_version)
        return self._stores.browse_embedding_store

    async def _matching_latest_committed(self, envelope: WriteJobEnvelope):
        latest = await _maybe_await(self._stores.manifest_store.latest_committed())
        if latest is None:
            return None
        metadata = getattr(latest, "metadata", {}) or {}
        if metadata.get("job_type") != envelope.job_type:
            return None
        if metadata.get("job_id") == envelope.job_id or metadata.get("idempotency_key") == envelope.idempotency_key:
            return latest
        return None

    async def _record_deletion(self, *, envelope: WriteJobEnvelope, manifest: Any) -> None:
        if self._stores.deletion_log is None:
            return
        await _maybe_await(
            self._stores.deletion_log.record_deletion(
                user_id=envelope.user_id,
                deletion_type="session",
                target_id=envelope.session_id,
                reason="delete_session",
                metadata={
                    "job_id": envelope.job_id,
                    "job_type": envelope.job_type,
                    "idempotency_key": envelope.idempotency_key,
                    "manifest_id": manifest.manifest_id,
                    "forest_version": manifest.forest_version,
                    "generation": manifest.generation,
                    "deleted_session_id": envelope.session_id,
                },
            )
        )

    async def _renew_lease_until_stopped(self, lease, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._config.lease_renewal_seconds)
                return
            except TimeoutError:
                pass
            try:
                lease = await self._stores.lease_store.renew(lease, ttl_seconds=self._config.lease_ttl_seconds)
            except Exception:
                log.exception("failed to renew materialization lease")
                return


def project_forest_snapshot(
    snapshot: ForestMaterializationSnapshot,
    *,
    tenant_id: str,
    user_id: str,
    forest_version: str,
    generation: int,
    embedding_model_version: str,
) -> MaterializationProjection:
    if not (tenant_id and user_id and forest_version and embedding_model_version):
        raise ValueError("tenant_id, user_id, forest_version, and embedding_model_version are required.")

    stored_nodes: list[StoredTreeNode] = []
    fact_locations: list[FactLocation] = []
    recall_documents: list[RecallDocument] = []
    browse_embeddings: dict[str, list[float]] = {}
    browse_embedding_records: list[BrowseEmbeddingRecord] = []

    for tree in snapshot.trees:
        root = tree.nodes.get(tree.root_node_id)
        for node in tree.nodes.values():
            stored_nodes.append(
                StoredTreeNode(
                    tree_id=tree.tree_id,
                    node_id=node.node_id,
                    level=_canonical_level(node.level),
                    summary=node.summary,
                    child_ids=tuple(node.child_ids),
                    fact_ids=tuple(_node_fact_ids(tree, node.node_id)),
                    tenant_id=tenant_id,
                    user_id=user_id,
                    forest_version=forest_version,
                    schema_version=SCHEMA_VERSION,
                    metadata={
                        "tree_type": tree.tree_type,
                        "tree_label": tree.label,
                        "tree_key": tree.tree_key,
                        "k": tree.k,
                        "time_start": node.time_start,
                        "time_end": node.time_end,
                        "item_count": node.item_count,
                        "parent_id": node.parent_id,
                        "generation": generation,
                    },
                )
            )
            if node.node_id in snapshot.node_embeddings:
                embedding = list(snapshot.node_embeddings[node.node_id])
                browse_embeddings[node.node_id] = embedding
                browse_embedding_records.append(
                    BrowseEmbeddingRecord(
                        node_id=node.node_id,
                        tree_id=tree.tree_id,
                        tree_type=tree.tree_type,
                        embedding=tuple(embedding),
                    )
                )
            if node.level == 0:
                for fact_id in _node_fact_ids(tree, node.node_id):
                    fact_locations.append(
                        FactLocation(
                            fact_id=fact_id,
                            tree_id=tree.tree_id,
                            node_id=node.node_id,
                            tree_type=tree.tree_type,
                            level=_canonical_level(node.level),
                            tenant_id=tenant_id,
                            user_id=user_id,
                            forest_version=forest_version,
                            schema_version=SCHEMA_VERSION,
                            time_start=node.time_start,
                            time_end=node.time_end,
                            metadata={"generation": generation},
                        )
                    )

        if root is not None and root.node_id in snapshot.node_embeddings:
            recall_documents.append(
                RecallDocument(
                    tree_id=tree.tree_id,
                    node_id=root.node_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    tree_type=tree.tree_type,
                    forest_version=forest_version,
                    embedding_model_version=embedding_model_version,
                    summary=root.summary,
                    embedding=tuple(snapshot.node_embeddings[root.node_id]),
                    schema_version=SCHEMA_VERSION,
                    metadata={
                        "tree_label": tree.label,
                        "tree_key": tree.tree_key,
                        "generation": generation,
                    },
                )
            )

    summary_cache_entries = [
        SummaryCacheEntry(
            content_hash=content_hash,
            summary=summary,
            metadata={
                "forest_version": forest_version,
                "generation": generation,
                "summary_key_hash": content_hash,
            },
        )
        for content_hash, summary in sorted(snapshot.summary_cache.items())
    ]

    projection = MaterializationProjection(
        stored_nodes=stored_nodes,
        fact_locations=fact_locations,
        recall_documents=recall_documents,
        browse_embeddings=browse_embeddings,
        browse_embedding_records=browse_embedding_records,
        summary_cache_entries=summary_cache_entries,
        snapshot_payload=_snapshot_payload(
            snapshot,
            tenant_id=tenant_id,
            user_id=user_id,
            forest_version=forest_version,
            generation=generation,
            embedding_model_version=embedding_model_version,
        ),
        changed_tree_ids=snapshot.changed_tree_ids,
        deleted_tree_ids=snapshot.deleted_tree_ids,
        baseline_forest_version=snapshot.baseline_forest_version,
    )
    return MaterializationProjection(
        stored_nodes=projection.stored_nodes,
        fact_locations=projection.fact_locations,
        recall_documents=projection.recall_documents,
        browse_embeddings=projection.browse_embeddings,
        browse_embedding_records=projection.browse_embedding_records,
        summary_cache_entries=projection.summary_cache_entries,
        snapshot_payload=projection.snapshot_payload,
        changed_tree_ids=projection.changed_tree_ids,
        deleted_tree_ids=projection.deleted_tree_ids,
        baseline_forest_version=projection.baseline_forest_version,
        projection_skip_plans=_build_projection_skip_plans(projection, embedding_model_version=embedding_model_version),
    )


def _parse_ingest_request(envelope: WriteJobEnvelope) -> IngestSessionRequest:
    try:
        return IngestSessionRequest.model_validate(envelope.payload)
    except Exception as exc:
        raise PermanentHandlerError("invalid ingest_session payload") from exc


def _normalize_snapshot_policy(value: str) -> str:
    policy = str(value or SNAPSHOT_POLICY_ALWAYS).strip().lower().replace("-", "_")
    if policy == "checkpoint":
        return SNAPSHOT_POLICY_CLOUD_CHECKPOINT
    return policy


def _build_projection_skip_plans(
    projection: MaterializationProjection,
    *,
    embedding_model_version: str,
    committed_baseline: CommittedProjectionBaseline | None = None,
) -> dict[str, ProjectionSkipPlan]:
    projected = {
        "tree_node_membership": _tree_node_projection_refs(projection.stored_nodes),
        "fact_location": _fact_location_projection_refs(projection.fact_locations),
        "recall_document": _recall_document_projection_refs(projection.recall_documents),
        "browse_embedding": _browse_embedding_projection_refs(
            projection.browse_embedding_records,
            embedding_model_version=embedding_model_version,
        ),
        "summary_cache": _summary_cache_projection_refs(projection.summary_cache_entries),
    }
    retained = _retained_committed_projection_refs(projection, projected, committed_baseline=committed_baseline)
    return {
        kind: plan_projection_skip(records, retained.get(kind, ()))
        for kind, records in projected.items()
    }


def _with_projection_skip_plans(
    projection: MaterializationProjection,
    *,
    embedding_model_version: str,
    committed_baseline: CommittedProjectionBaseline | None = None,
) -> MaterializationProjection:
    return MaterializationProjection(
        stored_nodes=projection.stored_nodes,
        fact_locations=projection.fact_locations,
        recall_documents=projection.recall_documents,
        browse_embeddings=projection.browse_embeddings,
        browse_embedding_records=projection.browse_embedding_records,
        summary_cache_entries=projection.summary_cache_entries,
        snapshot_payload=projection.snapshot_payload,
        changed_tree_ids=projection.changed_tree_ids,
        deleted_tree_ids=projection.deleted_tree_ids,
        baseline_forest_version=projection.baseline_forest_version,
        projection_skip_plans=_build_projection_skip_plans(
            projection,
            embedding_model_version=embedding_model_version,
            committed_baseline=committed_baseline,
        ),
    )


def _without_projection_skip_plans(projection: MaterializationProjection) -> MaterializationProjection:
    return MaterializationProjection(
        stored_nodes=projection.stored_nodes,
        fact_locations=projection.fact_locations,
        recall_documents=projection.recall_documents,
        browse_embeddings=projection.browse_embeddings,
        browse_embedding_records=projection.browse_embedding_records,
        summary_cache_entries=projection.summary_cache_entries,
        snapshot_payload=projection.snapshot_payload,
        changed_tree_ids=projection.changed_tree_ids,
        deleted_tree_ids=projection.deleted_tree_ids,
        baseline_forest_version=projection.baseline_forest_version,
        projection_skip_plans={},
    )


def _retained_committed_projection_refs(
    projection: MaterializationProjection,
    projected: Mapping[str, tuple[ProjectionRecordRef, ...]],
    *,
    committed_baseline: CommittedProjectionBaseline | None = None,
) -> dict[str, tuple[ProjectionRecordRef, ...]]:
    if committed_baseline is None:
        return {}
    # Tree memberships and fact locations use generation-range MVCC, so
    # unchanged records can remain visible without per-version copy-forward.
    # Shared browse/summary stores are intersection-limited so per-job deletes
    # stay GC/retention controlled.
    retained: dict[str, tuple[ProjectionRecordRef, ...]] = {}
    for kind in ("tree_node_membership", "fact_location", "recall_document"):
        retained[kind] = committed_baseline.records_for(kind)
    for kind in ("browse_embedding", "summary_cache"):
        projected_records = projected.get(kind, ())
        projected_keys = {record.key for record in projected_records}
        retained[kind] = tuple(
            record
            for record in committed_baseline.records_for(kind)
            if record.key in projected_keys
        )
    return retained


def _tree_node_projection_refs(nodes: list[StoredTreeNode]) -> tuple[ProjectionRecordRef, ...]:
    return tuple(
        ProjectionRecordRef(
            "tree_node_membership",
            f"{node.tree_id}/{node.node_id}",
            content_hash=_tree_node_stable_content_hash(node),
            content_id=_tree_node_stable_body_id(node),
            metadata={"tree_id": node.tree_id, "node_id": node.node_id},
        )
        for node in nodes
    )


def _fact_location_projection_refs(locations: list[FactLocation]) -> tuple[ProjectionRecordRef, ...]:
    return tuple(
        ProjectionRecordRef(
            "fact_location",
            f"{location.fact_id}/{location.tree_id}/{location.node_id}/{location.tree_type}/{location.level}",
            content_hash=_fact_location_stable_content_hash(location),
            metadata={"tree_id": location.tree_id, "node_id": location.node_id, "fact_id": location.fact_id},
        )
        for location in locations
    )


def _recall_document_projection_refs(documents: list[RecallDocument]) -> tuple[ProjectionRecordRef, ...]:
    return tuple(
        ProjectionRecordRef(
            "recall_document",
            f"{document.tree_id}/{document.node_id}",
            content_hash=_recall_document_stable_content_hash(document),
            metadata={"tree_id": document.tree_id, "node_id": document.node_id},
        )
        for document in documents
    )


def _browse_embedding_projection_refs(
    records: list[BrowseEmbeddingRecord],
    *,
    embedding_model_version: str,
) -> tuple[ProjectionRecordRef, ...]:
    return tuple(
        ProjectionRecordRef(
            "browse_embedding",
            f"{record.node_id}/{embedding_model_version}",
            content_hash=_browse_embedding_stable_content_hash(record),
            metadata={"tree_id": record.tree_id, "node_id": record.node_id},
        )
        for record in records
    )


def _summary_cache_projection_refs(entries: list[SummaryCacheEntry]) -> tuple[ProjectionRecordRef, ...]:
    return tuple(
        ProjectionRecordRef(
            "summary_cache",
            entry.content_hash,
            content_hash=_sha256(entry.summary.encode("utf-8")),
        )
        for entry in entries
    )


def _baseline_store_for_version(store: Any | None, factory: Any | None, forest_version: str) -> Any | None:
    if factory is None:
        return store
    return factory(forest_version)


def _container_from_store(store: Any | None) -> Any | None:
    if store is None:
        return None
    if hasattr(store, "query_items"):
        return store
    container = getattr(store, "container", None)
    if container is not None and hasattr(container, "query_items"):
        return container
    return None


async def _call_baseline_provider(provider: Any, **kwargs: Any) -> CommittedProjectionBaseline:
    method = provider
    if not callable(method):
        method = getattr(provider, "build_committed_projection_baseline", None) or getattr(provider, "build_baseline", None)
    if method is None or not callable(method):
        raise TypeError("baseline_provider must be callable or expose build_baseline")
    baseline = await _maybe_await(_call_with_supported_kwargs(method, **kwargs))
    if not isinstance(baseline, CommittedProjectionBaseline):
        raise TypeError("baseline_provider must return CommittedProjectionBaseline")
    return baseline


def _call_with_supported_kwargs(method: Any, **kwargs: Any) -> Any:
    signature = inspect.signature(method)
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values()):
        return method(**kwargs)
    supported = {key: value for key, value in kwargs.items() if key in signature.parameters}
    return method(**supported)


def _projection_baseline_is_authoritative(manifest: Any | None, projection: MaterializationProjection) -> bool:
    if manifest is None:
        return False
    if str(getattr(manifest, "status", "") or "") != "committed":
        return False
    baseline_forest_version = str(projection.baseline_forest_version or "")
    manifest_forest_version = str(getattr(manifest, "forest_version", "") or "")
    return bool(baseline_forest_version) and baseline_forest_version == manifest_forest_version


def _baseline_recall_search_client(recall_index: Any | None) -> Any | None:
    if recall_index is None:
        return None
    raw_client = getattr(recall_index, "_search_client", None)
    if raw_client is not None and hasattr(raw_client, "search"):
        return raw_client
    client_factory = getattr(recall_index, "_client", None)
    if client_factory is not None:
        try:
            raw_client = client_factory()
        except Exception:
            raw_client = None
        if raw_client is not None and hasattr(raw_client, "search"):
            return raw_client
    search = getattr(recall_index, "search", None)
    if search is None:
        return None
    try:
        signature = inspect.signature(search)
    except (TypeError, ValueError):
        return None
    accepts_keywords = any(param.kind is inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values())
    return recall_index if accepts_keywords or "search_text" in signature.parameters else None


def _safe_upsert_browse_embedding_records(projection: MaterializationProjection) -> list[BrowseEmbeddingRecord]:
    plan = projection.projection_skip_plans.get("browse_embedding")
    if plan is None:
        return list(projection.browse_embedding_records)
    upsert_node_ids = {str(record.metadata.get("node_id") or "") for record in plan.upsert}
    return [record for record in projection.browse_embedding_records if record.node_id in upsert_node_ids]


def _safe_upsert_tree_nodes(projection: MaterializationProjection) -> list[StoredTreeNode]:
    plan = projection.projection_skip_plans.get("tree_node_membership")
    if plan is None:
        return list(projection.stored_nodes)
    upsert_keys = {record.logical_id for record in plan.upsert}
    return [node for node in projection.stored_nodes if f"{node.tree_id}/{node.node_id}" in upsert_keys]


def _tree_node_delete_keys(projection: MaterializationProjection) -> list[tuple[str, str]]:
    plan = projection.projection_skip_plans.get("tree_node_membership")
    if plan is None:
        return []
    keys: list[tuple[str, str]] = []
    for record in plan.delete:
        tree_id = str(record.metadata.get("tree_id") or "")
        node_id = str(record.metadata.get("node_id") or "")
        if tree_id and node_id:
            keys.append((tree_id, node_id))
    return keys


def _tree_node_unchanged_keys(projection: MaterializationProjection) -> list[tuple[str, str]]:
    plan = projection.projection_skip_plans.get("tree_node_membership")
    if plan is None:
        return []
    keys: list[tuple[str, str]] = []
    for record in plan.unchanged:
        tree_id = str(record.metadata.get("tree_id") or "")
        node_id = str(record.metadata.get("node_id") or "")
        if tree_id and node_id:
            keys.append((tree_id, node_id))
    return keys


def _safe_upsert_fact_locations(projection: MaterializationProjection) -> list[FactLocation]:
    plan = projection.projection_skip_plans.get("fact_location")
    if plan is None:
        return list(projection.fact_locations)
    upsert_keys = {record.logical_id for record in plan.upsert}
    return [
        location
        for location in projection.fact_locations
        if f"{location.fact_id}/{location.tree_id}/{location.node_id}/{location.tree_type}/{location.level}" in upsert_keys
    ]


def _fact_location_delete_keys(projection: MaterializationProjection) -> list[str]:
    plan = projection.projection_skip_plans.get("fact_location")
    if plan is None:
        return []
    return [record.logical_id for record in plan.delete]


def _fact_location_unchanged_keys(projection: MaterializationProjection) -> list[str]:
    plan = projection.projection_skip_plans.get("fact_location")
    if plan is None:
        return []
    return [record.logical_id for record in plan.unchanged]


def _safe_upsert_recall_documents(projection: MaterializationProjection) -> list[RecallDocument]:
    plan = projection.projection_skip_plans.get("recall_document")
    if plan is None:
        return list(projection.recall_documents)
    upsert_keys = {record.logical_id for record in plan.upsert}
    return [document for document in projection.recall_documents if f"{document.tree_id}/{document.node_id}" in upsert_keys]


def _recall_close_refs(projection: MaterializationProjection) -> list[ProjectionRecordRef]:
    plan = projection.projection_skip_plans.get("recall_document")
    if plan is None:
        return []
    return [*plan.delete, *plan.superseded]


def _safe_upsert_summary_cache_entries(projection: MaterializationProjection) -> list[SummaryCacheEntry]:
    plan = projection.projection_skip_plans.get("summary_cache")
    if plan is None:
        return list(projection.summary_cache_entries)
    upsert_hashes = {record.logical_id for record in plan.upsert}
    return [entry for entry in projection.summary_cache_entries if entry.content_hash in upsert_hashes]


def _projection_plan_counts(projection: MaterializationProjection) -> dict[str, dict[str, int]]:
    return {
        kind: {
            "unchanged": len(plan.unchanged),
            "upsert": len(plan.upsert),
            "delete": len(plan.delete),
        }
        for kind, plan in sorted(projection.projection_skip_plans.items())
    }


def _tree_node_stable_content_hash(node: StoredTreeNode) -> str:
    metadata = {
        key: value
        for key, value in dict(node.metadata).items()
        if key not in {"generation", "forest_version"}
    }
    return _hash_json(
        {
            "level": node.level,
            "summary": node.summary,
            "child_ids": list(node.child_ids),
            "fact_ids": list(node.fact_ids),
            "tree_id": node.tree_id,
            "node_id": node.node_id,
            "metadata": metadata,
        }
    )


def _tree_node_stable_body_id(node: StoredTreeNode) -> str:
    return tree_node_body_content_id(
        tenant_user_key=f"{node.tenant_id}:{node.user_id}" if node.tenant_id and node.user_id else "",
        tree_id=node.tree_id,
        node_id=node.node_id,
        content_sha256=_tree_node_stable_content_hash(node),
    )


def _fact_location_stable_content_hash(location: FactLocation) -> str:
    metadata = {
        key: value
        for key, value in dict(location.metadata).items()
        if key not in {"generation", "forest_version"}
    }
    return _hash_json(
        {
            "fact_id": location.fact_id,
            "tree_id": location.tree_id,
            "node_id": location.node_id,
            "tree_type": location.tree_type,
            "level": location.level,
            "time_start": location.time_start,
            "time_end": location.time_end,
            "metadata": metadata,
        }
    )


def _recall_document_stable_content_hash(document: RecallDocument) -> str:
    return _hash_json(
        {
            "tenant_id": document.tenant_id,
            "user_id": document.user_id,
            "tree_id": document.tree_id,
            "node_id": document.node_id,
            "tree_type": document.tree_type,
            "embedding_model_version": document.embedding_model_version,
            "summary": document.summary,
            "embedding": [float(value) for value in document.embedding],
        }
    )


def _browse_embedding_stable_content_hash(record: BrowseEmbeddingRecord) -> str:
    return browse_embedding_content_hash(
        node_id=record.node_id,
        tree_id=record.tree_id,
        tree_type=record.tree_type,
        embedding=record.embedding,
    )


async def _write_snapshot_idempotent(snapshot_store: Any, *, snapshot_id: str, payload: bytes, metadata: dict[str, str]) -> None:
    try:
        await _maybe_await(snapshot_store.write_snapshot(snapshot_id=snapshot_id, payload=payload, metadata=metadata))
    except Exception as exc:
        if _is_conflict(exc):
            return
        raise


def _forest_version(*, generation: int, job_id: str) -> str:
    return f"g{generation}-{job_id[-12:]}"


def _snapshot_id(*, tenant_id: str, user_id: str, forest_version: str) -> str:
    return f"{tenant_id}/{user_id}/{forest_version}.json"


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _materialization_fingerprint_metadata(
    *,
    projection: MaterializationProjection,
    snapshot: ForestMaterializationSnapshot,
    generation: int,
    forest_version: str,
    embedding_model_version: str,
    code_version: str,
    prompt_config_hash: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    inputs = {
        "schema_version": SCHEMA_VERSION,
        "generation": generation,
        "forest_version": forest_version,
        "embedding_model_version": embedding_model_version,
        "code_version": code_version,
        "prompt_config_hash": prompt_config_hash,
        "fact_set_hash": _hash_json(
            [
                {
                    "fact_id": fact.fact_id,
                    "fact_text": fact.fact_text,
                    "time_start": fact.time_start,
                    "time_end": fact.time_end,
                    "occurrence_count": len(fact.occurrences),
                }
                for fact in sorted(snapshot.facts, key=lambda item: item.fact_id)
            ]
        ),
        "tree_node_payload_hash": _hash_json(
            [
                {
                    "tree_id": node.tree_id,
                    "node_id": node.node_id,
                    "level": node.level,
                    "summary": node.summary,
                    "child_ids": list(node.child_ids),
                    "fact_ids": list(node.fact_ids),
                    "metadata": dict(node.metadata),
                }
                for node in sorted(projection.stored_nodes, key=lambda item: (item.tree_id, item.node_id))
            ]
        ),
        "fact_location_hash": _hash_json(
            [
                {
                    "fact_id": location.fact_id,
                    "tree_id": location.tree_id,
                    "node_id": location.node_id,
                    "tree_type": location.tree_type,
                    "level": location.level,
                }
                for location in sorted(projection.fact_locations, key=lambda item: (item.fact_id, item.tree_id, item.node_id))
            ]
        ),
        "recall_document_hash": _hash_json(
            [
                {
                    "tree_id": doc.tree_id,
                    "node_id": doc.node_id,
                    "tree_type": doc.tree_type,
                    "summary": doc.summary,
                    "embedding_hash": _hash_json(list(doc.embedding)),
                }
                for doc in sorted(projection.recall_documents, key=lambda item: (item.tree_id, item.node_id))
            ]
        ),
        "browse_embedding_hash": _hash_json(
            [
                {
                    "node_id": record.node_id,
                    "tree_id": record.tree_id,
                    "tree_type": record.tree_type,
                    "embedding_hash": _hash_json(list(record.embedding)),
                }
                for record in sorted(projection.browse_embedding_records, key=lambda item: (item.tree_id, item.node_id))
            ]
        ),
        "summary_cache_hash": _hash_json(
            [
                {
                    "content_hash": entry.content_hash,
                    "summary_hash": _sha256(entry.summary.encode("utf-8")),
                }
                for entry in sorted(projection.summary_cache_entries, key=lambda item: item.content_hash)
            ]
        ),
        "projection_delta": {
            "changed_tree_ids": None if projection.changed_tree_ids is None else sorted(projection.changed_tree_ids),
            "deleted_tree_ids": sorted(projection.deleted_tree_ids),
            "baseline_forest_version": projection.baseline_forest_version,
            "full_projection_required": projection.changed_tree_ids is None,
        },
        "projection_plan_counts": _projection_plan_counts(projection),
        "dirty_path_plan": _dirty_path_plan_summary(projection),
        "projection_tree_scope_plan": _projection_tree_scope_plan_summary(projection),
        "projection_tree_scope_shadow": _projection_tree_scope_shadow_summary(
            projection,
            embedding_model_version=embedding_model_version,
        ),
    }
    return {
        "materialization_fingerprint": _hash_json(inputs),
        "materialization_schema_version": SCHEMA_VERSION,
        "embedding_model_version": embedding_model_version,
        "code_version": code_version,
        "prompt_config_hash": prompt_config_hash,
        "fact_set_hash": inputs["fact_set_hash"],
        "tree_node_payload_hash": inputs["tree_node_payload_hash"],
        "fact_location_hash": inputs["fact_location_hash"],
        "recall_document_hash": inputs["recall_document_hash"],
        "browse_embedding_hash": inputs["browse_embedding_hash"],
        "summary_cache_hash": inputs["summary_cache_hash"],
        "projection_delta_changed_tree_ids": None if projection.changed_tree_ids is None else sorted(projection.changed_tree_ids),
        "projection_delta_deleted_tree_ids": sorted(projection.deleted_tree_ids),
        "projection_delta_baseline_forest_version": projection.baseline_forest_version,
        "projection_delta_full_required": projection.changed_tree_ids is None,
        "projection_plan_counts": inputs["projection_plan_counts"],
        "dirty_path_plan": inputs["dirty_path_plan"],
        "projection_tree_scope_plan": inputs["projection_tree_scope_plan"],
        "projection_tree_scope_shadow": inputs["projection_tree_scope_shadow"],
        "projection_plan_live_skipped_browse_embedding_count": len(projection.projection_skip_plans.get("browse_embedding", ProjectionSkipPlan((), (), ())).unchanged),
        "projection_plan_live_skipped_summary_cache_count": len(projection.projection_skip_plans.get("summary_cache", ProjectionSkipPlan((), (), ())).unchanged),
    }, inputs


def _hash_json(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _dirty_path_plan_summary(projection: MaterializationProjection) -> dict[str, Any]:
    tree_plan = projection.projection_skip_plans.get("tree_node_membership")
    location_plan = projection.projection_skip_plans.get("fact_location")
    recall_plan = projection.projection_skip_plans.get("recall_document")
    skip_plans_present = bool(projection.projection_skip_plans)
    affected = plan_affected_trees(projection.projection_skip_plans)
    return {
        "schema_version": 1,
        "mode": "instrumentation_only",
        "skip_plans_present": skip_plans_present,
        "id_arrays_truncated": (
            affected.affected_tree_count > 100
            or affected.affected_fact_count > 100
            or affected.dirty_node_count > 200
            or affected.dirty_root_tree_count > 100
        ),
        "baseline_forest_version": projection.baseline_forest_version,
        "full_projection_required": projection.changed_tree_ids is None,
        "changed_tree_ids": None if projection.changed_tree_ids is None else sorted(projection.changed_tree_ids),
        "deleted_tree_ids": sorted(projection.deleted_tree_ids),
        "affected_tree_count": affected.affected_tree_count,
        "affected_tree_ids": list(affected.affected_tree_ids[:100]),
        "affected_fact_count": affected.affected_fact_count,
        "affected_fact_ids": list(affected.affected_fact_ids[:100]),
        "dirty_node_count": affected.dirty_node_count,
        "dirty_node_keys": list(affected.dirty_node_keys[:200]),
        "dirty_root_tree_count": affected.dirty_root_tree_count,
        "dirty_root_tree_ids": list(affected.dirty_root_tree_ids[:100]),
        "candidate_upsert_tree_node_count": len(tree_plan.upsert) if tree_plan is not None else len(projection.stored_nodes),
        "candidate_delete_tree_node_count": len(tree_plan.delete) if tree_plan is not None else 0,
        "candidate_upsert_fact_location_count": len(location_plan.upsert) if location_plan is not None else len(projection.fact_locations),
        "candidate_delete_fact_location_count": len(location_plan.delete) if location_plan is not None else 0,
        "candidate_upsert_recall_document_count": len(recall_plan.upsert) if recall_plan is not None else len(projection.recall_documents),
        "candidate_close_recall_document_count": (len(recall_plan.delete) + len(recall_plan.superseded)) if recall_plan is not None else 0,
    }


def _projection_tree_scope_plan_summary(projection: MaterializationProjection) -> dict[str, Any]:
    # Tree-bearing projection surfaces define the scope universe. Fact locations
    # are counted after scope selection and are not authoritative tree records.
    all_tree_ids = {
        str(getattr(record, "tree_id", "") or "")
        for record in [*projection.stored_nodes, *projection.recall_documents, *projection.browse_embedding_records]
        if str(getattr(record, "tree_id", "") or "")
    }
    scope = plan_projection_tree_scope(
        all_tree_ids=all_tree_ids,
        changed_tree_ids=projection.changed_tree_ids,
        deleted_tree_ids=projection.deleted_tree_ids,
    )
    projected_tree_ids = set(scope.projected_tree_ids)
    tree_node_refs = _tree_node_projection_refs(projection.stored_nodes)
    fact_location_refs = _fact_location_projection_refs(projection.fact_locations)
    recall_refs = _recall_document_projection_refs(projection.recall_documents)
    browse_refs = _browse_embedding_projection_refs(
        projection.browse_embedding_records,
        embedding_model_version="",
    )
    scoped_tree_nodes = filter_projection_refs_to_tree_scope(tree_node_refs, tree_ids=projected_tree_ids)
    scoped_fact_locations = filter_projection_refs_to_tree_scope(fact_location_refs, tree_ids=projected_tree_ids)
    scoped_recall = filter_projection_refs_to_tree_scope(recall_refs, tree_ids=projected_tree_ids)
    scoped_browse = filter_projection_refs_to_tree_scope(browse_refs, tree_ids=projected_tree_ids)
    return {
        "schema_version": 1,
        "mode": "instrumentation_only",
        "full_projection_required": scope.full_projection_required,
        "projected_tree_count": len(scope.projected_tree_ids),
        "projected_tree_ids": list(scope.projected_tree_ids[:100]),
        "omitted_tree_count": len(scope.omitted_tree_ids),
        "omitted_tree_ids": list(scope.omitted_tree_ids[:100]),
        "deleted_tree_count": len(scope.deleted_tree_ids),
        "deleted_tree_ids": list(scope.deleted_tree_ids[:100]),
        "unknown_changed_tree_count": len(scope.unknown_changed_tree_ids),
        "unknown_changed_tree_ids": list(scope.unknown_changed_tree_ids[:100]),
        "id_arrays_truncated": (
            len(scope.projected_tree_ids) > 100
            or len(scope.omitted_tree_ids) > 100
            or len(scope.deleted_tree_ids) > 100
            or len(scope.unknown_changed_tree_ids) > 100
        ),
        "scoped_tree_node_ref_count": len(scoped_tree_nodes),
        "scoped_fact_location_ref_count": len(scoped_fact_locations),
        "scoped_recall_document_ref_count": len(scoped_recall),
        "scoped_browse_embedding_ref_count": len(scoped_browse),
        "full_tree_node_ref_count": len(tree_node_refs),
        "full_fact_location_ref_count": len(fact_location_refs),
        "full_recall_document_ref_count": len(recall_refs),
        "full_browse_embedding_ref_count": len(browse_refs),
    }


def _projection_tree_scope_shadow_summary(projection: MaterializationProjection, *, embedding_model_version: str) -> dict[str, Any]:
    if not projection.projection_skip_plans:
        return {
            "schema_version": 1,
            "mode": "shadow_compare",
            "skip_plans_present": False,
            "comparison_supported": False,
            "reason": "projection_skip_plans_missing",
        }
    all_tree_ids = {
        str(getattr(record, "tree_id", "") or "")
        for record in [*projection.stored_nodes, *projection.recall_documents, *projection.browse_embedding_records]
        if str(getattr(record, "tree_id", "") or "")
    }
    scope = plan_projection_tree_scope(
        all_tree_ids=all_tree_ids,
        changed_tree_ids=projection.changed_tree_ids,
        deleted_tree_ids=projection.deleted_tree_ids,
    )
    if scope.full_projection_required:
        return {
            "schema_version": 1,
            "mode": "shadow_compare",
            "skip_plans_present": True,
            "comparison_supported": False,
            "reason": "full_projection_required",
            "unknown_changed_tree_count": len(scope.unknown_changed_tree_ids),
        }
    if scope.unknown_changed_tree_ids:
        return {
            "schema_version": 1,
            "mode": "shadow_compare",
            "skip_plans_present": True,
            "comparison_supported": False,
            "reason": "unknown_changed_tree_ids",
            "unknown_changed_tree_count": len(scope.unknown_changed_tree_ids),
            "unknown_changed_tree_ids": list(scope.unknown_changed_tree_ids[:100]),
        }

    scoped_tree_ids = set(scope.projected_tree_ids)
    scoped_projected = _tree_scoped_projection_refs(
        projection,
        scoped_tree_ids,
        embedding_model_version=embedding_model_version,
    )
    full_plans = projection.projection_skip_plans
    full_projected = _projection_refs_by_kind(projection, embedding_model_version=embedding_model_version)
    shadow: dict[str, dict[str, int | bool]] = {}
    divergent = False
    for kind in ("tree_node_membership", "fact_location", "recall_document", "browse_embedding"):
        scoped_plan = plan_projection_skip(scoped_projected.get(kind, ()), _retained_records_for_shadow(full_plans.get(kind), scoped_projected.get(kind, ())))
        full_plan = full_plans.get(kind, ProjectionSkipPlan((), (), ()))
        counts = _skip_plan_divergence_counts(full_plan, scoped_plan)
        shadow[kind] = counts
        divergent = divergent or any(
            counts[key] > 0
            for key in (
                "missing_upsert",
                "extra_upsert",
                "missing_delete",
                "extra_delete",
                "missing_superseded",
                "extra_superseded",
            )
        )
    return {
        "schema_version": 1,
        "mode": "shadow_compare",
        "skip_plans_present": True,
        "comparison_supported": True,
        "projected_tree_count": len(scope.projected_tree_ids),
        "omitted_tree_count": len(scope.omitted_tree_ids),
        "unknown_changed_tree_count": len(scope.unknown_changed_tree_ids),
        "divergent": divergent,
        "by_kind": shadow,
        "full_tree_node_ref_count": len(full_projected.get("tree_node_membership", ())),
        "scoped_tree_node_ref_count": len(scoped_projected.get("tree_node_membership", ())),
    }


def _projection_refs_by_kind(projection: MaterializationProjection, *, embedding_model_version: str) -> dict[str, tuple[ProjectionRecordRef, ...]]:
    return {
        "tree_node_membership": _tree_node_projection_refs(projection.stored_nodes),
        "fact_location": _fact_location_projection_refs(projection.fact_locations),
        "recall_document": _recall_document_projection_refs(projection.recall_documents),
        "browse_embedding": _browse_embedding_projection_refs(
            projection.browse_embedding_records,
            embedding_model_version=embedding_model_version,
        ),
    }


def _tree_scoped_projection_refs(projection: MaterializationProjection, tree_ids: set[str], *, embedding_model_version: str) -> dict[str, tuple[ProjectionRecordRef, ...]]:
    projected = _projection_refs_by_kind(projection, embedding_model_version=embedding_model_version)
    return {
        kind: filter_projection_refs_to_tree_scope(records, tree_ids=tree_ids)
        for kind, records in projected.items()
    }


def _retained_records_for_shadow(plan: ProjectionSkipPlan | None, scoped_projected: tuple[ProjectionRecordRef, ...]) -> tuple[ProjectionRecordRef, ...]:
    if plan is None:
        return ()
    scoped_keys = {record.key for record in scoped_projected}
    return tuple(
        record
        for record in [*plan.unchanged, *plan.delete, *plan.superseded]
        if record.key in scoped_keys or record.key in plan.delete_keys
    )


def _skip_plan_divergence_counts(full: ProjectionSkipPlan, scoped: ProjectionSkipPlan) -> dict[str, int | bool]:
    full_superseded = frozenset(record.key for record in full.superseded)
    scoped_superseded = frozenset(record.key for record in scoped.superseded)
    return {
        "full_unchanged": len(full.unchanged),
        "scoped_unchanged": len(scoped.unchanged),
        "missing_unchanged": len(full.unchanged_keys - scoped.unchanged_keys),
        "extra_unchanged": len(scoped.unchanged_keys - full.unchanged_keys),
        "full_upsert": len(full.upsert),
        "scoped_upsert": len(scoped.upsert),
        "missing_upsert": len(full.upsert_keys - scoped.upsert_keys),
        "extra_upsert": len(scoped.upsert_keys - full.upsert_keys),
        "full_delete": len(full.delete),
        "scoped_delete": len(scoped.delete),
        "missing_delete": len(full.delete_keys - scoped.delete_keys),
        "extra_delete": len(scoped.delete_keys - full.delete_keys),
        "full_superseded": len(full.superseded),
        "scoped_superseded": len(scoped.superseded),
        "missing_superseded": len(full_superseded - scoped_superseded),
        "extra_superseded": len(scoped_superseded - full_superseded),
    }


def _fingerprint_inputs_id(*, tenant_id: str, user_id: str, forest_version: str) -> str:
    return f"{tenant_id}/{user_id}/{forest_version}.fingerprint.json"


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def _is_conflict(exc: Exception) -> bool:
    return exc.__class__.__name__ in {"ResourceExistsError", "CosmosResourceExistsError"} or getattr(exc, "status_code", None) == 409


def _node_fact_ids(tree: Any, node_id: str) -> list[str]:
    node = tree.nodes[node_id]
    if node.level != 0:
        return []
    if tree.tree_type == "session":
        fact_ids: list[str] = []
        for cell_id in node.child_ids:
            leaf = tree.session_leaves.get(cell_id)
            if leaf is not None:
                fact_ids.extend(leaf.fact_ids)
        return fact_ids
    return list(node.child_ids)


def _snapshot_payload(
    snapshot: ForestMaterializationSnapshot,
    *,
    tenant_id: str,
    user_id: str,
    forest_version: str,
    generation: int,
    embedding_model_version: str,
) -> bytes:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "forest_version": forest_version,
        "generation": generation,
        "embedding_model_version": embedding_model_version,
        "fact_count": len(snapshot.facts),
        "tree_count": len(snapshot.trees),
        "node_count": sum(len(tree.nodes) for tree in snapshot.trees),
        "summary_cache_keys": sorted(snapshot.summary_cache),
        "tree_ids": sorted(tree.tree_id for tree in snapshot.trees),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def content_hash_for_fact_ids(fact_ids: list[str]) -> str:
    return _content_key(frozenset(fact_ids))


def _canonical_level(level: Any) -> str:
    value = str(level)
    if value == "root":
        return "root"
    if value.startswith("L"):
        return value
    return f"L{value}"


def _is_factless_snapshot(snapshot: ForestMaterializationSnapshot) -> bool:
    return not snapshot.facts and not any(_tree_has_fact_payload(tree) for tree in snapshot.trees)


def _record_extract_heartbeat(telemetry: dict[str, Any]) -> dict[str, Any]:
    snapshot = snapshot_model_telemetry(telemetry)
    return update_model_telemetry_fields(
        telemetry,
        {
            "extract_build_heartbeat_count": int(snapshot.get("extract_build_heartbeat_count", 0)) + 1,
            "extract_build_last_heartbeat_unix": time.time(),
        },
    )


def _telemetry_progress_signature(telemetry: dict[str, Any]) -> dict[str, int]:
    telemetry = snapshot_model_telemetry(telemetry)
    calls = telemetry.get("calls") if isinstance(telemetry, dict) else {}
    embedded_items = 0
    if isinstance(calls, dict):
        for entry in calls.values():
            if isinstance(entry, dict) and entry.get("operation") == "embedding":
                embedded_items += int(entry.get("item_count") or 0)
    return {
        "completed_model_calls": int(telemetry.get("total_calls") or 0),
        "embedded_items": embedded_items,
        "durable_writes": int(telemetry.get("durable_write_count") or 0),
    }


def _extractable_turns(turns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return turns eligible for memory extraction.

    Source storage may include assistant transcript turns for UI reload. Any
    rebuild/backfill path that replays source turns into ingest_core must apply
    this same filter to avoid extracting assistant copy as durable user memory.
    """
    return [turn for turn in turns if not ((turn.get("metadata") or {}).get("skip_extraction"))]


def _is_content_filter_error(exc: Exception) -> bool:
    text = str(exc)
    return "content_filter" in text or "ResponsibleAIPolicyViolation" in text


async def _purge_factory_container(factory: Any, envelope: WriteJobEnvelope) -> int:
    if factory is None:
        return 0
    try:
        store = factory("__purge__")
    except TypeError:
        try:
            store = factory("__purge__", "1")
        except TypeError:
            return 0
    return await _purge_container_by_tenant_user_key(store, envelope)


async def _purge_container_by_tenant_user_key(store: Any, envelope: WriteJobEnvelope) -> int:
    container = getattr(store, "container", None)
    if container is None:
        return 0
    tenant_user_key = f"{envelope.tenant_id}:{envelope.user_id}"
    query = "SELECT c.id FROM c WHERE c.tenant_user_key=@pk"
    params = [{"name": "@pk", "value": tenant_user_key}]
    return await _purge_container_query(container, query=query, params=params, partition_key=tenant_user_key)


async def _purge_container_by_tenant_user(store: Any, envelope: WriteJobEnvelope) -> int:
    container = getattr(store, "container", None)
    if container is None:
        return 0
    query = "SELECT c.id FROM c WHERE c.tenant_id=@tenant_id AND c.user_id=@user_id"
    params = [{"name": "@tenant_id", "value": envelope.tenant_id}, {"name": "@user_id", "value": envelope.user_id}]
    return await _purge_container_query(container, query=query, params=params, partition_key=envelope.tenant_id)


async def _purge_container_query(container: Any, *, query: str, params: list[dict[str, Any]], partition_key: str) -> int:
    ids = []
    async for item in _aiter(container.query_items(query=query, parameters=params, partition_key=partition_key)):
        ids.append(item["id"])
    for chunk in _chunks(ids, 50):
        await asyncio.gather(*[
            _maybe_await(container.delete_item(item=item_id, partition_key=partition_key))
            for item_id in chunk
        ])
    return len(ids)


def _chunks(values: list[Any], size: int):
    for index in range(0, len(values), size):
        yield values[index:index + size]


async def _aiter(value):
    if hasattr(value, "__aiter__"):
        async for item in value:
            yield item
        return
    for item in value:
        yield item


def _tree_has_fact_payload(tree: Any) -> bool:
    if getattr(tree, "fact_ids_ordered", None):
        return True
    return any(bool(leaf.fact_ids) for leaf in getattr(tree, "session_leaves", {}).values())
