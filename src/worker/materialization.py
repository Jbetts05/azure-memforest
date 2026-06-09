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
from typing import Any, Protocol

from src.cloud.async_batch import run_bounded
from src.cloud.cosmos_browse_embeddings import BrowseEmbeddingRecord
from src.cloud.contracts import FactLocation, RecallDocument, StoredTreeNode
from src.cloud.cosmos_leases import LeaseConflict
from src.cloud.schema import SCHEMA_VERSION
from src.forest.memforest import _content_key
from src.forest.user_forest import ForestMaterializationSnapshot
from src.service.jobs import WriteJobEnvelope
from src.service.schemas import IngestSessionRequest
from src.worker.consumer import PermanentHandlerError, TerminalHandlerError, TransientHandlerError
from src.observability.model_telemetry import collect_model_telemetry, snapshot_model_telemetry, update_model_telemetry_fields
from src.worker.progress import JobProgressBudget, budget_for_envelope, budget_from_document

log = logging.getLogger(__name__)


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
            self._call_store_method(self._tree_node_store(forest_version).upsert_nodes, projection.stored_nodes),
            self._call_store_method(self._fact_location_index(forest_version).upsert_locations, projection.fact_locations),
            self._write_browse_embeddings(forest_version, projection),
            self._write_summary_cache_entries(projection),
            self._write_recall_documents(projection),
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
        if self._stores.snapshot_store is not None:
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

    async def _write_browse_embeddings(self, forest_version: str, projection: MaterializationProjection) -> None:
        browse_store = self._browse_embedding_store(forest_version)
        if browse_store is None:
            return
        if hasattr(browse_store, "upsert_records"):
            await self._call_store_method(
                browse_store.upsert_records,
                projection.browse_embedding_records,
                model_version=self._config.embedding_model_version,
            )
            return
        await self._call_store_method(
            browse_store.upsert_embeddings,
            projection.browse_embeddings,
            model_version=self._config.embedding_model_version,
        )

    async def _write_summary_cache_entries(self, projection: MaterializationProjection) -> None:
        if self._stores.summary_cache is None:
            return

        async def _put(entry: SummaryCacheEntry) -> None:
            await self._call_store_method(
                self._stores.summary_cache.put,
                entry.content_hash,
                entry.summary,
                metadata=entry.metadata,
            )

        await run_bounded(projection.summary_cache_entries, _put)

    async def _write_recall_documents(self, projection: MaterializationProjection) -> None:
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

    return MaterializationProjection(
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


def _parse_ingest_request(envelope: WriteJobEnvelope) -> IngestSessionRequest:
    try:
        return IngestSessionRequest.model_validate(envelope.payload)
    except Exception as exc:
        raise PermanentHandlerError("invalid ingest_session payload") from exc


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
    }, inputs


def _hash_json(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


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
