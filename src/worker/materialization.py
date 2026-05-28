"""Projection helpers for cloud write materialization."""

from __future__ import annotations

import json
import asyncio
import contextlib
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

from src.cloud.cosmos_browse_embeddings import BrowseEmbeddingRecord
from src.cloud.contracts import FactLocation, RecallDocument, StoredTreeNode
from src.cloud.cosmos_leases import LeaseConflict
from src.cloud.schema import SCHEMA_VERSION
from src.forest.memforest import _content_key
from src.forest.user_forest import ForestMaterializationSnapshot
from src.service.jobs import WriteJobEnvelope
from src.service.schemas import IngestSessionRequest
from src.worker.consumer import PermanentHandlerError, TransientHandlerError

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
    lease_ttl_seconds: int = 300
    lease_renewal_seconds: float = 60.0


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
        if envelope.job_type != "ingest_session":
            raise PermanentHandlerError(f"unsupported job_type {envelope.job_type!r}")
        await self._handle_ingest(envelope)

    async def _handle_ingest(self, envelope: WriteJobEnvelope) -> None:
        request = _parse_ingest_request(envelope)
        if await self._latest_committed_matches_job(envelope):
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
            if await self._latest_committed_matches_job(envelope):
                return
            latest = await _maybe_await(self._stores.manifest_store.latest_committed())
            await self._hydrate_core_from_manifest(latest)
            generation = 1 if latest is None else int(latest.generation) + 1
            forest_version = _forest_version(generation=generation, job_id=envelope.job_id)
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
                        "fencing_token": lease.fencing_token,
                    },
                )
            )
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

            snapshot = await asyncio.to_thread(
                self._ingest_core.ingest_session,
                session_id=envelope.session_id,
                turns=turns,
                metadata=request.metadata,
            )
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
            try:
                await self._stores.lease_store.assert_current(lease)
            except LeaseConflict as exc:
                raise TransientHandlerError("tenant/user write lease expired before manifest commit") from exc
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
        await _maybe_await(self._stores.fact_store.upsert_facts(snapshot.facts))
        await _maybe_await(self._tree_node_store(forest_version).upsert_nodes(projection.stored_nodes))
        await _maybe_await(self._fact_location_index(forest_version).upsert_locations(projection.fact_locations))
        browse_store = self._browse_embedding_store(forest_version)
        if browse_store is not None:
            if hasattr(browse_store, "upsert_records"):
                await _maybe_await(
                    browse_store.upsert_records(
                        projection.browse_embedding_records,
                        model_version=self._config.embedding_model_version,
                    )
                )
            else:
                await _maybe_await(
                    browse_store.upsert_embeddings(
                        projection.browse_embeddings,
                        model_version=self._config.embedding_model_version,
                    )
                )
        if self._stores.summary_cache is not None:
            for entry in projection.summary_cache_entries:
                await _maybe_await(
                    self._stores.summary_cache.put(
                        entry.content_hash,
                        entry.summary,
                        metadata=entry.metadata,
                    )
                )
        await _maybe_await(self._stores.recall_index.upsert_documents(projection.recall_documents))
        metadata: dict[str, Any] = {}
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
                "snapshot_id": snapshot_id,
                "snapshot_content_sha256": snapshot_hash,
                "snapshot_size_bytes": len(snapshot_payload),
            }
        return metadata

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
