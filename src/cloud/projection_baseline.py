"""Read-side helpers for committed projection baselines.

The helpers in this module only describe already committed durable records.
They do not delete records and are not wired into live materialization skips.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from src.cloud.projection_plan import ProjectionRecordKey, ProjectionRecordRef, browse_embedding_content_hash, tree_node_body_content_id
from src.cloud.schema import partition_key


@dataclass(frozen=True, slots=True)
class ProjectionBaselineScope:
    tenant_id: str
    user_id: str
    forest_version: str
    embedding_model_version: str = ""

    def __post_init__(self) -> None:
        for field_name in ("tenant_id", "user_id", "forest_version"):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"{field_name} is required")
        object.__setattr__(self, "tenant_id", str(self.tenant_id))
        object.__setattr__(self, "user_id", str(self.user_id))
        object.__setattr__(self, "forest_version", str(self.forest_version))
        object.__setattr__(self, "embedding_model_version", str(self.embedding_model_version or ""))

    @classmethod
    def from_manifest(cls, manifest: Any, *, embedding_model_version: str = "") -> "ProjectionBaselineScope":
        status = str(getattr(manifest, "status", "") or "")
        if status != "committed":
            raise ValueError("Projection baseline requires a committed manifest.")
        return cls(
            tenant_id=str(getattr(manifest, "tenant_id", "") or ""),
            user_id=str(getattr(manifest, "user_id", "") or ""),
            forest_version=str(getattr(manifest, "forest_version", "") or ""),
            embedding_model_version=embedding_model_version,
        )


@dataclass(frozen=True, slots=True)
class CommittedProjectionBaseline:
    scope: ProjectionBaselineScope
    records: Mapping[str, tuple[ProjectionRecordRef, ...]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        normalized = {
            str(kind): _unique_sorted_records(values, label=f"records[{kind!r}]")
            for kind, values in dict(self.records).items()
        }
        object.__setattr__(self, "records", MappingProxyType(normalized))

    @property
    def forest_version(self) -> str:
        return self.scope.forest_version

    def records_for(self, record_kind: str) -> tuple[ProjectionRecordRef, ...]:
        return self.records.get(record_kind, ())

    def as_retained_committed_records(self) -> dict[str, tuple[ProjectionRecordRef, ...]]:
        return {kind: records for kind, records in self.records.items()}

    def with_records(self, record_kind: str, records: Iterable[ProjectionRecordRef]) -> "CommittedProjectionBaseline":
        merged = self.as_retained_committed_records()
        merged[record_kind] = tuple(records)
        return CommittedProjectionBaseline(scope=self.scope, records=merged)


def baseline_from_committed_docs(
    scope: ProjectionBaselineScope,
    *,
    tree_node_docs: Iterable[Mapping[str, Any]] = (),
    fact_location_docs: Iterable[Mapping[str, Any]] = (),
    recall_document_docs: Iterable[Mapping[str, Any]] = (),
    browse_embedding_docs: Iterable[Mapping[str, Any]] = (),
) -> CommittedProjectionBaseline:
    """Build per-record refs from already committed durable documents."""

    records = {
        "tree_node_membership": tuple(_tree_node_refs(scope, tree_node_docs)),
        "fact_location": tuple(_fact_location_refs(scope, fact_location_docs)),
        "recall_document": tuple(_recall_document_refs(scope, recall_document_docs)),
        "browse_embedding": tuple(_browse_embedding_refs(scope, browse_embedding_docs)),
    }
    return CommittedProjectionBaseline(scope=scope, records=records)


def summary_cache_refs_from_entries(entries: Mapping[str, str]) -> tuple[ProjectionRecordRef, ...]:
    """Build committed refs for content-addressed summary cache entries."""

    return _unique_sorted_records(
        (
            ProjectionRecordRef(
                "summary_cache",
                str(content_hash),
                content_hash=_summary_sha256(str(summary)),
            )
            for content_hash, summary in entries.items()
        ),
        label="summary_cache_entries",
    )


async def build_committed_projection_baseline(
    scope: ProjectionBaselineScope,
    *,
    tree_nodes_container: Any | None = None,
    fact_locations_container: Any | None = None,
    recall_search_client: Any | None = None,
    browse_embeddings_container: Any | None = None,
    limit: int = 10000,
) -> CommittedProjectionBaseline:
    """Read committed docs from durable read-side stores and return refs.

    Callers must pass a scope produced from a committed manifest or an otherwise
    trusted concrete forest_version. The function only reads records.
    """

    tree_node_docs = await _query_cosmos_docs(
        tree_nodes_container,
        scope,
        "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.forest_version=@forest_version",
        limit=limit,
    )
    fact_location_docs = await _query_cosmos_docs(
        fact_locations_container,
        scope,
        "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.forest_version=@forest_version",
        limit=limit,
    )
    browse_embedding_docs = await _query_cosmos_docs(
        browse_embeddings_container,
        scope,
        (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk "
            "AND c.embedding_model_version=@embedding_model_version"
            if scope.embedding_model_version
            else "SELECT * FROM c WHERE c.tenant_user_key=@pk"
        ),
        extra_params=(
            [{"name": "@embedding_model_version", "value": scope.embedding_model_version}]
            if scope.embedding_model_version
            else None
        ),
        include_forest_version=False,
        limit=limit,
    )
    recall_document_docs = await _query_recall_docs(recall_search_client, scope, limit=limit)
    return baseline_from_committed_docs(
        scope,
        tree_node_docs=tree_node_docs,
        fact_location_docs=fact_location_docs,
        recall_document_docs=recall_document_docs,
        browse_embedding_docs=browse_embedding_docs,
    )


def _tree_node_refs(scope: ProjectionBaselineScope, docs: Iterable[Mapping[str, Any]]) -> Iterable[ProjectionRecordRef]:
    for raw in docs:
        doc = dict(raw)
        if _skip_doc(scope, doc) or doc.get("doc_kind") != "tree_node_membership":
            continue
        tree_id = _required(doc, "tree_id", "tree_node_membership")
        node_id = _required(doc, "node_id", "tree_node_membership")
        content_hash = _required(doc, "content_sha256", "tree_node_membership")
        yield ProjectionRecordRef(
            "tree_node_membership",
            f"{tree_id}/{node_id}",
            content_hash=content_hash,
            content_id=str(doc.get("body_id") or tree_node_body_content_id(tree_id=tree_id, node_id=node_id, content_sha256=content_hash)),
            metadata={"tree_id": tree_id, "node_id": node_id, "forest_version": scope.forest_version},
        )


def _fact_location_refs(scope: ProjectionBaselineScope, docs: Iterable[Mapping[str, Any]]) -> Iterable[ProjectionRecordRef]:
    for raw in docs:
        doc = dict(raw)
        if _skip_doc(scope, doc):
            continue
        if doc.get("doc_kind") not in {None, "", "fact_location"}:
            continue
        fact_id = _required(doc, "fact_id", "fact_location")
        tree_id = _required(doc, "tree_id", "fact_location")
        node_id = _required(doc, "node_id", "fact_location")
        tree_type = _required(doc, "tree_type", "fact_location")
        level = _required(doc, "level", "fact_location")
        yield ProjectionRecordRef(
            "fact_location",
            f"{fact_id}/{tree_id}/{node_id}/{tree_type}/{level}",
            content_hash=_required(doc, "content_sha256", "fact_location"),
            metadata={"tree_id": tree_id, "node_id": node_id, "fact_id": fact_id, "forest_version": scope.forest_version},
        )


def _recall_document_refs(scope: ProjectionBaselineScope, docs: Iterable[Mapping[str, Any]]) -> Iterable[ProjectionRecordRef]:
    for raw in docs:
        doc = dict(raw)
        if _skip_doc(scope, doc) or _skip_embedding_model(scope, doc):
            continue
        tree_id = _required(doc, "tree_id", "recall_document")
        node_id = _required(doc, "node_id", "recall_document")
        yield ProjectionRecordRef(
            "recall_document",
            f"{tree_id}/{node_id}",
            content_hash=_required(doc, "content_sha256", "recall_document"),
            metadata={"tree_id": tree_id, "node_id": node_id, "forest_version": scope.forest_version},
        )


def _browse_embedding_refs(scope: ProjectionBaselineScope, docs: Iterable[Mapping[str, Any]]) -> Iterable[ProjectionRecordRef]:
    for raw in docs:
        doc = dict(raw)
        if _skip_doc(scope, doc, require_forest_version=False) or _skip_embedding_model(scope, doc):
            continue
        node_id = _required(doc, "node_id", "browse_embedding")
        model_version = _required(doc, "embedding_model_version", "browse_embedding")
        content_hash = str(doc.get("embedding_sha256") or "")
        if not content_hash and "embedding" in doc:
            content_hash = _embedding_sha256(tuple(float(value) for value in doc.get("embedding", ())))
        if not content_hash:
            raise ValueError("browse_embedding doc missing embedding_sha256")
        yield ProjectionRecordRef(
            "browse_embedding",
            f"{node_id}/{model_version}",
            content_hash=browse_embedding_content_hash(
                node_id=node_id,
                tree_id=str(doc.get("tree_id") or ""),
                tree_type=str(doc.get("tree_type") or ""),
                embedding_sha256=content_hash,
            ),
            metadata={
                "tree_id": str(doc.get("tree_id") or ""),
                "node_id": node_id,
                "forest_version": scope.forest_version,
            },
        )


def _skip_doc(scope: ProjectionBaselineScope, doc: Mapping[str, Any], *, require_forest_version: bool = True) -> bool:
    if doc.get("tombstoned"):
        return True
    in_scope = (
        str(doc.get("tenant_id") or "") == scope.tenant_id
        and str(doc.get("user_id") or "") == scope.user_id
    )
    if not in_scope:
        return True
    if require_forest_version and str(doc.get("forest_version") or "") != scope.forest_version:
        return True
    return False


def _skip_embedding_model(scope: ProjectionBaselineScope, doc: Mapping[str, Any]) -> bool:
    return bool(scope.embedding_model_version) and str(doc.get("embedding_model_version") or "") != scope.embedding_model_version


def _required(doc: Mapping[str, Any], field_name: str, record_kind: str) -> str:
    value = str(doc.get(field_name) or "").strip()
    if not value:
        raise ValueError(f"{record_kind} doc missing {field_name}")
    return value


def _unique_sorted_records(records: Iterable[ProjectionRecordRef], *, label: str) -> tuple[ProjectionRecordRef, ...]:
    indexed: dict[ProjectionRecordKey, ProjectionRecordRef] = {}
    for record in records:
        if not record.content_id and not record.content_hash:
            raise ValueError(f"{label} record {record.key!r} needs content_id or content_hash")
        if record.key in indexed:
            raise ValueError(f"{label} contains duplicate record key {record.key!r}")
        indexed[record.key] = record
    return tuple(sorted(indexed.values(), key=lambda item: (item.record_kind, item.logical_id)))


def _embedding_sha256(embedding: tuple[float, ...]) -> str:
    raw = ",".join(repr(float(value)) for value in embedding)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _summary_sha256(summary: str) -> str:
    return hashlib.sha256(summary.encode("utf-8")).hexdigest()


async def _query_cosmos_docs(
    container: Any | None,
    scope: ProjectionBaselineScope,
    query: str,
    *,
    extra_params: list[dict[str, Any]] | None = None,
    include_forest_version: bool = True,
    limit: int,
) -> list[dict[str, Any]]:
    if container is None:
        return []
    params = [
        {"name": "@pk", "value": partition_key(tenant_id=scope.tenant_id, user_id=scope.user_id)},
    ]
    if include_forest_version:
        params.append({"name": "@forest_version", "value": scope.forest_version})
    params.extend(extra_params or [])
    docs = []
    values = await _maybe_await(
        container.query_items(query=query, parameters=params, partition_key=partition_key(tenant_id=scope.tenant_id, user_id=scope.user_id))
    )
    async for item in _aiter(values):
        docs.append(dict(item))
        if len(docs) >= limit:
            break
    return docs


async def _query_recall_docs(search_client: Any | None, scope: ProjectionBaselineScope, *, limit: int) -> list[dict[str, Any]]:
    if search_client is None:
        return []
    clauses = [
        f"tenant_id eq '{_escape_filter_value(scope.tenant_id)}'",
        f"user_id eq '{_escape_filter_value(scope.user_id)}'",
        f"forest_version eq '{_escape_filter_value(scope.forest_version)}'",
    ]
    if scope.embedding_model_version:
        clauses.append(f"embedding_model_version eq '{_escape_filter_value(scope.embedding_model_version)}'")
    results = await _maybe_await(
        search_client.search(
            search_text="*",
            filter=" and ".join(clauses),
            select=["tenant_id", "user_id", "tree_id", "node_id", "forest_version", "embedding_model_version", "content_sha256"],
            top=limit,
        )
    )
    return [dict(item) async for item in _aiter(results)]


def _escape_filter_value(value: str) -> str:
    return value.replace("'", "''")


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


async def _aiter(values: Any):
    if hasattr(values, "__aiter__"):
        async for item in values:
            yield item
    else:
        for item in values:
            yield item
