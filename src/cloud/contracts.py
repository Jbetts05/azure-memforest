"""Typed contracts for future Azure-backed MemForest adapters.

These protocols intentionally avoid Azure SDK dependencies. Phase 0 defines
the seams without changing the current local FAISS/file behavior.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping, Protocol

from src.cloud.schema import SCHEMA_VERSION


@dataclass(frozen=True)
class RecallCandidate:
    """One recall result returned by a managed or local recall index."""

    tree_id: str
    node_id: str
    score: float
    tree_type: str | None = None
    tenant_id: str | None = None
    user_id: str | None = None
    forest_version: str | None = None
    embedding_model_version: str | None = None
    schema_version: int = SCHEMA_VERSION
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))


@dataclass(frozen=True)
class RecallDocument:
    """One write-side recall document projected into a recall index."""

    tree_id: str
    node_id: str
    tenant_id: str
    user_id: str
    tree_type: str
    forest_version: str
    embedding_model_version: str
    summary: str
    embedding: tuple[float, ...]
    schema_version: int = SCHEMA_VERSION
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "embedding", tuple(self.embedding))
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))


@dataclass(frozen=True)
class StoredTreeNode:
    """Cloud-portable tree node payload for random-access browsing."""

    tree_id: str
    node_id: str
    level: str
    summary: str
    child_ids: tuple[str, ...] = field(default_factory=tuple)
    fact_ids: tuple[str, ...] = field(default_factory=tuple)
    tenant_id: str | None = None
    user_id: str | None = None
    forest_version: str | None = None
    schema_version: int = SCHEMA_VERSION
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "child_ids", tuple(self.child_ids))
        object.__setattr__(self, "fact_ids", tuple(self.fact_ids))
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))


@dataclass(frozen=True)
class FactLocation:
    """Durable fact-to-tree location used for scalable bottom-up recall."""

    fact_id: str
    tree_id: str
    node_id: str
    tree_type: str
    level: str
    tenant_id: str | None = None
    user_id: str | None = None
    forest_version: str | None = None
    schema_version: int = SCHEMA_VERSION
    time_start: float | None = None
    time_end: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))


@dataclass(frozen=True)
class Lease:
    """Fenced write lease for serialized user/tenant mutations."""

    lease_id: str
    owner_id: str
    resource_id: str
    fencing_token: str
    expires_at_unix: float


class ModelClient(Protocol):
    """Model abstraction for chat/json and embedding deployments."""

    def generate_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model_name: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        top_p: float | None = None,
        timeout: float | None = None,
        step_label: str = "llm",
        trace: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ...

    def embed_texts(self, texts: list[str], *, batch_size: int = 256) -> list[list[float]]:
        ...


class FactStore(Protocol):
    """Canonical fact source-of-truth contract."""

    def get_fact(self, fact_id: str) -> Any | None:
        ...

    def get_facts(self, fact_ids: list[str]) -> list[Any]:
        ...

    def upsert_facts(self, facts: list[Any]) -> None:
        ...

    def tombstone_facts(self, fact_ids: list[str], *, reason: str) -> None:
        ...


class TreeNodeStore(Protocol):
    """Random-access tree-node storage used by browsing."""

    def get_node(self, tree_id: str, node_id: str) -> StoredTreeNode | None:
        ...

    def get_nodes(self, tree_id: str, node_ids: list[str]) -> list[StoredTreeNode]:
        ...

    def upsert_nodes(self, nodes: list[StoredTreeNode]) -> None:
        ...

    def tombstone_tree(self, tree_id: str, *, reason: str) -> None:
        ...


class TreeSnapshotStore(Protocol):
    """Versioned tree/forest snapshot storage for archive and restore."""

    def write_snapshot(self, *, snapshot_id: str, payload: bytes, metadata: dict[str, Any]) -> None:
        ...

    def read_snapshot(self, snapshot_id: str) -> bytes:
        ...


class RecallIndex(Protocol):
    """Searchable recall projection over tree roots, nodes, or facts."""

    def search(self, query_embedding: list[float], *, top_k: int, filters: dict[str, Any] | None = None) -> list[RecallCandidate]:
        ...

    def upsert_documents(self, documents: list[RecallDocument]) -> None:
        ...

    def delete_by_tree(self, tree_id: str, *, tenant_id: str | None = None) -> None:
        ...


class BrowseEmbeddingStore(Protocol):
    """Embedding lookup store for browse-time node scoring."""

    def get_embedding(self, node_id: str) -> list[float] | None:
        ...

    def get_embeddings(self, node_ids: list[str]) -> dict[str, list[float]]:
        ...

    def upsert_embeddings(self, embeddings: dict[str, list[float]], *, model_version: str) -> None:
        ...


class FactLocationIndex(Protocol):
    """Durable inverted index for fact-to-tree back-projection.

    Implementations may be scoped to one tenant/user or multi-tenant. Within a
    single implementation scope, upserting the same logical location replaces
    the prior record. The logical key is fact_id, tree_id, node_id, tree_type,
    level, tenant_id, user_id, and forest_version.
    """

    def get_locations(self, fact_ids: list[str]) -> dict[str, list[FactLocation]]:
        """Return locations for requested facts.

        Missing fact IDs may be absent from the returned mapping; callers should
        treat `result.get(fact_id, [])` as the portable lookup shape. Returned
        lists must be independent of internal storage.
        """
        ...

    def upsert_locations(self, locations: list[FactLocation]) -> None:
        """Insert or replace fact locations by logical location key."""
        ...

    def delete_locations_for_tree(self, tree_id: str) -> None:
        """Remove all locations for a tree within the implementation scope."""
        ...


class SummaryCache(Protocol):
    """Content-hash summary cache contract."""

    def get(self, content_hash: str) -> str | None:
        """Return cached summary text, or None when absent."""
        ...

    def put(self, content_hash: str, summary: str, *, metadata: dict[str, Any] | None = None) -> None:
        """Store or replace a summary for a content hash."""
        ...

    def delete(self, content_hash: str) -> None:
        """Delete a cached summary. Deleting an absent key is a no-op."""
        ...


class TraceSink(Protocol):
    """Structured trace sink for query and retrieval explainability."""

    def emit(self, event: Any) -> None:
        ...


class LeaseManager(Protocol):
    """Lease manager for serialized mutable operations."""

    def acquire(self, resource_id: str, *, owner_id: str, ttl_seconds: int) -> Lease:
        ...

    def renew(self, lease: Lease, *, ttl_seconds: int) -> Lease:
        ...

    def release(self, lease: Lease) -> None:
        ...


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(value))
