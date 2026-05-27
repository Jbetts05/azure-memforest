"""Local adapters that expose current MemForest components through cloud contracts.

These adapters are intentionally conservative. They make existing local
FAISS/file-backed behavior visible behind Azure-ready interfaces without
changing the canonical local mutation paths.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from src.cloud.contracts import FactLocation, RecallCandidate, StoredTreeNode

if TYPE_CHECKING:
    from src.build.node_index import NodeIndex
    from src.build.tree_store import TreeStore


class NodeIndexRecallAdapter:
    """Read-oriented RecallIndex adapter over the existing local NodeIndex."""

    def __init__(self, node_index: "NodeIndex") -> None:
        self._node_index = node_index

    def search(
        self,
        query_embedding: list[float],
        *,
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[RecallCandidate]:
        filters = filters or {}
        _validate_supported_filters(filters)
        query = _normalize(query_embedding)
        if filters:
            candidates = [
                candidate for candidate in (
                    self._candidate_from_entry(entry, query)
                    for entry in self._node_index.all_entries()
                    if _entry_matches_filters(entry, filters)
                )
                if candidate is not None
            ]
            candidates.sort(key=lambda candidate: candidate.score, reverse=True)
            return candidates[:top_k]

        entries = self._node_index.search(query_embedding, top_n=top_k)
        return [
            candidate for candidate in (
                self._candidate_from_entry(entry, query) for entry in entries
            )
            if candidate is not None
        ]

    def _candidate_from_entry(self, entry: Any, query: list[float]) -> RecallCandidate | None:
        embedding = self._node_index.get_embedding(entry.node_id)
        if embedding is None:
            return None
        return RecallCandidate(
            tree_id=entry.tree_id,
            node_id=entry.node_id,
            score=_dot(query, embedding),
            metadata={
                "level": _canonical_level(entry.level),
                "score_source": "local_node_index_dot",
            },
        )

    def upsert_candidates(self, candidates: list[RecallCandidate]) -> None:
        raise NotImplementedError(
            "Local recall writes must use NodeIndex.add_node(entry, embedding, searchable=True)."
        )

    def delete_by_tree(self, tree_id: str) -> None:
        self._node_index.remove_tree(tree_id)


class NodeIndexBrowseEmbeddingAdapter:
    """BrowseEmbeddingStore adapter over NodeIndex's browse embedding store."""

    def __init__(self, node_index: "NodeIndex") -> None:
        self._node_index = node_index

    def get_embedding(self, node_id: str) -> list[float] | None:
        return self._node_index.get_embedding(node_id)

    def get_embeddings(self, node_ids: list[str]) -> dict[str, list[float]]:
        embeddings: dict[str, list[float]] = {}
        for node_id in node_ids:
            embedding = self._node_index.get_embedding(node_id)
            if embedding is not None:
                embeddings[node_id] = embedding
        return embeddings

    def upsert_embeddings(self, embeddings: dict[str, list[float]], *, model_version: str) -> None:
        raise NotImplementedError(
            "Local browse embedding writes must use NodeIndex.add_node with NodeEntry metadata."
        )


class TreeStoreNodeAdapter:
    """Read-oriented TreeNodeStore adapter over loaded local MemTrees."""

    def __init__(self, tree_store: "TreeStore") -> None:
        self._tree_store = tree_store

    def get_node(self, tree_id: str, node_id: str) -> StoredTreeNode | None:
        tree = self._tree_store.get(tree_id)
        if tree is None:
            return None
        node = tree.nodes.get(node_id)
        if node is None:
            return None
        fact_ids = tuple(node.child_ids) if node.level == 0 and tree.tree_type != "session" else ()
        return StoredTreeNode(
            tree_id=tree.tree_id,
            node_id=node.node_id,
            level=_canonical_level(node.level),
            summary=node.summary,
            child_ids=tuple(node.child_ids),
            fact_ids=fact_ids,
            metadata={
                "tree_type": tree.tree_type,
                "tree_label": tree.label,
                "time_start": node.time_start,
                "time_end": node.time_end,
                "item_count": node.item_count,
                "parent_id": node.parent_id,
            },
        )

    def get_nodes(self, tree_id: str, node_ids: list[str]) -> list[StoredTreeNode]:
        nodes: list[StoredTreeNode] = []
        for node_id in node_ids:
            node = self.get_node(tree_id, node_id)
            if node is not None:
                nodes.append(node)
        return nodes

    def upsert_nodes(self, nodes: list[StoredTreeNode]) -> None:
        raise NotImplementedError(
            "Local tree writes must use TreeStore.register/save_tree with MemTree objects."
        )

    def tombstone_tree(self, tree_id: str, *, reason: str) -> None:
        raise NotImplementedError(
            "Local tree tombstones must be implemented through MemTree lifecycle/deletion semantics."
        )


class FactManagerStoreAdapter:
    """Read-oriented FactStore adapter over objects with FactManager-like APIs."""

    def __init__(self, fact_manager: Any) -> None:
        self._fact_manager = fact_manager

    def get_fact(self, fact_id: str) -> Any | None:
        return self._fact_manager.get_fact(fact_id)

    def get_facts(self, fact_ids: list[str]) -> list[Any]:
        facts: list[Any] = []
        for fact_id in fact_ids:
            fact = self.get_fact(fact_id)
            if fact is not None:
                facts.append(fact)
        return facts

    def upsert_facts(self, facts: list[Any]) -> None:
        raise NotImplementedError(
            "Local fact writes must use FactManager.add_memory_items/add_session_result for dedupe."
        )

    def tombstone_facts(self, fact_ids: list[str], *, reason: str) -> None:
        raise NotImplementedError("Local fact tombstones are not available through FactManagerStoreAdapter.")


class InMemoryFactLocationIndex:
    """Simple FactLocationIndex implementation for tests and local prototypes."""

    def __init__(self) -> None:
        self._locations_by_fact: dict[str, list[FactLocation]] = {}

    def get_locations(self, fact_ids: list[str]) -> dict[str, list[FactLocation]]:
        return {
            fact_id: list(self._locations_by_fact.get(fact_id, ()))
            for fact_id in fact_ids
        }

    def upsert_locations(self, locations: list[FactLocation]) -> None:
        for location in locations:
            existing = self._locations_by_fact.setdefault(location.fact_id, [])
            existing = [
                item for item in existing
                if not (
                    item.tree_id == location.tree_id
                    and item.node_id == location.node_id
                    and item.level == location.level
                )
            ]
            existing.append(location)
            self._locations_by_fact[location.fact_id] = existing

    def delete_locations_for_tree(self, tree_id: str) -> None:
        for fact_id, locations in list(self._locations_by_fact.items()):
            kept = [location for location in locations if location.tree_id != tree_id]
            if kept:
                self._locations_by_fact[fact_id] = kept
            else:
                del self._locations_by_fact[fact_id]


class InMemorySummaryCache:
    """Simple SummaryCache implementation for local tests and development."""

    def __init__(self) -> None:
        self._summaries: dict[str, str] = {}
        self._metadata: dict[str, dict[str, Any]] = {}

    def get(self, content_hash: str) -> str | None:
        return self._summaries.get(content_hash)

    def put(self, content_hash: str, summary: str, *, metadata: dict[str, Any] | None = None) -> None:
        self._summaries[content_hash] = summary
        self._metadata[content_hash] = dict(metadata or {})

    def delete(self, content_hash: str) -> None:
        self._summaries.pop(content_hash, None)
        self._metadata.pop(content_hash, None)


def _entry_matches_filters(entry: Any, filters: dict[str, Any]) -> bool:
    _validate_supported_filters(filters)
    if "tree_id" in filters and entry.tree_id != filters["tree_id"]:
        return False
    if "tree_ids" in filters and entry.tree_id not in set(filters["tree_ids"]):
        return False
    if "level" in filters and _canonical_level(entry.level) != _canonical_level(filters["level"]):
        return False
    return True


def _validate_supported_filters(filters: dict[str, Any]) -> None:
    supported = {"tree_id", "tree_ids", "level"}
    unsupported = set(filters) - supported
    if unsupported:
        raise ValueError(f"Unsupported local recall filters: {sorted(unsupported)}")


def _normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in vec))
    if norm < 1e-9:
        return list(vec)
    return [x / norm for x in vec]


def _dot(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def _canonical_level(level: Any) -> str:
    value = str(level)
    if value == "root":
        return "root"
    if value.startswith("L"):
        return value
    return f"L{value}"
