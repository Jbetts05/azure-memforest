"""Read-side cloud state hydration helpers.

This module is intentionally not wired into the write hot path yet.  It proves
that committed tree structure, canonical facts, and content-hash summaries can
be reconstructed from cloud state, which is a prerequisite for demoting Blob
snapshots from every ingest.
"""

from __future__ import annotations

import inspect
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import faiss
import numpy as np

from src.build.node_index import NodeEntry, NodeIndex
from src.build.tree import validate_tree_structure
from src.build.tree_types import MemTree, MemTreeNode, SessionLeaf
from src.forest.memforest import _content_key
from src.forest.session_registry import CellRecord, SessionRecord, TurnRecord
from src.utils.types import FactOccurrence, ManagedFact

try:
    from src.worker.snapshot_hydration import restore_user_forest_from_manifest
except ImportError:  # pragma: no cover - defensive for partial imports
    restore_user_forest_from_manifest = None


@dataclass(frozen=True, slots=True)
class CloudHydrationPrerequisites:
    """Unwired state required before a Blob-free forest hydrate can be safe."""

    trees: list[MemTree]
    facts: list[Any] = field(default_factory=list)
    summary_cache: dict[str, str] = field(default_factory=dict)
    missing_fact_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_summary_cache_keys: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class CloudUserForestState:
    """Cloud state sufficient to seed a local ``UserForest`` read/query graph."""

    trees: list[MemTree]
    facts: list[ManagedFact] = field(default_factory=list)
    summary_cache: dict[str, str] = field(default_factory=dict)
    node_embeddings: dict[str, list[float]] = field(default_factory=dict)
    missing_fact_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_summary_cache_keys: tuple[str, ...] = field(default_factory=tuple)
    missing_node_embedding_ids: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class CloudHydrationApplyResult:
    """Result of applying cloud state to a local ``UserForest``."""

    tree_count: int
    fact_count: int
    summary_cache_count: int
    node_embedding_count: int
    missing_fact_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_summary_cache_keys: tuple[str, ...] = field(default_factory=tuple)
    missing_node_embedding_ids: tuple[str, ...] = field(default_factory=tuple)
    used_snapshot_fallback: bool = False


class CloudStateHydratingUserForestCore:
    """Gated worker core that can hydrate from cloud state, with Blob fallback.

    The feature is disabled unless ``enable_cloud_state_hydration`` is true, so
    the existing archive snapshot path remains the default production behavior.
    """

    def __init__(
        self,
        *,
        user_forest: Any,
        tree_store: Any | None = None,
        tree_store_factory: Any | None = None,
        fact_store: Any | None = None,
        summary_cache: Any | None = None,
        browse_embedding_store: Any | None = None,
        browse_embedding_store_factory: Any | None = None,
        snapshot_store: Any | None = None,
        enable_cloud_state_hydration: bool = False,
        fallback_to_snapshot: bool = True,
        cloud_state_read_attempts: int = 2,
    ) -> None:
        self.user_forest = user_forest
        self.tree_store = tree_store
        self.tree_store_factory = tree_store_factory
        self.fact_store = fact_store
        self.summary_cache = summary_cache
        self.browse_embedding_store = browse_embedding_store
        self.browse_embedding_store_factory = browse_embedding_store_factory
        self.snapshot_store = snapshot_store
        self.enable_cloud_state_hydration = bool(enable_cloud_state_hydration)
        self.fallback_to_snapshot = bool(fallback_to_snapshot)
        self.cloud_state_read_attempts = max(1, int(cloud_state_read_attempts))
        self.last_hydration_result: CloudHydrationApplyResult | None = None

    async def hydrate_from_manifest(self, manifest: Any, *, tree_ids: list[str] | tuple[str, ...] | set[str] | None = None) -> None:
        """Hydrate local state from a committed manifest.

        ``tree_ids`` filters which cloud trees are loaded, but this method still
        resets the local ``UserForest`` before applying state. Passing a subset
        therefore replaces the resident forest with that subset; it is not an
        additive merge operation.
        """

        if not self.enable_cloud_state_hydration:
            self.last_hydration_result = self._snapshot_fallback(manifest)
            return
        try:
            forest_version = _manifest_forest_version(manifest)
            tree_store = _store_for_manifest_version(self.tree_store, self.tree_store_factory, forest_version)
            browse_embedding_store = _store_for_manifest_version(self.browse_embedding_store, self.browse_embedding_store_factory, forest_version)
            if tree_store is None:
                raise CloudStateHydrationIncomplete("tree_store is required for cloud state hydration")
            state = await self._load_cloud_state_with_retry(
                tree_store=tree_store,
                browse_embedding_store=browse_embedding_store,
                tree_ids=tree_ids,
            )
            if _cloud_state_missing_prerequisites(state):
                raise CloudStateHydrationIncomplete("cloud state hydration is incomplete")
            self.last_hydration_result = apply_cloud_state_to_user_forest(self.user_forest, state)
            self.user_forest._record_projection_baseline(forest_version)
        except Exception:
            if not self.fallback_to_snapshot or not _manifest_has_snapshot(manifest):
                raise
            self.last_hydration_result = self._snapshot_fallback(manifest)

    async def merge_from_manifest(self, manifest: Any, *, tree_ids: list[str] | tuple[str, ...] | set[str]) -> None:
        """Merge a requested tree subset from a committed manifest into resident state.

        This is an explicit partial-hydration entry point. It never falls back to
        snapshots because snapshot restore is whole-forest, not partial.
        """

        if not self.enable_cloud_state_hydration:
            raise CloudStateHydrationIncomplete("cloud state hydration is disabled")
        requested_tree_ids = tuple(str(tree_id) for tree_id in tree_ids if str(tree_id))
        if not requested_tree_ids:
            raise ValueError("tree_ids must contain at least one tree id for partial merge hydration")
        forest_version = _manifest_forest_version(manifest)
        tree_store = _store_for_manifest_version(self.tree_store, self.tree_store_factory, forest_version)
        browse_embedding_store = _store_for_manifest_version(self.browse_embedding_store, self.browse_embedding_store_factory, forest_version)
        if tree_store is None:
            raise CloudStateHydrationIncomplete("tree_store is required for partial cloud state hydration")
        state = await self._load_cloud_state_with_retry(
            tree_store=tree_store,
            browse_embedding_store=browse_embedding_store,
            tree_ids=requested_tree_ids,
            fact_scope="all",
        )
        if _cloud_state_missing_prerequisites(state):
            raise CloudStateHydrationIncomplete("partial cloud state hydration is incomplete")
        self.last_hydration_result = merge_cloud_state_into_user_forest(self.user_forest, state)
        self.user_forest._mark_projection_delta_unknown()

    async def check_partial_hydration_from_manifest(self, manifest: Any, *, tree_ids: list[str] | tuple[str, ...] | set[str]) -> dict[str, Any]:
        """Load a requested tree subset and report completeness without applying it."""

        if not self.enable_cloud_state_hydration:
            return {"supported": False, "reason": "cloud_state_hydration_disabled"}
        requested_tree_ids = tuple(str(tree_id) for tree_id in tree_ids if str(tree_id))
        if not requested_tree_ids:
            return {
                "supported": True,
                "complete": True,
                "tree_count": 0,
                "fact_count": 0,
                "summary_cache_count": 0,
                "node_embedding_count": 0,
                "missing_fact_count": 0,
                "missing_summary_cache_count": 0,
                "missing_node_embedding_count": 0,
            }
        forest_version = _manifest_forest_version(manifest)
        tree_store = _store_for_manifest_version(self.tree_store, self.tree_store_factory, forest_version)
        browse_embedding_store = _store_for_manifest_version(self.browse_embedding_store, self.browse_embedding_store_factory, forest_version)
        if tree_store is None:
            return {"supported": False, "reason": "tree_store_missing"}
        state = await self._load_cloud_state_with_retry(
            tree_store=tree_store,
            browse_embedding_store=browse_embedding_store,
            tree_ids=requested_tree_ids,
            fact_scope="all",
        )
        return {
            "supported": True,
            "complete": not _cloud_state_missing_prerequisites(state),
            "tree_count": len(state.trees),
            "fact_count": len(state.facts),
            "summary_cache_count": len(state.summary_cache),
            "node_embedding_count": len(state.node_embeddings),
            "missing_fact_count": len(state.missing_fact_ids),
            "missing_summary_cache_count": len(state.missing_summary_cache_keys),
            "missing_node_embedding_count": len(state.missing_node_embedding_ids),
        }

    async def partial_hydration_shadow_snapshot_from_manifest(
        self,
        manifest: Any,
        *,
        tree_ids: list[str] | tuple[str, ...] | set[str],
        session_id: str,
        turns: list[dict[str, Any]],
        metadata: dict[str, Any],
    ) -> Any:
        """Run partial hydrate plus ingest in an isolated forest and return its snapshot."""

        if not self.enable_cloud_state_hydration:
            raise CloudStateHydrationIncomplete("cloud state hydration is disabled")
        requested_tree_ids = tuple(str(tree_id) for tree_id in tree_ids if str(tree_id))
        if not requested_tree_ids:
            raise ValueError("tree_ids must contain at least one tree id for partial hydration shadow")
        from src.forest.user_forest import UserForest

        with tempfile.TemporaryDirectory(prefix="azure-memforest-partial-shadow-") as tmp_dir:
            shadow_forest = UserForest(
                self.user_forest.user_id,
                Path(tmp_dir) / self.user_forest.user_id,
                self.user_forest._config,
                chat_client=self.user_forest._chat_client,
                embedding_client=self.user_forest._embedding_client,
            )
            shadow_core = CloudStateHydratingUserForestCore(
                user_forest=shadow_forest,
                tree_store=self.tree_store,
                tree_store_factory=self.tree_store_factory,
                fact_store=self.fact_store,
                summary_cache=self.summary_cache,
                browse_embedding_store=self.browse_embedding_store,
                browse_embedding_store_factory=self.browse_embedding_store_factory,
                snapshot_store=None,
                enable_cloud_state_hydration=True,
                fallback_to_snapshot=False,
                cloud_state_read_attempts=self.cloud_state_read_attempts,
            )
            await shadow_core.merge_from_manifest(manifest, tree_ids=requested_tree_ids)
            return shadow_core.ingest_session(session_id=session_id, turns=turns, metadata=metadata)

    def ingest_session(self, *, session_id: str, turns: list[dict[str, Any]], metadata: dict[str, Any]) -> Any:
        self.user_forest.ingest_session(session_id, turns)
        return self.user_forest.snapshot_for_materialization()

    def prepare_pre_ingest_route_prediction(self, *, session_id: str, turns: list[dict[str, Any]], route_index_manifest: dict, embedding_model_version: str) -> Any:
        return self.user_forest.prepare_pre_ingest_route_prediction(
            session_id,
            turns,
            route_index_manifest=route_index_manifest,
            embedding_model_version=embedding_model_version,
        )

    def finalize_pre_ingest_route_prediction(self, prediction: Any) -> Any:
        self.user_forest.finalize_pre_ingest_route_prediction(prediction)
        return self.user_forest.snapshot_for_materialization()

    def delete_session(self, *, session_id: str) -> Any:
        self.user_forest.delete_session(session_id)
        return self.user_forest.snapshot_for_materialization()

    def snapshot_for_materialization(self) -> Any:
        return self.user_forest.snapshot_for_materialization()

    def snapshot_payload(self) -> bytes:
        from src.worker.snapshot_hydration import build_user_forest_archive

        return build_user_forest_archive(self.user_forest)

    async def _load_cloud_state_with_retry(
        self,
        *,
        tree_store: Any,
        browse_embedding_store: Any | None,
        tree_ids: list[str] | tuple[str, ...] | set[str] | None = None,
        fact_scope: str = "tree_refs",
    ) -> CloudUserForestState:
        last_error: Exception | None = None
        for _attempt in range(self.cloud_state_read_attempts):
            try:
                return await load_cloud_user_forest_state(
                    tree_store=tree_store,
                    fact_store=self.fact_store,
                    summary_cache=self.summary_cache,
                    browse_embedding_store=browse_embedding_store,
                    tree_ids=tree_ids,
                    fact_scope=fact_scope,
                )
            except Exception as exc:
                last_error = exc
        assert last_error is not None
        raise last_error

    def _snapshot_fallback(self, manifest: Any) -> CloudHydrationApplyResult:
        if self.snapshot_store is None or restore_user_forest_from_manifest is None:
            raise ValueError("snapshot_store is required when cloud hydration falls back to Blob snapshots")
        restore_user_forest_from_manifest(self.user_forest, self.snapshot_store, manifest)
        return CloudHydrationApplyResult(
            tree_count=0,
            fact_count=0,
            summary_cache_count=0,
            node_embedding_count=0,
            used_snapshot_fallback=True,
        )


class CloudStateHydrationIncomplete(ValueError):
    """Cloud state is readable but missing required facts, summaries, or embeddings."""


def _cloud_state_missing_prerequisites(state: CloudUserForestState) -> bool:
    # Node summaries are persisted on tree nodes; the content-hash cache is a
    # rebuild optimization and can self-heal when affected nodes are dirtied.
    return bool(state.missing_fact_ids or state.missing_node_embedding_ids)


def _manifest_forest_version(manifest: Any) -> str:
    forest_version = str(getattr(manifest, "forest_version", "") or "")
    if forest_version:
        return forest_version
    metadata = getattr(manifest, "metadata", {}) or {}
    forest_version = str(metadata.get("forest_version") or "")
    if not forest_version:
        raise ValueError("manifest forest_version is required for cloud state hydration")
    return forest_version


def _store_for_manifest_version(store: Any | None, factory: Any | None, forest_version: str) -> Any | None:
    if factory is not None:
        return factory(forest_version)
    return store


def _manifest_has_snapshot(manifest: Any) -> bool:
    metadata = getattr(manifest, "metadata", {}) or {}
    return bool(metadata.get("snapshot_id") and metadata.get("snapshot_content_sha256"))


async def load_memtrees_from_tree_store(tree_store: Any, *, tree_ids: list[str] | tuple[str, ...] | set[str] | None = None) -> list[MemTree]:
    """Load committed MemTree structures from a cloud tree-node store.

    ``tree_ids=None`` loads all tree IDs from the store. An explicit iterable is
    a filter: duplicate IDs are collapsed into sorted canonical order, and an
    empty iterable intentionally loads no trees.
    """

    if tree_ids is None:
        tree_ids_to_load = await tree_store.list_tree_ids()
    else:
        tree_ids_to_load = tuple(sorted({str(tree_id) for tree_id in tree_ids if str(tree_id)}))
    trees: list[MemTree] = []
    for tree_id in tree_ids_to_load:
        stored_nodes = await tree_store.list_nodes_in_tree(tree_id, limit=100000)
        if not stored_nodes:
            continue
        nodes = {
            node.node_id: MemTreeNode(
                node_id=node.node_id,
                tree_id=node.tree_id,
                level=_level_int(node.level),
                time_start=float((node.metadata or {}).get("time_start") or 0.0),
                time_end=float((node.metadata or {}).get("time_end") or 0.0),
                summary=node.summary,
                summary_dirty=False,
                child_ids=_node_child_ids(node),
                item_count=int((node.metadata or {}).get("item_count") or len(node.fact_ids) or len(node.child_ids)),
                parent_id=(node.metadata or {}).get("parent_id"),
            )
            for node in stored_nodes
        }
        _dedupe_duplicate_child_links(nodes)
        _link_l0_leaves(nodes)
        root_node_id = _root_node_id(nodes)
        root = nodes[root_node_id]
        metadata = dict(next((node.metadata for node in stored_nodes if node.node_id == root_node_id), {}) or {})
        tree_type = str(metadata.get("tree_type") or "entity")
        session_leaves = _session_leaves_from_nodes(nodes, stored_nodes, tree_id=tree_id, tree_key=str(metadata.get("tree_key") or tree_id)) if tree_type == "session" else {}
        trees.append(
            MemTree(
                tree_id=tree_id,
                tree_type=tree_type,
                tree_key=str(metadata.get("tree_key") or tree_id),
                label=str(metadata.get("tree_label") or metadata.get("tree_key") or tree_id),
                k=_tree_fanout_k(metadata, nodes),
                root_node_id=root_node_id,
                nodes=nodes,
                session_leaves=session_leaves,
                fact_ids_ordered=[] if tree_type == "session" else _ordered_leaf_payloads(nodes),
                centroid=None,
                version=int(metadata.get("generation") or 1),
                lifecycle_state="active",
                deactivated=False,
                replacement_tree_ids=[],
                created_at=float(metadata.get("created_at") or time.time()),
                updated_at=float(metadata.get("updated_at") or time.time()),
            )
        )
        _refresh_tree_bounds(trees[-1], root)
    return trees


async def load_facts_for_memtrees(fact_store: Any, trees: list[MemTree]) -> tuple[list[Any], tuple[str, ...]]:
    """Load canonical facts referenced by hydrated trees.

    The returned fact list preserves first-seen tree/leaf order and does not
    mutate a local ``FactManager``. That final wiring remains intentionally out
    of the worker hot path until cloud hydration can replace Blob snapshots
    end-to-end.
    """

    fact_ids = _ordered_unique_payload_ids(trees)
    return await load_facts_by_id(fact_store, fact_ids)


async def load_fact_ids_from_tree_store(tree_store: Any) -> list[str]:
    """Return ordered unique canonical fact IDs from cloud tree-node records."""

    tree_ids = await tree_store.list_tree_ids()
    seen: set[str] = set()
    ordered: list[str] = []
    for tree_id in tree_ids:
        stored_nodes = await tree_store.list_nodes_in_tree(tree_id, limit=100000)
        leaves = sorted(
            (node for node in stored_nodes if _level_int(node.level) == 0),
            key=lambda node: (
                float((node.metadata or {}).get("time_start") or 0.0),
                float((node.metadata or {}).get("time_end") or 0.0),
                node.node_id,
            ),
        )
        for leaf in leaves:
            payload_ids = list(getattr(leaf, "fact_ids", ()) or getattr(leaf, "child_ids", ()) or ())
            for fact_id in payload_ids:
                if fact_id in seen:
                    continue
                seen.add(fact_id)
                ordered.append(fact_id)
    return ordered


async def load_facts_by_id(fact_store: Any, fact_ids: list[str]) -> tuple[list[Any], tuple[str, ...]]:
    """Load canonical facts by ID while preserving the requested order."""

    if not fact_ids:
        return [], ()
    if hasattr(fact_store, "get_facts"):
        facts = list(await _maybe_await(fact_store.get_facts(fact_ids)))
    elif hasattr(fact_store, "get_fact"):
        facts = []
        for fact_id in fact_ids:
            fact = await _maybe_await(fact_store.get_fact(fact_id))
            if fact is not None:
                facts.append(fact)
    else:
        raise TypeError("fact_store must provide get_facts(fact_ids) or get_fact(fact_id)")

    facts_by_id = {_fact_id(fact): fact for fact in facts if _fact_id(fact)}
    ordered_facts = [facts_by_id[fact_id] for fact_id in fact_ids if fact_id in facts_by_id]
    missing = tuple(fact_id for fact_id in fact_ids if fact_id not in facts_by_id)
    return ordered_facts, missing


async def load_all_facts(fact_store: Any) -> list[Any]:
    """Load all non-tombstoned canonical facts for the tenant/user."""

    if hasattr(fact_store, "list_facts"):
        return list(await _maybe_await(fact_store.list_facts()))
    if hasattr(fact_store, "iter_facts"):
        return list(await _maybe_await(fact_store.iter_facts()))
    raise TypeError("fact_store must provide list_facts() or iter_facts() for global canonical fact hydration")


async def load_summary_cache_for_memtrees(summary_cache: Any, trees: list[MemTree]) -> tuple[dict[str, str], tuple[str, ...]]:
    """Load content-hash summaries needed by hydrated trees.

    Keys are derived from each internal node's descendant L0 payload IDs, using
    the same content hash as ``CachingTreeBuilder``. Missing keys are reported
    separately because this helper is a prerequisite check, not a production
    fallback path.
    """

    keys = _summary_cache_keys_for_memtrees(trees)
    if not keys:
        return {}, ()

    loaded: dict[str, str] = {}
    if hasattr(summary_cache, "get_many"):
        values = await _maybe_await(summary_cache.get_many(keys))
        loaded.update({str(key): str(value) for key, value in dict(values or {}).items() if value})
    elif hasattr(summary_cache, "get"):
        for key in keys:
            value = await _maybe_await(summary_cache.get(key))
            if value:
                loaded[key] = str(value)
    else:
        raise TypeError("summary_cache must provide get(content_hash) or get_many(content_hashes)")

    missing = tuple(key for key in keys if key not in loaded)
    return loaded, missing


async def load_cloud_hydration_prerequisites(
    *,
    tree_store: Any,
    fact_store: Any | None = None,
    summary_cache: Any | None = None,
    tree_ids: list[str] | tuple[str, ...] | set[str] | None = None,
    fact_scope: str = "tree_refs",
) -> CloudHydrationPrerequisites:
    """Load cloud hydration prerequisite state without changing hot-path behavior."""

    trees = await load_memtrees_from_tree_store(tree_store, tree_ids=tree_ids)
    facts: list[Any] = []
    missing_fact_ids: tuple[str, ...] = ()
    if fact_store is not None:
        if fact_scope == "all":
            facts = await load_all_facts(fact_store)
            referenced_fact_ids = _ordered_unique_payload_ids(trees)
            loaded_fact_ids = {_fact_id(fact) for fact in facts if _fact_id(fact)}
            missing_fact_ids = tuple(fact_id for fact_id in referenced_fact_ids if fact_id not in loaded_fact_ids)
        elif fact_scope == "tree_refs":
            fact_ids = _ordered_unique_payload_ids(trees)
            facts, missing_fact_ids = await load_facts_by_id(fact_store, fact_ids)
        else:
            raise ValueError(f"unsupported fact_scope {fact_scope!r}")
    else:
        missing_fact_ids = tuple(_ordered_unique_payload_ids(trees))

    summaries: dict[str, str] = {}
    missing_summary_keys: tuple[str, ...] = ()
    if summary_cache is not None:
        summaries, missing_summary_keys = await load_summary_cache_for_memtrees(summary_cache, trees)
    else:
        missing_summary_keys = tuple(_summary_cache_keys_for_memtrees(trees))

    return CloudHydrationPrerequisites(
        trees=trees,
        facts=facts,
        summary_cache=summaries,
        missing_fact_ids=missing_fact_ids,
        missing_summary_cache_keys=missing_summary_keys,
    )


async def load_cloud_user_forest_state(
    *,
    tree_store: Any,
    fact_store: Any | None = None,
    summary_cache: Any | None = None,
    browse_embedding_store: Any | None = None,
    tree_ids: list[str] | tuple[str, ...] | set[str] | None = None,
    fact_scope: str = "tree_refs",
) -> CloudUserForestState:
    """Load cloud state that can be applied to a local ``UserForest`` instance."""

    prerequisites = await load_cloud_hydration_prerequisites(
        tree_store=tree_store,
        fact_store=fact_store,
        summary_cache=summary_cache,
        tree_ids=tree_ids,
        fact_scope=fact_scope,
    )
    node_ids = _node_ids_requiring_embeddings(prerequisites.trees)
    node_embeddings: dict[str, list[float]] = {}
    if browse_embedding_store is not None and node_ids:
        if hasattr(browse_embedding_store, "get_embeddings"):
            values = await _maybe_await(browse_embedding_store.get_embeddings(node_ids))
            node_embeddings = {str(key): [float(v) for v in value] for key, value in dict(values or {}).items()}
        elif hasattr(browse_embedding_store, "get_embedding"):
            for node_id in node_ids:
                value = await _maybe_await(browse_embedding_store.get_embedding(node_id))
                if value is not None:
                    node_embeddings[node_id] = [float(v) for v in value]
        else:
            raise TypeError("browse_embedding_store must provide get_embedding(node_id) or get_embeddings(node_ids)")
    missing_node_ids = tuple(node_id for node_id in node_ids if node_id not in node_embeddings)
    return CloudUserForestState(
        trees=prerequisites.trees,
        facts=[_managed_fact_from_any(fact) for fact in prerequisites.facts],
        summary_cache=prerequisites.summary_cache,
        node_embeddings=node_embeddings,
        missing_fact_ids=prerequisites.missing_fact_ids,
        missing_summary_cache_keys=prerequisites.missing_summary_cache_keys,
        missing_node_embedding_ids=missing_node_ids,
    )


def apply_cloud_state_to_user_forest(user_forest: Any, state: CloudUserForestState) -> CloudHydrationApplyResult:
    """Seed a local ``UserForest`` from cloud trees, facts, summaries, and node embeddings."""

    lock = getattr(user_forest, "_lock", None)
    if lock is None:
        return _apply_cloud_state_locked(user_forest, state)
    with lock:
        return _apply_cloud_state_locked(user_forest, state)


def merge_cloud_state_into_user_forest(user_forest: Any, state: CloudUserForestState) -> CloudHydrationApplyResult:
    """Merge a partial cloud state slice into an existing local ``UserForest``.

    This helper is intentionally non-destructive: it replaces only the loaded
    tree IDs, adds/updates loaded facts and summaries, and refreshes node-index
    entries for the loaded trees. It does not remove facts that are absent from
    the partial slice; deletion semantics must be supplied by a future caller.
    """

    lock = getattr(user_forest, "_lock", None)
    if lock is None:
        return _merge_cloud_state_locked(user_forest, state)
    with lock:
        return _merge_cloud_state_locked(user_forest, state)


def _level_int(level: str | int) -> int:
    text = str(level)
    if text.upper().startswith("L"):
        text = text[1:]
    return int(text)


def _node_child_ids(stored_node: Any) -> list[str]:
    child_ids = list(getattr(stored_node, "child_ids", ()) or ())
    if child_ids or _level_int(getattr(stored_node, "level")) != 0:
        return child_ids
    return list(getattr(stored_node, "fact_ids", ()) or ())


def _root_node_id(nodes: dict[str, MemTreeNode]) -> str:
    roots = [node_id for node_id, node in nodes.items() if node.parent_id in {None, ""}]
    if len(roots) != 1:
        raise ValueError(f"expected exactly one root node, found {len(roots)}")
    return roots[0]


def _tree_fanout_k(metadata: dict[str, Any], nodes: dict[str, MemTreeNode]) -> int:
    configured = int(metadata.get("k") or 0)
    if configured > 0:
        return max(configured, 3)
    observed = max((len(node.child_ids) for node in nodes.values() if node.level > 0), default=0)
    return max(observed, 3)


def _dedupe_duplicate_child_links(nodes: dict[str, MemTreeNode]) -> None:
    levels = sorted({node.level for node in nodes.values() if node.level > 0})
    for level in levels:
        owners: dict[str, str] = {}
        parents = sorted(
            (node for node in nodes.values() if node.level == level),
            key=lambda node: _node_order_key(node.node_id),
        )
        for parent in parents:
            kept: list[str] = []
            for child_id in parent.child_ids:
                prior_parent_id = owners.get(child_id)
                if prior_parent_id and prior_parent_id in nodes:
                    prior = nodes[prior_parent_id]
                    prior.child_ids = [existing for existing in prior.child_ids if existing != child_id]
                    prior.summary_dirty = True
                owners[child_id] = parent.node_id
                if child_id not in kept:
                    kept.append(child_id)
            if kept != parent.child_ids:
                parent.child_ids = kept
                parent.summary_dirty = True
        for child_id, parent_id in owners.items():
            child = nodes.get(child_id)
            if child is not None and child.parent_id != parent_id:
                child.parent_id = parent_id
    _prune_empty_internal_nodes(nodes)
    _refresh_internal_metadata(nodes)


def _prune_empty_internal_nodes(nodes: dict[str, MemTreeNode]) -> None:
    changed = True
    while changed:
        changed = False
        for node_id, node in list(nodes.items()):
            if node.level <= 0 or node.child_ids or node.parent_id in {None, ""}:
                continue
            parent = nodes.get(node.parent_id)
            if parent is not None:
                parent.child_ids = [child_id for child_id in parent.child_ids if child_id != node_id]
                parent.summary_dirty = True
            del nodes[node_id]
            changed = True


def _refresh_internal_metadata(nodes: dict[str, MemTreeNode]) -> None:
    for level in sorted({node.level for node in nodes.values() if node.level > 0}):
        for node in [candidate for candidate in nodes.values() if candidate.level == level]:
            children = [nodes[child_id] for child_id in node.child_ids if child_id in nodes]
            if not children:
                node.item_count = 0
                continue
            node.item_count = sum(max(0, child.item_count) for child in children)
            node.time_start = min(child.time_start for child in children)
            node.time_end = max(child.time_end for child in children)


def _node_order_key(node_id: str) -> tuple[str, int, str]:
    prefix, sep, suffix = node_id.rpartition(":")
    if sep and suffix.isdigit():
        return (prefix, int(suffix), node_id)
    return (node_id, -1, node_id)


def _link_l0_leaves(nodes: dict[str, MemTreeNode]) -> None:
    leaves = sorted((node for node in nodes.values() if node.level == 0), key=lambda node: (node.time_start, node.time_end, node.child_ids[0] if node.child_ids else node.node_id))
    for index, leaf in enumerate(leaves):
        leaf.prev_leaf_id = leaves[index - 1].node_id if index > 0 else None
        leaf.next_leaf_id = leaves[index + 1].node_id if index + 1 < len(leaves) else None


def _ordered_leaf_payloads(nodes: dict[str, MemTreeNode]) -> list[str]:
    leaves = sorted((node for node in nodes.values() if node.level == 0), key=lambda node: (node.time_start, node.time_end, node.node_id))
    payloads: list[str] = []
    for leaf in leaves:
        payloads.extend(leaf.child_ids)
    return payloads


def _session_leaves_from_nodes(nodes: dict[str, MemTreeNode], stored_nodes: list[Any], *, tree_id: str, tree_key: str) -> dict[str, SessionLeaf]:
    stored_by_node_id = {str(getattr(node, "node_id", "")): node for node in stored_nodes}
    leaves: dict[str, SessionLeaf] = {}
    default_session_id = tree_key.removeprefix("session:") or tree_id.removeprefix("session:")
    for leaf in sorted((node for node in nodes.values() if node.level == 0), key=lambda node: (node.time_start, node.time_end, node.node_id)):
        if not leaf.child_ids:
            continue
        cell_id = str(leaf.child_ids[0])
        stored = stored_by_node_id.get(leaf.node_id)
        leaves[cell_id] = SessionLeaf(
            cell_id=cell_id,
            session_id=_session_id_from_cell_id(cell_id, default_session_id),
            cell_index=_cell_index_from_id(cell_id),
            time_start=leaf.time_start,
            time_end=leaf.time_end,
            raw_turns=[],
            raw_text=leaf.summary,
            fact_ids=list(getattr(stored, "fact_ids", ()) if stored is not None else ()),
        )
    return leaves


def _cell_index_from_id(cell_id: str) -> int:
    import re

    match = re.search(r"cell[_-](\d+)", str(cell_id))
    return int(match.group(1)) if match else 0


def _session_id_from_cell_id(cell_id: str, default_session_id: str) -> str:
    text = str(cell_id or "")
    if "#" in text:
        prefix = text.split("#", 1)[0]
        if prefix:
            return prefix
    return default_session_id


def _refresh_tree_bounds(tree: MemTree, root: MemTreeNode) -> None:
    if root.time_start or root.time_end:
        return
    leaves = [node for node in tree.nodes.values() if node.level == 0]
    if not leaves:
        return
    root.time_start = min(leaf.time_start for leaf in leaves)
    root.time_end = max(leaf.time_end for leaf in leaves)


def _ordered_unique_payload_ids(trees: list[MemTree]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for tree in trees:
        if tree.tree_type == "session":
            continue
        for payload_id in tree.fact_ids_ordered or _ordered_leaf_payloads(tree.nodes):
            if payload_id in seen:
                continue
            seen.add(payload_id)
            ordered.append(payload_id)
    return ordered


def _summary_cache_keys_for_memtrees(trees: list[MemTree]) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for tree in trees:
        for node in sorted(tree.nodes.values(), key=lambda item: (item.level, item.node_id)):
            if node.level == 0:
                continue
            descendants = _collect_descendant_payload_ids(tree, node)
            if not descendants:
                continue
            key = _content_key(frozenset(descendants))
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)
    return keys


def _collect_descendant_payload_ids(tree: MemTree, node: MemTreeNode) -> set[str]:
    payloads: set[str] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        if current.level == 0:
            payloads.update(current.child_ids)
            continue
        for child_id in current.child_ids:
            child = tree.nodes.get(child_id)
            if child is not None:
                stack.append(child)
    return payloads


def _fact_id(fact: Any) -> str:
    if isinstance(fact, dict):
        return str(fact.get("fact_id") or "")
    return str(getattr(fact, "fact_id", "") or "")


def _apply_cloud_state_locked(user_forest: Any, state: CloudUserForestState) -> CloudHydrationApplyResult:
    _reset_empty_user_forest(user_forest)
    _clear_fact_manager(user_forest._fact_manager)
    facts = [_managed_fact_from_any(fact) for fact in state.facts]
    _load_facts_into_fact_manager(user_forest._fact_manager, facts)
    user_forest._summary_cache = dict(state.summary_cache)
    user_forest._tree_builder.set_summary_cache(user_forest._summary_cache)
    user_forest._tree_builder._fact_catalog = {fact.fact_id: fact for fact in facts}

    for tree in state.trees:
        validate_tree_structure(tree)
        user_forest._tree_builder._trees[tree.tree_id] = tree
        user_forest._tree_store.register(tree)
    from src.forest.memforest import _populate_registry_from_trees

    _populate_registry_from_trees(user_forest._registry, user_forest._tree_store)
    _populate_session_alias_map(user_forest, state.trees)
    _populate_registry_from_fact_occurrences(user_forest._registry, facts)

    user_forest._node_index = _node_index_from_cloud_embeddings(
        user_forest,
        state.trees,
        state.node_embeddings,
    )
    user_forest._rewire_query_pipeline()
    user_forest._record_projection_delta(changed_tree_ids=set(), deleted_tree_ids=set())
    return CloudHydrationApplyResult(
        tree_count=len(state.trees),
        fact_count=len(facts),
        summary_cache_count=len(state.summary_cache),
        node_embedding_count=len(state.node_embeddings),
        missing_fact_ids=state.missing_fact_ids,
        missing_summary_cache_keys=state.missing_summary_cache_keys,
        missing_node_embedding_ids=state.missing_node_embedding_ids,
    )


def _merge_cloud_state_locked(user_forest: Any, state: CloudUserForestState) -> CloudHydrationApplyResult:
    facts = [_managed_fact_from_any(fact) for fact in state.facts]
    _upsert_facts_into_fact_manager(user_forest._fact_manager, facts)
    user_forest._summary_cache.update(dict(state.summary_cache))
    user_forest._tree_builder.set_summary_cache(user_forest._summary_cache)
    for fact in facts:
        user_forest._tree_builder._fact_catalog[fact.fact_id] = fact

    for tree in state.trees:
        validate_tree_structure(tree)
        user_forest._tree_builder._trees[tree.tree_id] = tree
        user_forest._tree_store.register(tree)
        user_forest._node_index.remove_tree(tree.tree_id)
    _add_cloud_embeddings_to_node_index(user_forest, state.trees, state.node_embeddings)

    _populate_registry_from_loaded_session_trees(user_forest._registry, state.trees)
    _merge_session_alias_map(user_forest, state.trees)
    _populate_registry_from_fact_occurrences(user_forest._registry, facts)
    user_forest._rewire_query_pipeline()
    user_forest._record_projection_delta(changed_tree_ids=set(), deleted_tree_ids=set())
    return CloudHydrationApplyResult(
        tree_count=len(state.trees),
        fact_count=len(facts),
        summary_cache_count=len(state.summary_cache),
        node_embedding_count=len(state.node_embeddings),
        missing_fact_ids=state.missing_fact_ids,
        missing_summary_cache_keys=state.missing_summary_cache_keys,
        missing_node_embedding_ids=state.missing_node_embedding_ids,
    )


def _clear_fact_manager(fact_manager: Any) -> None:
    with fact_manager._lock:
        fact_manager._facts = {}
        fact_manager._faiss_ids = []
        fact_manager._duplicate_records = []
        fact_manager._exact_index = {}
        fact_manager._index = faiss.IndexFlatIP(fact_manager.vector_dim)


def _populate_session_alias_map(user_forest: Any, trees: list[MemTree]) -> None:
        alias_map: dict[str, str] = {}
        for tree in trees:
            if tree.tree_type != "session":
                continue
            alias = str(tree.tree_key or tree.tree_id).removeprefix("session:")
            for leaf in tree.session_leaves.values():
                raw_session_id = str(getattr(leaf, "session_id", "") or "")
                if raw_session_id:
                    alias_map[raw_session_id] = alias
        user_forest._tree_builder._session_alias_map = alias_map


def _merge_session_alias_map(user_forest: Any, trees: list[MemTree]) -> None:
        alias_map = dict(getattr(user_forest._tree_builder, "_session_alias_map", {}) or {})
        for tree in trees:
            if tree.tree_type != "session":
                continue
            alias = str(tree.tree_key or tree.tree_id).removeprefix("session:")
            for leaf in tree.session_leaves.values():
                raw_session_id = str(getattr(leaf, "session_id", "") or "")
                if raw_session_id:
                    alias_map[raw_session_id] = alias
        user_forest._tree_builder._session_alias_map = alias_map


def _populate_registry_from_loaded_session_trees(registry: Any, trees: list[MemTree]) -> None:
        for tree in trees:
            if tree.tree_type != "session":
                continue
            cells_by_session: dict[str, list[str]] = {}
            cell_fact_ids_by_session: dict[str, dict[str, list[str]]] = {}
            for cell_id, leaf in tree.session_leaves.items():
                session_id = str(getattr(leaf, "session_id", "") or tree.tree_key or tree.tree_id).removeprefix("session:")
                cells_by_session.setdefault(session_id, []).append(cell_id)
                cell_fact_ids_by_session.setdefault(session_id, {})[cell_id] = list(getattr(leaf, "fact_ids", []))
            for session_id, cell_ids in cells_by_session.items():
                registry.register_synthetic_session(
                    session_id=session_id,
                    cell_ids=cell_ids,
                    cell_fact_ids=cell_fact_ids_by_session.get(session_id, {}),
                )


def _populate_registry_from_fact_occurrences(registry: Any, facts: list[ManagedFact]) -> None:
        with registry._lock:
            for fact in facts:
                for occurrence in fact.occurrences:
                    session_id = str(occurrence.session_id or "")
                    cell_id = str(occurrence.cell_id or "")
                    if not session_id or not cell_id:
                        continue
                    session = registry._sessions.get(session_id)
                    if session is None:
                        registry._sessions[session_id] = SessionRecord(session_id=session_id, cell_ids=[cell_id], deleted=False)
                    elif cell_id not in session.cell_ids:
                        registry._sessions[session_id] = SessionRecord(
                            session_id=session_id,
                            cell_ids=[*session.cell_ids, cell_id],
                            deleted=session.deleted,
                        )

                    turn_ids = [str(turn_id) for turn_id in occurrence.source_turn_ids if str(turn_id)]
                    cell = registry._cells.get(cell_id)
                    if cell is None:
                        registry._cells[cell_id] = CellRecord(cell_id=cell_id, session_id=session_id, turn_ids=turn_ids, fact_ids=[fact.fact_id])
                    else:
                        registry._cells[cell_id] = CellRecord(
                            cell_id=cell_id,
                            session_id=cell.session_id or session_id,
                            turn_ids=list(dict.fromkeys([*cell.turn_ids, *turn_ids])),
                            fact_ids=list(dict.fromkeys([*cell.fact_ids, fact.fact_id])),
                        )
                    for turn_id in turn_ids:
                        registry._turns[turn_id] = TurnRecord(turn_id=turn_id, session_id=session_id, cell_id=cell_id, deleted=False)
                    registry._fact_to_cells.setdefault(fact.fact_id, set()).add(cell_id)


def _reset_empty_user_forest(user_forest: Any) -> None:
    user_id = user_forest.user_id
    user_dir = user_forest._dir
    config = user_forest._config
    chat_client = user_forest._chat_client
    embedding_client = user_forest._embedding_client
    user_forest.__init__(
        user_id=user_id,
        user_dir=user_dir,
        config=config,
        chat_client=chat_client,
        embedding_client=embedding_client,
    )


def _load_facts_into_fact_manager(fact_manager: Any, facts: list[ManagedFact]) -> None:
    embedded: list[tuple[ManagedFact, list[float]]] = []
    missing_embeddings: list[ManagedFact] = []
    for fact in facts:
        embedding = _fact_embedding(fact)
        if embedding and len(embedding) == fact_manager.vector_dim:
            embedded.append((fact, embedding))
        else:
            missing_embeddings.append(fact)

    if missing_embeddings:
        fact_manager.load_managed_facts(missing_embeddings)

    if not embedded:
        return
    with fact_manager._lock:
        for fact, embedding in embedded:
            if fact.fact_id in fact_manager._facts:
                continue
            arr = np.asarray(embedding, dtype="float32")
            if arr.ndim != 1 or arr.shape[0] != fact_manager.vector_dim:
                raise ValueError(f"fact embedding dim for {fact.fact_id} does not match FactManager vector_dim")
            if fact_manager.normalize_embeddings:
                arr = fact_manager._normalize_vector(arr)
            fact_manager._facts[fact.fact_id] = fact
            fact_manager._faiss_ids.append(fact.fact_id)
            fact_manager._exact_index[fact_manager._normalize_fact_text(fact.fact_text)] = fact.fact_id
            fact_manager._index.add(arr.reshape(1, -1))


def _upsert_facts_into_fact_manager(fact_manager: Any, facts: list[ManagedFact]) -> None:
    fact_ids = {fact.fact_id for fact in facts}
    if fact_ids:
        fact_manager.delete_facts(fact_ids)
    _load_facts_into_fact_manager(fact_manager, facts)


def _node_index_from_cloud_embeddings(
    user_forest: Any,
    trees: list[MemTree],
    embeddings: dict[str, list[float]],
) -> NodeIndex:
    index = NodeIndex(index_dir=user_forest._dir / "node_index", vector_dim=user_forest._config.api.embedding.dimension)
    for tree in trees:
        root_id = tree.root_node_id
        root = tree.nodes.get(root_id)
        is_depth0_scene = tree.tree_type == "scene" and root is not None and root.level == 0
        for node_id, embedding in embeddings.items():
            node = tree.nodes.get(node_id)
            if node is None or node.level == 0:
                continue
            index.add_node(
                NodeEntry(node_id=node_id, tree_id=tree.tree_id, level=_node_index_level(tree, node)),
                embedding,
                searchable=(node_id == root_id and not is_depth0_scene),
            )
    return index


def _add_cloud_embeddings_to_node_index(
    user_forest: Any,
    trees: list[MemTree],
    embeddings: dict[str, list[float]],
) -> None:
    for tree in trees:
        root_id = tree.root_node_id
        root = tree.nodes.get(root_id)
        is_depth0_scene = tree.tree_type == "scene" and root is not None and root.level == 0
        for node_id, embedding in embeddings.items():
            node = tree.nodes.get(node_id)
            if node is None or node.level == 0:
                continue
            user_forest._node_index.add_node(
                NodeEntry(node_id=node_id, tree_id=tree.tree_id, level=_node_index_level(tree, node)),
                embedding,
                searchable=(node_id == root_id and not is_depth0_scene),
            )


def _node_index_level(tree: MemTree, node: MemTreeNode) -> str:
    if node.node_id == tree.root_node_id:
        return "root"
    return f"L{node.level}"


def _node_ids_requiring_embeddings(trees: list[MemTree]) -> list[str]:
    seen: set[str] = set()
    node_ids: list[str] = []
    for tree in trees:
        for node in tree.nodes.values():
            if node.level == 0 or not node.summary or node.node_id in seen:
                continue
            seen.add(node.node_id)
            node_ids.append(node.node_id)
    return node_ids


def _managed_fact_from_any(value: Any) -> ManagedFact:
    if isinstance(value, ManagedFact):
        return value
    if not isinstance(value, dict):
        raise TypeError("cloud fact records must be ManagedFact instances or dictionaries")
    data = dict(value)
    occurrences = [
        occurrence if isinstance(occurrence, FactOccurrence) else FactOccurrence(**dict(occurrence))
        for occurrence in data.get("occurrences", [])
    ]
    data["occurrences"] = occurrences
    return ManagedFact(
        fact_id=str(data["fact_id"]),
        fact_text=str(data.get("fact_text", "")),
        embedding_id=str(data.get("embedding_id", "")),
        origin=str(data.get("origin", "")),
        semantic_role=str(data.get("semantic_role", "")),
        entities=list(data.get("entities", [])),
        topics=list(data.get("topics", [])),
        attribute_keys=list(data.get("attribute_keys", [])),
        domain_keys=list(data.get("domain_keys", [])),
        collection_keys=list(data.get("collection_keys", [])),
        detail_level=str(data.get("detail_level", "")),
        confidence=float(data.get("confidence", 1.0)),
        first_session_id=str(data.get("first_session_id", "")),
        first_cell_id=str(data.get("first_cell_id", "")),
        occurrences=occurrences,
        metadata=dict(data.get("metadata", {})),
        time_start=data.get("time_start"),
        time_end=data.get("time_end"),
    )


def _fact_embedding(fact: ManagedFact) -> list[float] | None:
    value = dict(fact.metadata).get("embedding")
    if value is None:
        return None
    return [float(item) for item in value]


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
