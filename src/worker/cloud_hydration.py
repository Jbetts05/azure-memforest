"""Read-side cloud state hydration helpers.

This module is intentionally not wired into the write hot path yet.  It proves
that committed tree structure, canonical facts, and content-hash summaries can
be reconstructed from cloud state, which is a prerequisite for demoting Blob
snapshots from every ingest.
"""

from __future__ import annotations

import inspect
import time
from dataclasses import dataclass, field
from typing import Any

from src.build.tree_types import MemTree, MemTreeNode
from src.forest.memforest import _content_key


@dataclass(frozen=True, slots=True)
class CloudHydrationPrerequisites:
    """Unwired state required before a Blob-free forest hydrate can be safe."""

    trees: list[MemTree]
    facts: list[Any] = field(default_factory=list)
    summary_cache: dict[str, str] = field(default_factory=dict)
    missing_fact_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_summary_cache_keys: tuple[str, ...] = field(default_factory=tuple)


async def load_memtrees_from_tree_store(tree_store: Any) -> list[MemTree]:
    """Load committed MemTree structures from a cloud tree-node store."""

    tree_ids = await tree_store.list_tree_ids()
    trees: list[MemTree] = []
    for tree_id in tree_ids:
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
        root_node_id = _root_node_id(nodes)
        root = nodes[root_node_id]
        metadata = dict(next((node.metadata for node in stored_nodes if node.node_id == root_node_id), {}) or {})
        trees.append(
            MemTree(
                tree_id=tree_id,
                tree_type=str(metadata.get("tree_type") or "entity"),
                tree_key=str(metadata.get("tree_key") or tree_id),
                label=str(metadata.get("tree_label") or metadata.get("tree_key") or tree_id),
                k=int(metadata.get("k") or 3),
                root_node_id=root_node_id,
                nodes=nodes,
                session_leaves={},
                fact_ids_ordered=_ordered_leaf_payloads(nodes),
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
) -> CloudHydrationPrerequisites:
    """Load cloud hydration prerequisite state without changing hot-path behavior."""

    trees = await load_memtrees_from_tree_store(tree_store)
    facts: list[Any] = []
    missing_fact_ids: tuple[str, ...] = ()
    if fact_store is not None:
        fact_ids = await load_fact_ids_from_tree_store(tree_store)
        facts, missing_fact_ids = await load_facts_by_id(fact_store, fact_ids)

    summaries: dict[str, str] = {}
    missing_summary_keys: tuple[str, ...] = ()
    if summary_cache is not None:
        summaries, missing_summary_keys = await load_summary_cache_for_memtrees(summary_cache, trees)

    return CloudHydrationPrerequisites(
        trees=trees,
        facts=facts,
        summary_cache=summaries,
        missing_fact_ids=missing_fact_ids,
        missing_summary_cache_keys=missing_summary_keys,
    )


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


def _ordered_leaf_payloads(nodes: dict[str, MemTreeNode]) -> list[str]:
    leaves = sorted((node for node in nodes.values() if node.level == 0), key=lambda node: (node.time_start, node.time_end, node.node_id))
    payloads: list[str] = []
    for leaf in leaves:
        payloads.extend(leaf.child_ids)
    return payloads


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


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
