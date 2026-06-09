"""Read-side cloud state hydration helpers.

This module is intentionally not wired into the write hot path yet.  It proves
that committed tree structure can be reconstructed from cloud tree-node state,
which is a prerequisite for demoting Blob snapshots from every ingest.
"""

from __future__ import annotations

import time
from typing import Any

from src.build.tree_types import MemTree, MemTreeNode


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
                child_ids=list(node.child_ids),
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


def _level_int(level: str | int) -> int:
    text = str(level)
    if text.upper().startswith("L"):
        text = text[1:]
    return int(text)


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

