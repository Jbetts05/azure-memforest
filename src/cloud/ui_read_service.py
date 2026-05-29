"""Read services for Azure-MemForest UI surfaces."""

from __future__ import annotations

from collections import deque
from typing import Any

from src.cloud.cosmos_facts import CosmosFactStore
from src.cloud.cosmos_manifests import CosmosManifestStore, ManifestNotFound
from src.cloud.cosmos_sessions import CosmosSessionStore, CosmosTurnStore
from src.cloud.cosmos_source import CosmosFactLocationIndex, CosmosTreeNodeStore
from src.service.auth import ActorContext
from src.service.schemas import (
    FactDetailResponse,
    ForestManifestResponse,
    SourceSessionResponse,
    SourceTurnResponse,
    SubtreeResponse,
    TreeListResponse,
    TreeNodeResponse,
)


class UiReadNotFound(Exception):
    pass


class CloudUiReadService:
    def __init__(self, database_client: Any) -> None:
        self._database = database_client

    async def list_sessions(self, actor: ActorContext, *, user_id: str, limit: int = 100) -> list[SourceSessionResponse]:
        store = self._session_store(actor, user_id)
        sessions = await store.list_sessions(limit=_clamp(limit, 1, 200))
        return [SourceSessionResponse(session_id=s.session_id, cell_ids=list(s.cell_ids), metadata=s.metadata) for s in sessions]

    async def get_session(self, actor: ActorContext, *, user_id: str, session_id: str) -> SourceSessionResponse:
        session = await self._session_store(actor, user_id).get_session(session_id)
        if session is None:
            raise UiReadNotFound()
        return SourceSessionResponse(session_id=session.session_id, cell_ids=list(session.cell_ids), metadata=session.metadata)

    async def list_turns(self, actor: ActorContext, *, user_id: str, session_id: str, limit: int = 100) -> list[SourceTurnResponse]:
        turns = await self._turn_store(actor, user_id).list_turns(session_id)
        return [
            SourceTurnResponse(
                turn_id=t.turn_id,
                session_id=t.session_id,
                turn_index=t.turn_index,
                role=t.role,
                content=t.content,
                timestamp=t.timestamp,
                content_id=t.content_id,
                metadata=t.metadata,
            )
            for t in turns[: _clamp(limit, 1, 500)]
        ]

    async def latest_manifest(self, actor: ActorContext, *, user_id: str) -> ForestManifestResponse:
        manifest = await self._manifest_store(actor, user_id).latest_committed()
        if manifest is None:
            raise UiReadNotFound()
        return _manifest_response(manifest)

    async def list_tree_roots(self, actor: ActorContext, *, user_id: str, forest_version: str, limit: int = 100) -> TreeListResponse:
        await self._require_committed_forest(actor, user_id, forest_version)
        roots = await self._tree_store(actor, user_id, forest_version).list_root_nodes(limit=_clamp(limit, 1, 200))
        return TreeListResponse(forest_version=forest_version, roots=[_node_response(node) for node in roots])

    async def get_tree_node(self, actor: ActorContext, *, user_id: str, forest_version: str, tree_id: str, node_id: str) -> TreeNodeResponse:
        await self._require_committed_forest(actor, user_id, forest_version)
        node = await self._tree_store(actor, user_id, forest_version).get_node(tree_id, node_id)
        if node is None:
            raise UiReadNotFound()
        return _node_response(node)

    async def get_subtree(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        forest_version: str,
        tree_id: str,
        root_node_id: str,
        max_depth: int = 2,
        max_nodes: int = 250,
    ) -> SubtreeResponse:
        await self._require_committed_forest(actor, user_id, forest_version)
        max_depth = _clamp(max_depth, 0, 6)
        max_nodes = _clamp(max_nodes, 1, 1000)
        all_nodes = await self._tree_store(actor, user_id, forest_version).list_nodes_in_tree(tree_id, limit=max_nodes * 4)
        by_id = {node.node_id: node for node in all_nodes}
        if root_node_id not in by_id:
            raise UiReadNotFound()
        out = []
        truncated = False
        queue = deque([(root_node_id, 0)])
        seen = set()
        while queue:
            node_id, depth = queue.popleft()
            if node_id in seen:
                continue
            node = by_id.get(node_id)
            if node is None:
                continue
            seen.add(node_id)
            out.append(node)
            if len(out) >= max_nodes:
                truncated = True
                break
            if depth < max_depth:
                for child_id in node.child_ids:
                    queue.append((child_id, depth + 1))
        return SubtreeResponse(
            forest_version=forest_version,
            tree_id=tree_id,
            root_node_id=root_node_id,
            truncated=truncated,
            nodes=[_node_response(node) for node in out],
        )

    async def get_fact(self, actor: ActorContext, *, user_id: str, forest_version: str, fact_id: str) -> FactDetailResponse:
        await self._require_committed_forest(actor, user_id, forest_version)
        fact = await self._fact_store(actor, user_id).get_fact(fact_id)
        if fact is None:
            raise UiReadNotFound()
        locations = await self._location_index(actor, user_id, forest_version).get_locations([fact_id])
        return FactDetailResponse(
            fact_id=fact.fact_id,
            text=fact.fact_text,
            time_start=fact.time_start,
            time_end=fact.time_end,
            metadata=dict(fact.metadata),
            locations=[loc.__dict__ for loc in locations.get(fact_id, [])],
        )

    async def _require_committed_forest(self, actor: ActorContext, user_id: str, forest_version: str) -> None:
        latest = await self._manifest_store(actor, user_id).latest_committed()
        if latest is None or latest.forest_version != forest_version or latest.status != "committed":
            raise UiReadNotFound()

    def _session_store(self, actor: ActorContext, user_id: str) -> CosmosSessionStore:
        return CosmosSessionStore(self._database.get_container_client("sessions"), tenant_id=actor.tenant.tenant_id, user_id=user_id)

    def _turn_store(self, actor: ActorContext, user_id: str) -> CosmosTurnStore:
        return CosmosTurnStore(self._database.get_container_client("turns"), tenant_id=actor.tenant.tenant_id, user_id=user_id)

    def _manifest_store(self, actor: ActorContext, user_id: str) -> CosmosManifestStore:
        return CosmosManifestStore(self._database.get_container_client("manifests"), tenant_id=actor.tenant.tenant_id, user_id=user_id)

    def _tree_store(self, actor: ActorContext, user_id: str, forest_version: str) -> CosmosTreeNodeStore:
        return CosmosTreeNodeStore(self._database.get_container_client("tree_nodes"), tenant_id=actor.tenant.tenant_id, user_id=user_id, forest_version=forest_version)

    def _fact_store(self, actor: ActorContext, user_id: str) -> CosmosFactStore:
        return CosmosFactStore(self._database.get_container_client("facts"), tenant_id=actor.tenant.tenant_id, user_id=user_id)

    def _location_index(self, actor: ActorContext, user_id: str, forest_version: str) -> CosmosFactLocationIndex:
        return CosmosFactLocationIndex(self._database.get_container_client("fact_locations"), tenant_id=actor.tenant.tenant_id, user_id=user_id, forest_version=forest_version)


def _manifest_response(manifest) -> ForestManifestResponse:
    return ForestManifestResponse(
        manifest_id=manifest.manifest_id,
        forest_version=manifest.forest_version,
        generation=manifest.generation,
        status=manifest.status,
        created_at_unix=manifest.created_at_unix,
        committed_at_unix=manifest.committed_at_unix,
        metadata=manifest.metadata,
    )


def _node_response(node) -> TreeNodeResponse:
    return TreeNodeResponse(
        tree_id=node.tree_id,
        node_id=node.node_id,
        level=node.level,
        summary=node.summary,
        child_ids=list(node.child_ids),
        fact_ids=list(node.fact_ids),
        metadata=dict(node.metadata),
    )


def _clamp(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, int(value)))
