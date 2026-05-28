"""Cloud-backed memory query service."""

from __future__ import annotations

import asyncio
import math
import os
import uuid
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.cloud.azure_models import AzureOpenAIModelClient
from src.cloud.azure_search import AzureSearchRecallIndex
from src.cloud.config import load_cloud_config
from src.cloud.cosmos_browse_embeddings import CosmosBrowseEmbeddingStore
from src.cloud.cosmos_facts import CosmosFactStore
from src.cloud.cosmos_manifests import CosmosManifestStore, ForestManifest
from src.cloud.cosmos_source import CosmosTreeNodeStore
from src.cloud.cosmos_traces import trace_recorder_from_database
from src.cloud.query_prerequisites import CloudQueryNotReady
from src.observability import TraceEvent, TraceEventType
from src.service.auth import ActorContext
from src.service.schemas import MemoryFactResponse, QueryRequest, QueryResponse

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CloudQueryConfig:
    default_recall_top_k: int = 8
    default_beam_width: int = 8
    default_max_facts: int = 20
    embedding_model_version: str = ""


class CloudMemoryQueryService:
    def __init__(
        self,
        *,
        manifest_store_factory: Any,
        fact_store_factory: Any,
        tree_node_store_factory: Any,
        browse_embedding_store_factory: Any,
        recall_index: Any,
        model_client: Any,
        config: CloudQueryConfig,
        trace_recorder: Any | None = None,
    ) -> None:
        self._manifest_store_factory = manifest_store_factory
        self._fact_store_factory = fact_store_factory
        self._tree_node_store_factory = tree_node_store_factory
        self._browse_embedding_store_factory = browse_embedding_store_factory
        self._recall_index = recall_index
        self._model_client = model_client
        self._config = config
        self._trace_recorder = trace_recorder

    async def query(self, actor: ActorContext, *, user_id: str, request: QueryRequest) -> QueryResponse:
        tenant_id = actor.tenant.tenant_id
        query_id = str(uuid.uuid4())
        events: list[TraceEvent] = [
            TraceEvent(
                query_id=query_id,
                event_type=TraceEventType.QUERY_STARTED,
                tenant_id=tenant_id,
                user_id=user_id,
                attributes={"trace_requested": bool(request.trace_requested)},
            )
        ]
        manifest_store = self._manifest_store_factory(tenant_id, user_id)
        manifest = await manifest_store.latest_committed()
        if manifest is None:
            raise CloudQueryNotReady("committed_manifest_missing")
        embedding_model_version = str((manifest.metadata or {}).get("embedding_model_version") or self._config.embedding_model_version)
        if not embedding_model_version:
            raise CloudQueryNotReady("embedding_model_version_missing")
        if self._config.embedding_model_version and embedding_model_version != self._config.embedding_model_version:
            raise CloudQueryNotReady("cloud_query_model_mismatch")

        query_embedding = _normalize((await asyncio.to_thread(self._model_client.embed_texts, [request.question]))[0])
        recall_top_k = request.top_k or self._config.default_recall_top_k
        events.append(
            TraceEvent(
                query_id=query_id,
                event_type=TraceEventType.RECALL_SEARCH_STARTED,
                tenant_id=tenant_id,
                user_id=user_id,
                attributes={"forest_version": manifest.forest_version, "recall_top_k": recall_top_k},
            )
        )
        candidates = await asyncio.to_thread(
            self._recall_index.search,
            query_embedding,
            top_k=recall_top_k,
            filters={
                "tenant_id": tenant_id,
                "user_id": user_id,
                "forest_version": manifest.forest_version,
                "embedding_model_version": embedding_model_version,
            },
        )
        if not candidates:
            await self._record_trace(
                tenant_id=tenant_id,
                user_id=user_id,
                query_id=query_id,
                forest_version=manifest.forest_version,
                events=events
                + [
                    TraceEvent(
                        query_id=query_id,
                        event_type=TraceEventType.QUERY_COMPLETED,
                        tenant_id=tenant_id,
                        user_id=user_id,
                        attributes={"fact_count": 0, "recall_count": 0},
                    )
                ],
            )
            return QueryResponse(query_id=query_id, facts=[])

        tree_store = self._tree_node_store_factory(tenant_id, user_id, manifest.forest_version)
        embedding_store = self._browse_embedding_store_factory(tenant_id, user_id, manifest.forest_version, embedding_model_version)
        fact_ids: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            events.append(
                TraceEvent(
                    query_id=query_id,
                    event_type=TraceEventType.RECALL_TREE_SELECTED,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    tree_id=candidate.tree_id,
                    node_id=candidate.node_id,
                    score=candidate.score,
                    attributes={
                        "tree_type": candidate.tree_type or "",
                        "forest_version": candidate.forest_version or manifest.forest_version,
                    },
                )
            )
            for fact_id in await _browse_tree(
                tree_store,
                embedding_store,
                tree_id=candidate.tree_id,
                root_node_id=candidate.node_id,
                query_embedding=query_embedding,
                beam_width=self._config.default_beam_width,
            ):
                if fact_id not in seen:
                    seen.add(fact_id)
                    fact_ids.append(fact_id)

        facts = await self._fact_store_factory(tenant_id, user_id).get_facts(fact_ids)
        events.append(
            TraceEvent(
                query_id=query_id,
                event_type=TraceEventType.FACTS_LOADED,
                tenant_id=tenant_id,
                user_id=user_id,
                attributes={"fact_count": len(facts), "raw_fact_id_count": len(fact_ids)},
            )
        )
        max_facts = request.max_facts if request.max_facts is not None else self._config.default_max_facts
        if max_facts and max_facts > 0:
            facts = facts[:max_facts]
        await self._record_trace(
            tenant_id=tenant_id,
            user_id=user_id,
            query_id=query_id,
            forest_version=manifest.forest_version,
            events=events
            + [
                TraceEvent(
                    query_id=query_id,
                    event_type=TraceEventType.QUERY_COMPLETED,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    attributes={"fact_count": len(facts), "recall_count": len(candidates)},
                )
            ],
        )
        return QueryResponse(
            query_id=query_id,
            facts=[
                MemoryFactResponse(
                    fact_id=fact.fact_id,
                    text=fact.fact_text,
                    time_start=fact.time_start,
                    time_end=fact.time_end,
                    metadata={"forest_version": manifest.forest_version},
                )
                for fact in facts
            ],
        )

    async def _record_trace(self, *, tenant_id: str, user_id: str, query_id: str, forest_version: str, events: list[TraceEvent]) -> None:
        if self._trace_recorder is None:
            return
        try:
            await self._trace_recorder.record_events(
                tenant_id=tenant_id,
                user_id=user_id,
                query_id=query_id,
                forest_version=forest_version,
                events=events,
            )
        except Exception:
            log.exception("failed to persist cloud query trace")


async def _browse_tree(
    tree_store: Any,
    embedding_store: Any,
    *,
    tree_id: str,
    root_node_id: str,
    query_embedding: list[float],
    beam_width: int,
) -> list[str]:
    root = await tree_store.get_node(tree_id, root_node_id)
    if root is None:
        return []
    frontier = [root]
    settled = []
    while frontier and len(settled) < beam_width:
        child_ids = [child_id for node in frontier for child_id in node.child_ids]
        if not child_ids:
            break
        nodes = await tree_store.get_nodes(tree_id, child_ids)
        if not nodes:
            break
        leaves = [node for node in nodes if node.level == "L0"]
        internals = [node for node in nodes if node.level != "L0"]
        settled.extend(leaves[: max(0, beam_width - len(settled))])
        if len(settled) >= beam_width or not internals:
            break
        embeddings = await embedding_store.get_embeddings([node.node_id for node in internals])
        scored = [
            (_dot(query_embedding, _normalize(embeddings.get(node.node_id, []))), node)
            for node in internals
        ]
        scored.sort(key=lambda item: item[0], reverse=True)
        frontier = [node for _, node in scored[: max(1, beam_width - len(settled))]]
    out: list[str] = []
    for node in settled:
        out.extend(node.fact_ids)
    return out


def cloud_query_service_from_env(environ: dict[str, str] | None = None) -> CloudMemoryQueryService | None:
    values = dict(os.environ if environ is None else environ)
    required = [
        "AZURE_CLIENT_ID",
        "AZURE_COSMOS_ENDPOINT",
        "AZURE_COSMOS_DATABASE_NAME",
        "AZURE_SEARCH_ENDPOINT",
        "AZURE_SEARCH_RECALL_INDEX_NAME",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_VERSION",
        "AZURE_OPENAI_CHAT_DEPLOYMENT",
        "AZURE_OPENAI_EMBEDDING_DEPLOYMENT",
        "AZURE_OPENAI_EMBEDDING_DIMENSION",
        "AZURE_OPENAI_EMBEDDING_MODEL_VERSION",
    ]
    if any(not str(values.get(name, "")).strip() for name in required):
        return None
    from azure.cosmos.aio import CosmosClient
    from azure.identity import ManagedIdentityCredential, get_bearer_token_provider

    client_id = values["AZURE_CLIENT_ID"]
    credential = ManagedIdentityCredential(client_id=client_id)
    cosmos = CosmosClient(values["AZURE_COSMOS_ENDPOINT"], credential=credential)
    database = cosmos.get_database_client(values["AZURE_COSMOS_DATABASE_NAME"])
    cloud_config = load_cloud_config(values)
    if cloud_config.azure_openai is None:
        return None
    model_client = AzureOpenAIModelClient(
        cloud_config.azure_openai,
        azure_ad_token_provider=get_bearer_token_provider(credential, "https://cognitiveservices.azure.com/.default"),
    )
    embedding_dimension = int(values["AZURE_OPENAI_EMBEDDING_DIMENSION"])
    recall_index = AzureSearchRecallIndex(
        endpoint=values["AZURE_SEARCH_ENDPOINT"],
        index_name=values["AZURE_SEARCH_RECALL_INDEX_NAME"],
        vector_dimensions=embedding_dimension,
        credential=credential,
    )
    return CloudMemoryQueryService(
        manifest_store_factory=lambda tenant_id, user_id: CosmosManifestStore(database.get_container_client("manifests"), tenant_id=tenant_id, user_id=user_id),
        fact_store_factory=lambda tenant_id, user_id: CosmosFactStore(database.get_container_client("facts"), tenant_id=tenant_id, user_id=user_id),
        tree_node_store_factory=lambda tenant_id, user_id, forest_version: CosmosTreeNodeStore(database.get_container_client("tree_nodes"), tenant_id=tenant_id, user_id=user_id, forest_version=forest_version),
        browse_embedding_store_factory=lambda tenant_id, user_id, forest_version, embedding_model_version: CosmosBrowseEmbeddingStore(database.get_container_client("browse_embeddings"), tenant_id=tenant_id, user_id=user_id, forest_version=forest_version, embedding_model_version=embedding_model_version),
        recall_index=recall_index,
        model_client=model_client,
        config=CloudQueryConfig(
            embedding_model_version=values["AZURE_OPENAI_EMBEDDING_MODEL_VERSION"],
        ),
        trace_recorder=trace_recorder_from_database(database),
    )


def _normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in vec))
    return list(vec) if norm < 1e-9 else [value / norm for value in vec]


def _dot(left: list[float], right: list[float]) -> float:
    return sum(a * b for a, b in zip(left, right))
