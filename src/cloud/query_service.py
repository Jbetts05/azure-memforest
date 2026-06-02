"""Cloud-backed memory query service."""

from __future__ import annotations

import asyncio
import math
import os
import re
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
from src.cloud.cosmos_source import CosmosFactLocationIndex, CosmosTreeNodeStore
from src.cloud.cosmos_traces import trace_recorder_from_database
from src.cloud.query_prerequisites import CloudQueryNotReady
from src.observability import TraceEvent, TraceEventType
from src.prompt.answer_prompts import ANSWER_SYSTEM, build_answer_user_message
from src.service.auth import ActorContext
from src.service.schemas import MemoryFactResponse, QueryRequest, QueryResponse

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CloudQueryConfig:
    default_recall_top_k: int = 8
    default_beam_width: int = 8
    default_max_facts: int = 20
    embedding_model_version: str = ""
    compose_answer: bool = True
    rerank_facts: bool = True
    lexical_fact_fallback: bool = True
    lexical_fact_fallback_top_k: int = 20
    recency_rerank_weight: float = 0.15


class CloudMemoryQueryService:
    def __init__(
        self,
        *,
        manifest_store_factory: Any,
        fact_store_factory: Any,
        tree_node_store_factory: Any,
        browse_embedding_store_factory: Any,
        fact_location_index_factory: Any | None = None,
        recall_index: Any,
        model_client: Any,
        config: CloudQueryConfig,
        trace_recorder: Any | None = None,
    ) -> None:
        self._manifest_store_factory = manifest_store_factory
        self._fact_store_factory = fact_store_factory
        self._tree_node_store_factory = tree_node_store_factory
        self._browse_embedding_store_factory = browse_embedding_store_factory
        self._fact_location_index_factory = fact_location_index_factory
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

        fact_store = self._fact_store_factory(tenant_id, user_id)
        facts = await fact_store.get_facts(fact_ids)
        fallback_facts = await self._lexical_fact_fallback(
            fact_store,
            tenant_id=tenant_id,
            user_id=user_id,
            forest_version=manifest.forest_version,
            question=request.question,
            existing_fact_ids={fact.fact_id for fact in facts},
        )
        if fallback_facts:
            facts = [*facts, *fallback_facts]
        events.append(
            TraceEvent(
                query_id=query_id,
                event_type=TraceEventType.FACTS_LOADED,
                tenant_id=tenant_id,
                user_id=user_id,
                attributes={
                    "fact_count": len(facts),
                    "raw_fact_id_count": len(fact_ids),
                    "lexical_fallback_fact_count": len(fallback_facts),
                },
            )
        )
        scored_facts = await self._rerank_facts(facts, query_embedding)
        if scored_facts:
            events.append(
                TraceEvent(
                    query_id=query_id,
                    event_type=TraceEventType.FACTS_RERANKED,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    attributes={
                        "candidate_count": len(scored_facts),
                        "top_score": round(scored_facts[0][0], 6),
                    },
                )
            )
        facts = [fact for _, fact in scored_facts] if scored_facts else facts
        max_facts = request.max_facts if request.max_facts is not None else self._config.default_max_facts
        if max_facts and max_facts > 0:
            facts = facts[:max_facts]
        answer, reason = await self._compose_answer(request.question, facts)
        if answer:
            events.append(
                TraceEvent(
                    query_id=query_id,
                    event_type=TraceEventType.ANSWER_GENERATED,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    attributes={"answer_length": len(answer), "fact_count": len(facts)},
                )
            )
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
            answer=answer,
            reason=reason,
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

    async def _compose_answer(self, question: str, facts: list[Any]) -> tuple[str | None, str | None]:
        if not self._config.compose_answer or not facts:
            return None, None
        context = _facts_context(facts)
        user_prompt = build_answer_user_message(question, context)
        try:
            payload = await self._generate_answer_json(user_prompt)
            answer = str(payload.get("answer") or "").strip()
            reason = str(payload.get("reason") or "").strip()
            if not answer:
                log.info("retrying blank cloud query answer", extra={"fact_count": len(facts)})
                retry_prompt = (
                    f"{user_prompt}\n\n"
                    "The previous response did not include a non-empty final answer. "
                    "Return JSON with a non-empty `answer` extracted from the provided facts. "
                    "If the facts directly state a count or total, use that value."
                )
                payload = await self._generate_answer_json(retry_prompt)
        except Exception:
            log.exception("failed to compose cloud query answer")
            return None, None
        answer = str(payload.get("answer") or "").strip()
        reason = str(payload.get("reason") or "").strip()
        return (answer or None), (reason or None)

    async def _generate_answer_json(self, user_prompt: str) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._model_client.generate_json,
            system_prompt=ANSWER_SYSTEM,
            user_prompt=user_prompt,
            model_name="answer",
            step_label="cloud_query_answer",
        )

    async def _lexical_fact_fallback(
        self,
        fact_store: Any,
        *,
        tenant_id: str,
        user_id: str,
        forest_version: str,
        question: str,
        existing_fact_ids: set[str],
    ) -> list[Any]:
        if (
            not self._config.lexical_fact_fallback
            or not _should_use_lexical_fact_fallback(question)
            or not hasattr(fact_store, "search_facts_text")
        ):
            return []
        try:
            candidates = await fact_store.search_facts_text(
                question,
                limit=max(1, int(self._config.lexical_fact_fallback_top_k)) * 4,
            )
        except Exception:
            log.exception("failed lexical cloud query fact fallback")
            return []
        candidates = [fact for fact in candidates if fact.fact_id not in existing_fact_ids]
        if not candidates:
            return []
        if self._fact_location_index_factory is None:
            return candidates
        location_index = self._fact_location_index_factory(tenant_id, user_id, forest_version)
        try:
            locations = await location_index.get_locations([fact.fact_id for fact in candidates])
        except Exception:
            log.exception("failed to filter lexical fallback facts by latest projection")
            return []
        filtered = [fact for fact in candidates if locations.get(fact.fact_id)]
        return filtered[: max(1, int(self._config.lexical_fact_fallback_top_k))]

    async def _rerank_facts(self, facts: list[Any], query_embedding: list[float]) -> list[tuple[float, Any]]:
        if not self._config.rerank_facts or not facts:
            return []
        embeddings = [_stored_fact_embedding(fact) for fact in facts]
        missing_indexes = [index for index, embedding in enumerate(embeddings) if not embedding]
        if missing_indexes:
            texts = [str(getattr(facts[index], "fact_text", "") or "") for index in missing_indexes]
            try:
                fallback_embeddings = await asyncio.to_thread(self._model_client.embed_texts, texts)
            except Exception:
                log.exception("failed to rerank cloud query facts")
                return []
            for index, embedding in zip(missing_indexes, fallback_embeddings):
                embeddings[index] = embedding
        recency_scores = _normalized_recency_scores(facts)
        scored = []
        for index, (embedding, fact) in enumerate(zip(embeddings, facts)):
            semantic_score = _dot(query_embedding, _normalize(embedding))
            recency_score = recency_scores.get(getattr(fact, "fact_id", ""), 0.0)
            scored.append((semantic_score + (self._config.recency_rerank_weight * recency_score), fact))
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored

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
    if root.level == "L0":
        return list(root.fact_ids)
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
        fact_location_index_factory=lambda tenant_id, user_id, forest_version: CosmosFactLocationIndex(database.get_container_client("fact_locations"), tenant_id=tenant_id, user_id=user_id, forest_version=forest_version),
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


_UPDATE_QUERY_PATTERN = re.compile(r"\b(how many|so far|current|currently|latest|now|updated?|total)\b", re.IGNORECASE)


def _should_use_lexical_fact_fallback(question: str) -> bool:
    return bool(_UPDATE_QUERY_PATTERN.search(str(question or "")))


def _normalized_recency_scores(facts: list[Any]) -> dict[str, float]:
    fact_times = {
        str(getattr(fact, "fact_id", "")): _fact_time(fact)
        for fact in facts
        if _fact_time(fact) is not None
    }
    if len(fact_times) < 2:
        return {fact_id: 0.0 for fact_id in fact_times}
    min_time = min(fact_times.values())
    max_time = max(fact_times.values())
    if max_time <= min_time:
        return {fact_id: 0.0 for fact_id in fact_times}
    return {
        fact_id: (value - min_time) / (max_time - min_time)
        for fact_id, value in fact_times.items()
    }


def _fact_time(fact: Any) -> float | None:
    candidates = [getattr(fact, "time_end", None), getattr(fact, "time_start", None)]
    for occurrence in getattr(fact, "occurrences", []) or []:
        candidates.extend([getattr(occurrence, "time_end", None), getattr(occurrence, "time_start", None)])
    numeric = [float(value) for value in candidates if value is not None]
    return max(numeric) if numeric else None


def _facts_context(facts: list[Any]) -> str:
    lines = []
    for index, fact in enumerate(facts, start=1):
        prefix = ""
        if getattr(fact, "time_start", None) is not None:
            prefix = f"[time_start={fact.time_start}] "
        lines.append(f"{index}. {prefix}{fact.fact_text}")
    return "\n".join(lines)


def _stored_fact_embedding(fact: Any) -> list[float] | None:
    metadata = getattr(fact, "metadata", {}) or {}
    embedding = metadata.get("embedding")
    if isinstance(embedding, list) and embedding:
        return [float(value) for value in embedding]
    return None
