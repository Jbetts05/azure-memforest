"""Async Cosmos canonical fact store."""

from __future__ import annotations

import json
import re
import time
from dataclasses import asdict
from typing import Any

from src.cloud.schema import SCHEMA_VERSION, partition_key
from src.utils.types import FactOccurrence, ManagedFact

MAX_FACT_DOCUMENT_BYTES = 1024 * 1024
MAX_LEXICAL_FACT_TOKENS = 5
_FACT_SEARCH_STOPWORDS = {
    "about",
    "already",
    "from",
    "have",
    "many",
    "much",
    "what",
    "when",
    "where",
    "which",
    "with",
    "so",
    "far",
    "the",
    "user",
}


class CosmosFactStore:
    """Canonical facts scoped by tenant/user.

    Facts are source-of-truth records and are not forest-version scoped.
    Derived projections such as tree nodes and fact locations carry
    forest_version. Hard-delete/erasure compaction is implemented separately.
    """

    def __init__(self, container: Any, *, tenant_id: str, user_id: str) -> None:
        self.container = container
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.tenant_user_key = partition_key(tenant_id=tenant_id, user_id=user_id)

    async def get_fact(self, fact_id: str) -> ManagedFact | None:
        try:
            item = await _maybe_await(self.container.read_item(item=_fact_doc_id(fact_id), partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id or item.get("tombstoned"):
            return None
        return _fact_from_doc(item)

    async def get_facts(self, fact_ids: list[str]) -> list[ManagedFact]:
        if not fact_ids:
            return []
        docs_by_fact_id: dict[str, ManagedFact] = {}
        for chunk in _chunks([_fact_doc_id(fact_id) for fact_id in fact_ids], 100):
            query = (
                "SELECT * FROM c WHERE c.tenant_user_key=@pk AND ARRAY_CONTAINS(@ids, c.id) "
                "AND (NOT IS_DEFINED(c.tombstoned) OR c.tombstoned=false)"
            )
            params = [
                {"name": "@pk", "value": self.tenant_user_key},
                {"name": "@ids", "value": chunk},
            ]
            async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
                if item.get("tenant_id") == self.tenant_id and item.get("user_id") == self.user_id:
                    fact = _fact_from_doc(item)
                    docs_by_fact_id[fact.fact_id] = fact
        return [docs_by_fact_id[fact_id] for fact_id in fact_ids if fact_id in docs_by_fact_id]

    async def search_facts_text(self, query_text: str, *, limit: int = 20) -> list[ManagedFact]:
        tokens = _lexical_tokens(query_text)[:MAX_LEXICAL_FACT_TOKENS]
        if not tokens or limit <= 0:
            return []
        clauses = [f"CONTAINS(LOWER(c.fact_text), @token{index})" for index, _ in enumerate(tokens)]
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk "
            "AND (NOT IS_DEFINED(c.tombstoned) OR c.tombstoned=false) "
            f"AND ({' OR '.join(clauses)})"
        )
        params = [{"name": "@pk", "value": self.tenant_user_key}] + [
            {"name": f"@token{index}", "value": token}
            for index, token in enumerate(tokens)
        ]
        facts = []
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if item.get("tenant_id") == self.tenant_id and item.get("user_id") == self.user_id:
                facts.append(_fact_from_doc(item))
        facts.sort(key=_managed_fact_time, reverse=True)
        return facts[:limit]

    async def search_event_facts_text(self, *, limit: int = 100) -> list[ManagedFact]:
        if limit <= 0:
            return []
        terms = [
            "concert",
            "show",
            "gig",
            "festival",
            "performance",
            "music",
            "musical",
            "event",
            "attended",
            "saw",
            "live",
            "venue",
            "jazz",
            "band",
            "artist",
            "outdoor",
            "open-air",
            "park",
            "bar",
            "club",
        ]
        clauses = [f"CONTAINS(LOWER(c.fact_text), @term{index})" for index, _ in enumerate(terms)]
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk "
            "AND (NOT IS_DEFINED(c.tombstoned) OR c.tombstoned=false) "
            f"AND ({' OR '.join(clauses)})"
        )
        params = [{"name": "@pk", "value": self.tenant_user_key}] + [
            {"name": f"@term{index}", "value": term}
            for index, term in enumerate(terms)
        ]
        facts = []
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if item.get("tenant_id") == self.tenant_id and item.get("user_id") == self.user_id:
                facts.append(_fact_from_doc(item))
        facts.sort(key=_managed_fact_time)
        return facts[:limit]

    async def upsert_facts(self, facts: list[ManagedFact]) -> None:
        for fact in facts:
            _validate_fact_temporality(fact)
            existing = await self._read_raw_fact(fact.fact_id)
            if existing is not None and existing.get("tombstoned"):
                continue
            doc = _fact_to_doc(fact, self.tenant_id, self.user_id, self.tenant_user_key)
            if len(json.dumps(doc, ensure_ascii=False).encode("utf-8")) > MAX_FACT_DOCUMENT_BYTES:
                raise ValueError("fact document exceeds Cosmos size budget.")
            await _maybe_await(self.container.upsert_item(doc))

    async def tombstone_facts(self, fact_ids: list[str], *, reason: str) -> None:
        for fact_id in fact_ids:
            try:
                item = await _maybe_await(self.container.read_item(item=_fact_doc_id(fact_id), partition_key=self.tenant_user_key))
            except Exception as exc:
                if _is_not_found(exc):
                    continue
                raise
            if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id:
                continue
            item["tombstoned"] = True
            item["tombstone_reason"] = reason
            item["tombstoned_at_unix"] = time.time()
            await _maybe_await(self.container.upsert_item(item))

    async def _read_raw_fact(self, fact_id: str) -> dict[str, Any] | None:
        try:
            item = await _maybe_await(self.container.read_item(item=_fact_doc_id(fact_id), partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id:
            return None
        return item


def _fact_to_doc(fact: ManagedFact, tenant_id: str, user_id: str, tenant_user_key: str) -> dict[str, Any]:
    return {
        "id": _fact_doc_id(fact.fact_id),
        "schema_version": SCHEMA_VERSION,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "tenant_user_key": tenant_user_key,
        "fact_id": fact.fact_id,
        "fact_text": fact.fact_text,
        "embedding_id": fact.embedding_id,
        "origin": fact.origin,
        "semantic_role": fact.semantic_role,
        "entities": list(fact.entities),
        "topics": list(fact.topics),
        "attribute_keys": list(fact.attribute_keys),
        "domain_keys": list(fact.domain_keys),
        "collection_keys": list(fact.collection_keys),
        "detail_level": fact.detail_level,
        "confidence": fact.confidence,
        "first_session_id": fact.first_session_id,
        "first_cell_id": fact.first_cell_id,
        "occurrences": [asdict(occurrence) for occurrence in fact.occurrences],
        "metadata": dict(fact.metadata),
        "time_start": fact.time_start,
        "time_end": fact.time_end,
        "tombstoned": False,
        "updated_at_unix": time.time(),
    }


def _validate_fact_temporality(fact: ManagedFact) -> None:
    if _positive_time(fact.time_start) is None or _positive_time(fact.time_end) is None:
        raise ValueError(f"fact {fact.fact_id} is missing required temporal metadata.")
    for occurrence in fact.occurrences:
        if _positive_time(occurrence.time_start) is None or _positive_time(occurrence.time_end) is None:
            raise ValueError(f"fact {fact.fact_id} occurrence {occurrence.item_id} is missing required temporal metadata.")


def _fact_from_doc(item: dict[str, Any]) -> ManagedFact:
    occurrences = [FactOccurrence(**occurrence) for occurrence in item.get("occurrences", [])]
    fact = ManagedFact(
        fact_id=item["fact_id"],
        fact_text=item["fact_text"],
        embedding_id=item.get("embedding_id", ""),
        origin=item.get("origin", ""),
        semantic_role=item.get("semantic_role", ""),
        entities=list(item.get("entities", [])),
        topics=list(item.get("topics", [])),
        attribute_keys=list(item.get("attribute_keys", [])),
        domain_keys=list(item.get("domain_keys", [])),
        collection_keys=list(item.get("collection_keys", [])),
        detail_level=item.get("detail_level", ""),
        confidence=float(item.get("confidence", 1.0)),
        first_session_id=item.get("first_session_id", ""),
        first_cell_id=item.get("first_cell_id", ""),
        occurrences=occurrences,
        metadata=dict(item.get("metadata", {})),
        time_start=item.get("time_start"),
        time_end=item.get("time_end"),
    )
    _validate_fact_temporality(fact)
    return fact


def _fact_doc_id(fact_id: str) -> str:
    return f"fact_{fact_id}"


def _chunks(values: list[str], size: int):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _lexical_tokens(text: str) -> list[str]:
    seen = set()
    tokens = []
    for raw in re.findall(r"[a-z0-9]+(?:[&-][a-z0-9]+)*", str(text or "").lower()):
        if len(raw) < 3 or raw in _FACT_SEARCH_STOPWORDS or raw in seen:
            continue
        seen.add(raw)
        tokens.append(raw)
    return tokens


def _managed_fact_time(fact: ManagedFact) -> float:
    candidates = [fact.time_end, fact.time_start]
    for occurrence in fact.occurrences:
        candidates.extend([occurrence.time_end, occurrence.time_start])
    numeric = [float(value) for value in candidates if _positive_time(value) is not None]
    return max(numeric) if numeric else 0.0


def _positive_time(value: Any) -> float | None:
    if value is None:
        return None
    value = float(value)
    if value <= 0:
        return None
    return value


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value


async def _aiter(values):
    if hasattr(values, "__aiter__"):
        async for item in values:
            yield item
    else:
        for item in values:
            yield item


def _is_not_found(exc: Exception) -> bool:
    return exc.__class__.__name__ == "CosmosResourceNotFoundError" or getattr(exc, "status_code", None) == 404
