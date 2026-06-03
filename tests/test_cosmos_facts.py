from __future__ import annotations

import pytest

from src.cloud.cosmos_facts import CosmosFactStore
from src.utils.types import FactOccurrence, ManagedFact


class NotFoundError(Exception):
    status_code = 404


class FakeContainer:
    def __init__(self):
        self.items = {}

    def upsert_item(self, item):
        self.items[(item["tenant_user_key"], item["id"])] = dict(item)

    def read_item(self, *, item, partition_key):
        key = (partition_key, item)
        if key not in self.items:
            raise NotFoundError()
        return dict(self.items[key])

    def query_items(self, *, query, parameters, partition_key):
        params = {param["name"]: param["value"] for param in parameters}
        result = []
        for (pk, _id), item in self.items.items():
            if pk != partition_key:
                continue
            if "@ids" in params and item["id"] not in params["@ids"]:
                continue
            token_values = [value for key, value in params.items() if key.startswith("@token")]
            if token_values and not any(token in item.get("fact_text", "").lower() for token in token_values):
                continue
            term_values = [value for key, value in params.items() if key.startswith("@term")]
            if term_values and not any(term in item.get("fact_text", "").lower() for term in term_values):
                continue
            if item.get("tombstoned"):
                continue
            result.append(dict(item))
        return result


def _fact(fact_id="fact-1") -> ManagedFact:
    return ManagedFact(
        fact_id=fact_id,
        fact_text="Bob moved to Davis.",
        embedding_id="emb-1",
        origin="user",
        semantic_role="event",
        entities=["Bob", "Davis"],
        topics=["move"],
        attribute_keys=["location"],
        domain_keys=["personal"],
        collection_keys=["memory"],
        detail_level="atomic",
        confidence=0.9,
        first_session_id="session-1",
        first_cell_id="cell-1",
        occurrences=[
            FactOccurrence(
                session_id="session-1",
                cell_id="cell-1",
                item_id="item-1",
                source_turn_ids=["turn-1"],
                source_spans=["span"],
                participants=["Bob"],
                time_text="today",
                time_start=1.0,
                time_end=1.0,
            )
        ],
        metadata={"embedding": [0.1, 0.2]},
        time_start=1.0,
        time_end=1.0,
    )


def _text_fact(fact_id: str, text: str, time_value: float) -> ManagedFact:
    fact = _fact(fact_id)
    return ManagedFact(
        **{
            **fact.__dict__,
            "fact_text": text,
            "time_start": time_value,
            "time_end": time_value,
            "occurrences": [
                FactOccurrence(
                    session_id="session-1",
                    cell_id="cell-1",
                    item_id=f"item-{fact_id}",
                    source_turn_ids=["turn-1"],
                    source_spans=["span"],
                    participants=["user"],
                    time_text=str(time_value),
                    time_start=time_value,
                    time_end=time_value,
                )
            ],
        }
    )


@pytest.mark.asyncio
async def test_cosmos_fact_store_round_trips_managed_fact() -> None:
    store = CosmosFactStore(FakeContainer(), tenant_id="tenant-1", user_id="user-1")

    await store.upsert_facts([_fact()])
    fact = await store.get_fact("fact-1")

    assert fact == _fact()
    assert fact.occurrences[0].source_turn_ids == ["turn-1"]


@pytest.mark.asyncio
async def test_cosmos_fact_store_get_facts_batches_and_preserves_order() -> None:
    store = CosmosFactStore(FakeContainer(), tenant_id="tenant-1", user_id="user-1")
    await store.upsert_facts([_fact("fact-1"), _fact("fact-2")])

    facts = await store.get_facts(["missing", "fact-2", "fact-1"])

    assert [fact.fact_id for fact in facts] == ["fact-2", "fact-1"]


@pytest.mark.asyncio
async def test_cosmos_fact_store_tombstone_hides_fact_without_scrubbing_text() -> None:
    container = FakeContainer()
    store = CosmosFactStore(container, tenant_id="tenant-1", user_id="user-1")
    await store.upsert_facts([_fact()])

    await store.tombstone_facts(["fact-1"], reason="session_deleted")

    assert await store.get_fact("fact-1") is None
    stored = next(iter(container.items.values()))
    assert stored["fact_text"] == "Bob moved to Davis."
    assert stored["tombstoned"] is True


@pytest.mark.asyncio
async def test_cosmos_fact_store_upsert_does_not_resurrect_tombstoned_fact() -> None:
    container = FakeContainer()
    store = CosmosFactStore(container, tenant_id="tenant-1", user_id="user-1")
    await store.upsert_facts([_fact()])
    await store.tombstone_facts(["fact-1"], reason="session_deleted")

    await store.upsert_facts([_fact()])

    assert await store.get_fact("fact-1") is None
    stored = next(iter(container.items.values()))
    assert stored["tombstoned"] is True


@pytest.mark.asyncio
async def test_cosmos_fact_store_is_not_forest_version_scoped() -> None:
    store = CosmosFactStore(FakeContainer(), tenant_id="tenant-1", user_id="user-1")
    await store.upsert_facts([_fact()])
    stored = next(iter(store.container.items.values()))

    assert "forest_version" not in stored


@pytest.mark.asyncio
async def test_cosmos_fact_store_search_preserves_brand_token_and_sorts_by_time() -> None:
    store = CosmosFactStore(FakeContainer(), tenant_id="tenant-1", user_id="user-1")
    await store.upsert_facts(
        [
            _text_fact("old", "The user bought three tops from H&M.", 1.0),
            _text_fact("new", "The user has already purchased five tops from H&M so far.", 2.0),
            _text_fact("other", "The user bought shoes from another brand.", 3.0),
        ]
    )

    facts = await store.search_facts_text("How many tops from H&M so far?", limit=10)

    assert [fact.fact_id for fact in facts[:2]] == ["new", "old"]


@pytest.mark.asyncio
async def test_cosmos_fact_store_search_event_facts_text_returns_event_terms_by_time() -> None:
    store = CosmosFactStore(FakeContainer(), tenant_id="tenant-1", user_id="user-1")
    await store.upsert_facts(
        [
            _text_fact("later", "The user saw Queen live at a venue.", 5.0),
            _text_fact("earlier", "The user attended a music festival in Brooklyn.", 2.0),
            _text_fact("non-event", "The user bought a kitchen table.", 1.0),
        ]
    )

    facts = await store.search_event_facts_text(limit=10)

    assert [fact.fact_id for fact in facts] == ["earlier", "later"]
