from __future__ import annotations

import json
from dataclasses import asdict

import pytest

from src.cloud.query_service import _facts_context, _reference_date
from src.extraction.chunker import chunk_session
from src.extraction.pipeline import ChunkExtractionPipeline
from src.extraction.fact_manager import FactManager
from src.query.pipeline import ForestQuery, QueryResult, _render_fact_time_tag
from src.utils.types import FactOccurrence, ManagedFact, MemCell, MemoryItem, SessionExtractionResult


class TinyEmbeddingClient:
    def embed_texts(self, texts):
        return [[1.0, 0.0] for _ in texts]


def _item(item_id: str, *, time_start=None, time_end=None) -> MemoryItem:
    return MemoryItem(
        item_id=item_id,
        session_id="session-1",
        cell_id="cell-1",
        fact_text="The user has 1300 followers on Instagram.",
        source_turn_ids=["turn-1"],
        source_spans=[],
        participants=["user"],
        origin="user",
        semantic_role="state",
        entities=["user"],
        topics=["social media"],
        time_text="",
        time_start=time_start,
        time_end=time_end,
        attribute_keys=[],
        domain_keys=[],
        collection_keys=[],
        detail_level="specific",
        confidence=0.8,
        metadata={},
    )


def test_fact_manager_backfills_zero_times_from_cell(tmp_path) -> None:
    manager = FactManager(storage_dir=tmp_path, embedding_client=TinyEmbeddingClient(), judge=None, vector_dim=2)
    cell = MemCell(
        cell_id="cell-1",
        session_id="session-1",
        cell_index=0,
        start_turn_index=0,
        end_turn_index=0,
        turn_ids=["turn-1"],
        content_ids=["turn-1"],
        speaker_names=["user"],
        time_start=1685006880.0,
        time_end=1685006880.0,
        text="[2023/05/25 (Thu) 09:28] user: I have close to 1300 followers.",
    )
    result = SessionExtractionResult(
        session_id="session-1",
        cells=[cell],
        memory_items=[_item("item-1", time_start=0.0, time_end=0.0)],
    )

    write = manager.add_session_result(result)
    fact = manager.get_fact(write.canonical_fact_ids[0])

    assert fact is not None
    assert fact.time_start == cell.time_start
    assert fact.time_end == cell.time_end
    assert fact.occurrences[0].time_start == cell.time_start
    assert "2023" in fact.occurrences[0].time_text


def test_fact_manager_ignores_non_positive_times_when_merging(tmp_path) -> None:
    manager = FactManager(storage_dir=tmp_path, embedding_client=TinyEmbeddingClient(), judge=None, vector_dim=2)
    manager.add_memory_items([_item("old", time_start=1685006880.0, time_end=1685006880.0)])
    cell = MemCell(
        cell_id="cell-1",
        session_id="session-1",
        cell_index=0,
        start_turn_index=0,
        end_turn_index=0,
        turn_ids=["turn-1"],
        content_ids=["turn-1"],
        speaker_names=["user"],
        time_start=1685093280.0,
        time_end=1685093280.0,
        text="[2023/05/26 (Fri) 09:28] user: I have close to 1300 followers.",
    )
    result = SessionExtractionResult(
        session_id="session-1",
        cells=[cell],
        memory_items=[_item("new", time_start=0.0, time_end=0.0)],
    )

    write = manager.add_session_result(result)

    fact = manager.get_fact(write.canonical_fact_ids[0])

    assert fact is not None
    assert fact.time_start == 1685006880.0
    assert fact.time_end == 1685093280.0


def test_fact_manager_rejects_items_without_temporal_metadata(tmp_path) -> None:
    manager = FactManager(storage_dir=tmp_path, embedding_client=TinyEmbeddingClient(), judge=None, vector_dim=2)

    with pytest.raises(ValueError, match="missing required temporal metadata"):
        manager.add_memory_items([_item("bad", time_start=None, time_end=None)])


def test_fact_manager_rejects_dateless_facts_on_load(tmp_path) -> None:
    fact = ManagedFact(
        fact_id="fact-1",
        fact_text="The user moved to Seattle.",
        embedding_id="emb-1",
        origin="user",
        semantic_role="state",
        entities=[],
        topics=[],
        attribute_keys=[],
        domain_keys=[],
        collection_keys=[],
        detail_level="specific",
        confidence=1.0,
        first_session_id="session-1",
        first_cell_id="cell-1",
        occurrences=[
            FactOccurrence(
                session_id="session-1",
                cell_id="cell-1",
                item_id="item-1",
                source_turn_ids=["turn-1"],
                source_spans=["span"],
                participants=["user"],
                time_text="",
                time_start=None,
                time_end=None,
            )
        ],
        time_start=None,
        time_end=None,
    )
    tmp_path.mkdir(exist_ok=True)
    (tmp_path / "facts.jsonl").write_text(json.dumps(asdict(fact)) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="missing required temporal metadata"):
        FactManager(storage_dir=tmp_path, embedding_client=TinyEmbeddingClient(), judge=None, vector_dim=2)


def test_cloud_facts_context_renders_human_dates_not_raw_unix() -> None:
    fact = _item("item-1", time_start=1685006880.0, time_end=1685006880.0)
    context = _facts_context([fact])

    assert "2023" in context
    assert "time_start=1685006880" not in context
    assert "The user has 1300 followers" in context


def test_cloud_facts_context_omits_degenerate_dates() -> None:
    fact = _item("item-1", time_start=0.0, time_end=0.0)
    context = _facts_context([fact])

    assert "1970" not in context
    assert "time_start=0" not in context


def test_cloud_reference_date_uses_query_time_or_latest_fact_time() -> None:
    fact = _item("item-1", time_start=1685006880.0, time_end=1685006880.0)

    assert "2023" in _reference_date(None, [fact])
    assert "2024" in _reference_date(1716542880.0, [fact])
    assert _reference_date(0.0, [_item("bad", time_start=0.0, time_end=0.0)]) == ""


def test_local_fact_time_tag_does_not_render_zero_as_1970() -> None:
    assert _render_fact_time_tag(0.0, 0.0) == ""
    assert "1970" not in _render_fact_time_tag(0.0, 0.0)


def test_local_query_context_rejects_retrieved_dateless_facts() -> None:
    result = QueryResult(
        question="Where?",
        question_type="simple",
        recalled_trees=[],
        browse_plans=[],
        raw_ids=[],
        top_facts=[
            ManagedFact(
                fact_id="fact-1",
                fact_text="The user moved to Seattle.",
                embedding_id="",
                origin="user",
                semantic_role="state",
                entities=[],
                topics=[],
                attribute_keys=[],
                domain_keys=[],
                collection_keys=[],
                detail_level="specific",
                confidence=1.0,
                first_session_id="session-1",
                first_cell_id="cell-1",
                time_start=None,
                time_end=None,
            )
        ],
        session_date=None,
        query_emb=[],
    )

    with pytest.raises(ValueError, match="missing required temporal metadata"):
        ForestQuery.build_context(None, result)


def test_timestampless_turns_do_not_fabricate_1970_dates() -> None:
    cells = chunk_session("session-1", [{"role": "user", "content": "I bought a Pixel 8."}])

    [cell] = cells
    assert "1970" not in cell.text
    assert cell.turns[0].timestamp_text == ""


def test_fallback_extraction_does_not_fabricate_1970_dates() -> None:
    pipeline = ChunkExtractionPipeline(backend=None)

    result = pipeline.extract_session(
        "session-1",
        [{"role": "user", "content": "I bought a Pixel 8."}],
    )

    [item] = result.memory_items
    assert item.time_text == ""
    assert item.time_start is None
    assert item.time_end is None
