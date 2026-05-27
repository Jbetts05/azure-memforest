from __future__ import annotations

from pathlib import Path

from src.build.node_index import NodeIndex
from src.build.tree_store import TreeStore
from src.build.tree_types import TreeCard
from src.config.query_config import LIGHTWEIGHT
from src.observability import InMemoryTraceSink, TraceEventType
from src.query.pipeline import ForestQuery


class DummyEmbeddingClient:
    def embed_texts(self, texts: list[str], *, batch_size: int = 256) -> list[list[float]]:
        return [[1.0, 0.0] for _ in texts]


def test_forest_query_emits_safe_step_level_trace_events(tmp_path: Path) -> None:
    question = "Where did Bob live before Miami?"
    trace_sink = InMemoryTraceSink()
    pipeline = ForestQuery(
        embedding_client=DummyEmbeddingClient(),
        chat_client=None,
        tree_store=TreeStore(tmp_path / "trees"),
        node_index=NodeIndex(index_dir=tmp_path / "node_index", vector_dim=2),
        config=LIGHTWEIGHT,
        trace_sink=trace_sink,
    )

    result = pipeline.query(question, query_id="query-test")

    events = trace_sink.events(query_id="query-test")
    event_types = [event.event_type for event in events]
    assert result.query_id == "query-test"
    assert result.recalled_trees == []
    assert event_types == [
        TraceEventType.QUERY_STARTED,
        TraceEventType.EMBEDDING_REQUESTED,
        TraceEventType.RECALL_SEARCH_STARTED,
        TraceEventType.PLANNER_RESULT,
        TraceEventType.LEAF_IDS_RETURNED,
        TraceEventType.FACTS_LOADED,
        TraceEventType.FACTS_RERANKED,
        TraceEventType.QUERY_COMPLETED,
    ]
    assert events[0].attributes["question_length"] == len(question)
    assert events[-1].duration_ms is not None

    serialized_attribute_values = " ".join(
        str(value)
        for event in events
        for value in event.attributes.values()
    )
    assert question not in serialized_attribute_values
    assert "planner disabled" not in serialized_attribute_values


def test_recall_trace_uses_label_length_not_raw_label(tmp_path: Path) -> None:
    trace_sink = InMemoryTraceSink()
    pipeline = ForestQuery(
        embedding_client=DummyEmbeddingClient(),
        chat_client=None,
        tree_store=TreeStore(tmp_path / "trees"),
        node_index=NodeIndex(index_dir=tmp_path / "node_index", vector_dim=2),
        config=LIGHTWEIGHT,
        trace_sink=trace_sink,
    )
    sensitive_label = "Sensitive Person Name"
    pipeline._retriever.recall_with_query_emb = lambda question, top_k=None: (  # type: ignore[method-assign]
        [
            TreeCard(
                tree_id="entity:sensitive",
                tree_type="entity",
                label=sensitive_label,
                time_start=1.0,
                time_end=2.0,
                root_summary="summary",
                item_count=3,
            )
        ],
        [1.0, 0.0],
    )

    pipeline.query("Question", query_id="query-label")

    recall_events = [
        event for event in trace_sink.events(query_id="query-label")
        if event.event_type == TraceEventType.RECALL_TREE_SELECTED
    ]
    assert recall_events
    assert recall_events[0].attributes["label_length"] == len(sensitive_label)
    assert sensitive_label not in str(recall_events[0].attributes)


def test_forest_query_generates_query_id_without_trace_sink(tmp_path: Path) -> None:
    pipeline = ForestQuery(
        embedding_client=DummyEmbeddingClient(),
        chat_client=None,
        tree_store=TreeStore(tmp_path / "trees"),
        node_index=NodeIndex(index_dir=tmp_path / "node_index", vector_dim=2),
        config=LIGHTWEIGHT,
    )

    result = pipeline.query("A question")

    assert result.query_id
