from __future__ import annotations

import pytest

from src.cloud.contracts import RecallCandidate, StoredTreeNode
from src.cloud.cosmos_manifests import ForestManifest
from src.cloud.query_service import CloudMemoryQueryService, CloudQueryConfig, CloudQueryNotReady
from src.service.auth import ActorContext, TenantContext
from src.service.schemas import QueryRequest
from src.utils.types import ManagedFact


class FakeManifestStore:
    def __init__(self, manifest=None):
        self.manifest = manifest

    async def latest_committed(self):
        return self.manifest


class FakeRecallIndex:
    def __init__(self):
        self.filters = None

    def search(self, query_embedding, *, top_k, filters=None):
        self.filters = filters
        return [RecallCandidate(tree_id="tree-1", node_id="root", score=1.0)]


class FakeModel:
    def embed_texts(self, texts):
        return [[1.0, 0.0]]


class FakeTreeStore:
    def __init__(self):
        self.nodes = {
            "root": StoredTreeNode(tree_id="tree-1", node_id="root", level="L1", summary="", child_ids=("leaf-a", "leaf-b"), tenant_id="tenant-1", user_id="user-1", forest_version="v1"),
            "leaf-a": StoredTreeNode(tree_id="tree-1", node_id="leaf-a", level="L0", summary="", fact_ids=("fact-a",), tenant_id="tenant-1", user_id="user-1", forest_version="v1"),
            "leaf-b": StoredTreeNode(tree_id="tree-1", node_id="leaf-b", level="L0", summary="", fact_ids=("fact-b",), tenant_id="tenant-1", user_id="user-1", forest_version="v1"),
        }

    async def get_node(self, tree_id, node_id):
        return self.nodes.get(node_id)

    async def get_nodes(self, tree_id, node_ids):
        return [self.nodes[node_id] for node_id in node_ids if node_id in self.nodes]


class FakeEmbeddingStore:
    async def get_embeddings(self, node_ids):
        return {node_id: [1.0, 0.0] for node_id in node_ids}


class FakeFactStore:
    async def get_facts(self, fact_ids):
        return [
            ManagedFact(
                fact_id=fact_id,
                fact_text=f"Text for {fact_id}",
                embedding_id="",
                origin="",
                semantic_role="",
                entities=[],
                topics=[],
                attribute_keys=[],
                domain_keys=[],
                collection_keys=[],
                detail_level="",
                confidence=1.0,
                first_session_id="s1",
                first_cell_id="c1",
            )
            for fact_id in fact_ids
        ]


class FakeTraceRecorder:
    def __init__(self):
        self.calls = []

    async def record_events(self, **kwargs):
        self.calls.append(kwargs)
        return kwargs


def _manifest():
    return ForestManifest(
        manifest_id="manifest-1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"embedding_model_version": "embed-v1"},
    )


def _service(manifest=None, recall=None, trace_recorder=None):
    return CloudMemoryQueryService(
        manifest_store_factory=lambda tenant_id, user_id: FakeManifestStore(manifest),
        fact_store_factory=lambda tenant_id, user_id: FakeFactStore(),
        tree_node_store_factory=lambda tenant_id, user_id, forest_version: FakeTreeStore(),
        browse_embedding_store_factory=lambda tenant_id, user_id, forest_version, embedding_model_version: FakeEmbeddingStore(),
        recall_index=recall or FakeRecallIndex(),
        model_client=FakeModel(),
        config=CloudQueryConfig(embedding_model_version="embed-v1", default_beam_width=2),
        trace_recorder=trace_recorder,
    )


@pytest.mark.asyncio
async def test_cloud_query_requires_committed_manifest() -> None:
    with pytest.raises(CloudQueryNotReady, match="committed_manifest_missing"):
        await _service(None).query(
            ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
            user_id="user-1",
            request=QueryRequest(question="Where?"),
        )


@pytest.mark.asyncio
async def test_cloud_query_filters_recall_by_latest_manifest_and_embedding_model() -> None:
    recall = FakeRecallIndex()
    response = await _service(_manifest(), recall).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="Where?", max_facts=1),
    )

    assert recall.filters == {
        "tenant_id": "tenant-1",
        "user_id": "user-1",
        "forest_version": "v1",
        "embedding_model_version": "embed-v1",
    }
    assert [fact.fact_id for fact in response.facts] == ["fact-a"]


@pytest.mark.asyncio
async def test_cloud_query_records_safe_trace_events() -> None:
    recorder = FakeTraceRecorder()
    response = await _service(_manifest(), trace_recorder=recorder).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="Where?", trace_requested=True),
    )

    assert recorder.calls
    call = recorder.calls[0]
    assert call["query_id"] == response.query_id
    assert call["forest_version"] == "v1"
    event_types = [str(event.event_type) for event in call["events"]]
    assert "query_started" in event_types
    assert "recall_search_started" in event_types
    assert "facts_loaded" in event_types
    assert "query_completed" in event_types
    assert all("Where?" not in str(event.attributes) for event in call["events"])
