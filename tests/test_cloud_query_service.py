from __future__ import annotations

import logging

import pytest

from src.cloud.contracts import RecallCandidate, StoredTreeNode
from src.cloud.cosmos_manifests import ForestManifest
from src.cloud.query_service import CloudMemoryQueryService, CloudQueryConfig, CloudQueryNotReady, _lexical_fallback_queries, _should_use_lexical_fact_fallback
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


class FakeLeafRecallIndex:
    def search(self, query_embedding, *, top_k, filters=None):
        return [RecallCandidate(tree_id="tree-1", node_id="leaf-b", score=1.0)]


class FakeModel:
    def __init__(self):
        self.embed_calls = []

    def embed_texts(self, texts):
        self.embed_calls.append(list(texts))
        embeddings = {
            "Where?": [1.0, 0.0],
            "Text for fact-a": [0.1, 1.0],
            "Text for fact-b": [1.0, 0.0],
        }
        return [embeddings.get(text, [1.0, 0.0]) for text in texts]

    def generate_json(self, **kwargs):
        return {"answer": "Alice moved to Seattle.", "reason": "The retrieved memory says Alice moved to Seattle."}


class BlankThenAnswerModel(FakeModel):
    def __init__(self):
        super().__init__()
        self.generate_prompts = []

    def generate_json(self, **kwargs):
        self.generate_prompts.append(kwargs["user_prompt"])
        if len(self.generate_prompts) == 1:
            return {"answer": "", "reason": ""}
        return {"answer": "4", "reason": "The memory says the user has 3 sisters and 1 brother."}


class RecordingModel(FakeModel):
    def __init__(self):
        super().__init__()
        self.generate_kwargs = []

    def generate_json(self, **kwargs):
        self.generate_kwargs.append(kwargs)
        return super().generate_json(**kwargs)


class BlankAnswerModel(FakeModel):
    def __init__(self):
        super().__init__()
        self.generate_count = 0

    def generate_json(self, **kwargs):
        self.generate_count += 1
        return {"answer": "", "reason": ""}


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
    def __init__(self, *, include_embeddings=True):
        self.include_embeddings = include_embeddings

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
                metadata={"embedding": [0.1, 1.0] if fact_id == "fact-a" else [1.0, 0.0]} if self.include_embeddings else {},
            )
            for fact_id in fact_ids
        ]


class FakeSiblingFactStore:
    async def get_facts(self, fact_ids):
        return [
            ManagedFact(
                fact_id=fact_id,
                fact_text="The user has 3 sisters and 1 brother, making a total of 4 siblings.",
                embedding_id="",
                origin="",
                semantic_role="state",
                entities=[],
                topics=[],
                attribute_keys=[],
                domain_keys=[],
                collection_keys=[],
                detail_level="specific",
                confidence=1.0,
                first_session_id="s1",
                first_cell_id="c1",
                metadata={"embedding": [1.0, 0.0]},
            )
            for fact_id in fact_ids
        ]


class FakeKnowledgeUpdateFactStore:
    def __init__(self):
        self.facts = {
            "old": ManagedFact(
                fact_id="old",
                fact_text="The user has bought three tops from H&M.",
                embedding_id="",
                origin="",
                semantic_role="state",
                entities=["H&M"],
                topics=["shopping"],
                attribute_keys=[],
                domain_keys=["retail"],
                collection_keys=["clothing_items"],
                detail_level="specific",
                confidence=1.0,
                first_session_id="s1",
                first_cell_id="c1",
                metadata={"embedding": [1.0, 0.0]},
                time_start=1.0,
                time_end=1.0,
            ),
            "new": ManagedFact(
                fact_id="new",
                fact_text="The user has already purchased five tops from H&M so far.",
                embedding_id="",
                origin="",
                semantic_role="state",
                entities=["H&M"],
                topics=["shopping"],
                attribute_keys=[],
                domain_keys=["retail"],
                collection_keys=["clothing_items"],
                detail_level="specific",
                confidence=1.0,
                first_session_id="s2",
                first_cell_id="c2",
                metadata={"embedding": [0.96, 0.0]},
                time_start=2.0,
                time_end=2.0,
            ),
        }

    async def get_facts(self, fact_ids):
        return [self.facts[fact_id] for fact_id in fact_ids if fact_id in self.facts]

    async def search_facts_text(self, query_text, *, limit=20):
        return [self.facts["new"], self.facts["old"]][:limit]


class FakeEventListFactStore:
    def __init__(self):
        self.facts = {
            "billie": _managed_fact("billie", "The user got back from a Billie Eilish concert at the Wells Fargo Center in Philly today.", time=1.0),
            "outdoor": _managed_fact("outdoor", "The user attended a free outdoor concert series in the park on March 25, 2023.", time=2.0),
            "brooklyn": _managed_fact("brooklyn", "The user attended a music festival in Brooklyn that featured the user's favorite indie bands.", time=3.0),
            "jazz": _managed_fact("jazz", "The user enjoyed a jazz night at a local bar today.", time=4.0),
            "queen": _managed_fact("queen", "The user saw Queen live with Adam Lambert at the Prudential Center in Newark, NJ.", time=5.0),
            "maybe": _managed_fact("maybe", "The user is thinking of attending a jazz night at a local bar soon.", time=0.5),
        }

    async def get_facts(self, fact_ids):
        return [self.facts[fact_id] for fact_id in fact_ids if fact_id in self.facts]

    async def search_facts_text(self, query_text, *, limit=20):
        query = query_text.lower()
        if "outdoor" in query or "jazz" in query:
            return [self.facts["maybe"], self.facts["outdoor"], self.facts["jazz"]][:limit]
        return [self.facts["billie"], self.facts["brooklyn"], self.facts["queen"]][:limit]

    async def search_event_facts_text(self, *, limit=100):
        return [
            self.facts["maybe"],
            self.facts["billie"],
            self.facts["outdoor"],
            self.facts["brooklyn"],
            self.facts["jazz"],
            self.facts["queen"],
        ][:limit]


class FakeFactLocationIndex:
    def __init__(self, present):
        self.present = set(present)

    async def get_locations(self, fact_ids):
        return {fact_id: ([object()] if fact_id in self.present else []) for fact_id in fact_ids}


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


def _managed_fact(fact_id: str, fact_text: str, *, time: float):
    return ManagedFact(
        fact_id=fact_id,
        fact_text=fact_text,
        embedding_id="",
        origin="",
        semantic_role="event",
        entities=[],
        topics=["music"],
        attribute_keys=[],
        domain_keys=[],
        collection_keys=[],
        detail_level="specific",
        confidence=1.0,
        first_session_id="s1",
        first_cell_id="c1",
        metadata={"embedding": [1.0, 0.0]},
        time_start=time,
        time_end=time,
    )


def _service(manifest=None, recall=None, trace_recorder=None, model=None, fact_store=None, fact_location_index=None, config=None):
    model = model or FakeModel()
    fact_store = fact_store or FakeFactStore()
    return CloudMemoryQueryService(
        manifest_store_factory=lambda tenant_id, user_id: FakeManifestStore(manifest),
        fact_store_factory=lambda tenant_id, user_id: fact_store,
        tree_node_store_factory=lambda tenant_id, user_id, forest_version: FakeTreeStore(),
        browse_embedding_store_factory=lambda tenant_id, user_id, forest_version, embedding_model_version: FakeEmbeddingStore(),
        fact_location_index_factory=(lambda tenant_id, user_id, forest_version: FakeFactLocationIndex(fact_location_index)) if fact_location_index is not None else None,
        recall_index=recall or FakeRecallIndex(),
        model_client=model,
        config=config or CloudQueryConfig(embedding_model_version="embed-v1", default_beam_width=2),
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
    assert [fact.fact_id for fact in response.facts] == ["fact-b"]
    assert response.answer == "Alice moved to Seattle."
    assert response.reason == "The retrieved memory says Alice moved to Seattle."


@pytest.mark.asyncio
async def test_cloud_query_rerank_uses_stored_fact_embeddings() -> None:
    model = FakeModel()
    response = await _service(_manifest(), model=model).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="Where?", max_facts=2),
    )

    assert [fact.fact_id for fact in response.facts] == ["fact-b", "fact-a"]
    assert model.embed_calls == [["Where?"]]


@pytest.mark.asyncio
async def test_cloud_query_handles_leaf_recall_candidate() -> None:
    response = await _service(_manifest(), recall=FakeLeafRecallIndex()).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="Where?", max_facts=2),
    )

    assert [fact.fact_id for fact in response.facts] == ["fact-b"]


@pytest.mark.asyncio
async def test_cloud_query_retries_blank_answer_with_stricter_prompt(caplog) -> None:
    model = BlankThenAnswerModel()
    with caplog.at_level(logging.INFO, logger="src.cloud.query_service"):
        response = await _service(_manifest(), model=model, fact_store=FakeSiblingFactStore()).query(
            ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
            user_id="user-1",
            request=QueryRequest(question="What is the total number of siblings I have?", max_facts=1),
        )

    assert response.answer == "4"
    assert len(model.generate_prompts) == 2
    assert "non-empty `answer`" in model.generate_prompts[1]
    assert "retrying blank cloud query answer" in caplog.text


@pytest.mark.asyncio
async def test_cloud_query_keeps_facts_when_answer_retry_stays_blank() -> None:
    model = BlankAnswerModel()
    response = await _service(_manifest(), model=model, fact_store=FakeSiblingFactStore()).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="What is the total number of siblings I have?", max_facts=1),
    )

    assert response.answer is None
    assert len(response.facts) == 1
    assert response.facts[0].text == "The user has 3 sisters and 1 brother, making a total of 4 siblings."
    assert model.generate_count == 2


def test_cloud_query_lexical_fallback_triggers_for_ordering_questions() -> None:
    assert _should_use_lexical_fact_fallback("What is the order of the concerts I attended?")
    assert _should_use_lexical_fact_fallback("List all musical events from the past two months.")


def test_cloud_query_event_list_expansion_puts_synonyms_first() -> None:
    queries = _lexical_fallback_queries("What is the order of the concerts and musical events I attended?")

    assert queries[1].startswith("concert show festival music event")
    assert queries[2].startswith("gig performance live venue band")
    assert queries[3].startswith("outdoor open-air park local bar")


@pytest.mark.asyncio
async def test_cloud_query_answer_prompt_includes_abstention_rule() -> None:
    model = RecordingModel()

    await _service(_manifest(), model=model).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="Which task happened first?", max_facts=1),
    )

    assert model.generate_kwargs
    assert "The information provided is not enough" in model.generate_kwargs[0]["system_prompt"]


@pytest.mark.asyncio
async def test_cloud_query_event_list_prioritizes_all_event_facts_chronologically() -> None:
    response = await _service(
        _manifest(),
        recall=FakeLeafRecallIndex(),
        fact_store=FakeEventListFactStore(),
        fact_location_index={"billie", "outdoor", "brooklyn", "jazz", "queen", "maybe"},
        config=CloudQueryConfig(embedding_model_version="embed-v1", default_beam_width=2, lexical_fact_fallback=True),
    ).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="What is the order of the concerts and musical events I attended?", max_facts=5),
    )

    assert [fact.fact_id for fact in response.facts] == ["billie", "outdoor", "brooklyn", "jazz", "queen"]


@pytest.mark.asyncio
async def test_cloud_query_lexical_fallback_and_recency_surface_latest_update() -> None:
    response = await _service(
        _manifest(),
        recall=FakeLeafRecallIndex(),
        fact_store=FakeKnowledgeUpdateFactStore(),
        fact_location_index={"new", "old"},
        config=CloudQueryConfig(
            embedding_model_version="embed-v1",
            default_beam_width=2,
            lexical_fact_fallback=True,
            recency_rerank_weight=0.15,
        ),
    ).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="How many tops have I bought from H&M so far?", max_facts=2),
    )

    assert [fact.fact_id for fact in response.facts] == ["new", "old"]


@pytest.mark.asyncio
async def test_cloud_query_lexical_fallback_filters_facts_not_in_latest_projection() -> None:
    response = await _service(
        _manifest(),
        recall=FakeLeafRecallIndex(),
        fact_store=FakeKnowledgeUpdateFactStore(),
        fact_location_index={"old"},
        config=CloudQueryConfig(embedding_model_version="embed-v1", default_beam_width=2, lexical_fact_fallback=True),
    ).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="How many tops have I bought from H&M so far?", max_facts=2),
    )

    assert [fact.fact_id for fact in response.facts] == ["old"]


@pytest.mark.asyncio
async def test_cloud_query_rerank_falls_back_to_fact_text_embedding_when_missing() -> None:
    model = FakeModel()
    response = await _service(_manifest(), model=model, fact_store=FakeFactStore(include_embeddings=False)).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="Where?", max_facts=2),
    )

    assert [fact.fact_id for fact in response.facts] == ["fact-b", "fact-a"]
    assert model.embed_calls == [["Where?"], ["Text for fact-a", "Text for fact-b"]]


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
    assert "facts_reranked" in event_types
    assert "answer_generated" in event_types
    assert "query_completed" in event_types
    assert all("Where?" not in str(event.attributes) for event in call["events"])


@pytest.mark.asyncio
async def test_cloud_query_can_disable_fact_rerank() -> None:
    service = CloudMemoryQueryService(
        manifest_store_factory=lambda tenant_id, user_id: FakeManifestStore(_manifest()),
        fact_store_factory=lambda tenant_id, user_id: FakeFactStore(),
        tree_node_store_factory=lambda tenant_id, user_id, forest_version: FakeTreeStore(),
        browse_embedding_store_factory=lambda tenant_id, user_id, forest_version, embedding_model_version: FakeEmbeddingStore(),
        recall_index=FakeRecallIndex(),
        model_client=FakeModel(),
        config=CloudQueryConfig(embedding_model_version="embed-v1", default_beam_width=2, rerank_facts=False),
    )

    response = await service.query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="Where?", max_facts=2),
    )

    assert [fact.fact_id for fact in response.facts] == ["fact-a", "fact-b"]
