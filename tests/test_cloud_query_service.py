from __future__ import annotations

import logging

import pytest

from src.cloud.contracts import RecallCandidate, StoredTreeNode
from src.cloud.cosmos_manifests import ForestManifest
from src.cloud.query_service import CloudMemoryQueryService, CloudQueryConfig, CloudQueryNotReady, _fact_time_for_rerank, _is_preference_activity_question, _lexical_fallback_queries, _normalized_recency_scores, _query_time_capped_fact_count, _query_time_open_interval_fact_count, _should_use_lexical_fact_fallback
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
                time_start=1685006880.0,
                time_end=1685006880.0,
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
                time_start=1685006880.0,
                time_end=1685006880.0,
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


class FakeAsOfFactStore:
    def __init__(self, *, interval_future_end: bool = False):
        self.interval_future_end = interval_future_end

    async def get_facts(self, fact_ids):
        fact_a_end = 30.0 if self.interval_future_end else 10.0
        facts = {
            "fact-a": ManagedFact(
                fact_id="fact-a",
                fact_text="The user had one cup of coffee.",
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
                metadata={"embedding": [0.1, 1.0]},
                time_start=10.0,
                time_end=fact_a_end,
            ),
            "fact-b": ManagedFact(
                fact_id="fact-b",
                fact_text="The user later had two cups of coffee.",
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
                first_session_id="s2",
                first_cell_id="c2",
                metadata={"embedding": [1.0, 0.0]},
                time_start=20.0,
                time_end=20.0,
            ),
        }
        return [facts[fact_id] for fact_id in fact_ids if fact_id in facts]


class FakeEventListFactStore:
    def __init__(self):
        self.facts = {
            "billie": _managed_fact("billie", "The user got back from a Billie Eilish concert at the Wells Fargo Center in Philly today.", time=1.0),
            "outdoor": _managed_fact("outdoor", "The user attended a free outdoor concert series in the park on March 25, 2023.", time=2.0),
            "brooklyn": _managed_fact("brooklyn", "The user attended a music festival in Brooklyn that featured the user's favorite indie bands.", time=3.0),
            "jazz": _managed_fact("jazz", "The user enjoyed a jazz night at a local bar today.", time=4.0),
            "queen": _managed_fact("queen", "The user saw Queen live with Adam Lambert at the Prudential Center in Newark, NJ.", time=5.0),
            "maybe": _managed_fact("maybe", "The user is thinking of attending a jazz night at a local bar soon.", time=0.5),
            "job": _managed_fact("job", "The user asked whether the library offers any job fairs or events.", time=0.1),
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
            self.facts["job"],
            self.facts["billie"],
            self.facts["outdoor"],
            self.facts["brooklyn"],
            self.facts["jazz"],
            self.facts["queen"],
        ][:limit]


class FakePreferenceActivityFactStore:
    def __init__(self):
        self.facts = {
            "relax": _managed_fact("relax", "The assistant suggested using relaxation techniques such as reading, meditation, deep breathing exercises, yoga or stretching, or listening to soothing music.", time=5.0),
            "winddown": _managed_fact("winddown", "The user prefers winding down by 9:30 pm to prepare for a good night's sleep.", time=5.0),
            "screens": _managed_fact("screens", "The assistant advised avoiding screens (phones, tablets, laptops, or TVs) for at least 30 minutes before bedtime.", time=5.0),
            "dance": _managed_fact("dance", "The user planned to ask Jamal whether he knows of any upcoming dance events or battles in the area.", time=1.0),
            "vacation": _managed_fact("vacation", "The user is planning a summer vacation that includes outdoor activities.", time=2.0),
        }

    async def get_facts(self, fact_ids):
        return [self.facts[fact_id] for fact_id in fact_ids if fact_id in self.facts]


class FakeDatelessFactStore:
    async def get_facts(self, fact_ids):
        return [
            ManagedFact(
                fact_id=fact_id,
                fact_text=f"Text for {fact_id}",
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
                time_start=None,
                time_end=None,
            )
            for fact_id in fact_ids
        ]


class FakePreferenceTreeStore:
    def __init__(self):
        self.nodes = {
            "root": StoredTreeNode(tree_id="tree-1", node_id="root", level="L1", summary="", child_ids=("leaf",), tenant_id="tenant-1", user_id="user-1", forest_version="v1"),
            "leaf": StoredTreeNode(tree_id="tree-1", node_id="leaf", level="L0", summary="", fact_ids=("dance", "screens", "vacation", "winddown", "relax"), tenant_id="tenant-1", user_id="user-1", forest_version="v1"),
        }

    async def get_node(self, tree_id, node_id):
        return self.nodes.get(node_id)

    async def get_nodes(self, tree_id, node_ids):
        return [self.nodes[node_id] for node_id in node_ids if node_id in self.nodes]


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


def _managed_fact(fact_id: str, fact_text: str = "fact", *, time: float | None = None, time_start: float | None = None, time_end: float | None = None):
    start = time if time is not None else time_start
    end = time if time is not None else time_end
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
        time_start=start,
        time_end=end,
    )


def _service(manifest=None, recall=None, trace_recorder=None, model=None, fact_store=None, tree_store=None, fact_location_index=None, config=None):
    model = model or FakeModel()
    fact_store = fact_store or FakeFactStore()
    tree_store = tree_store or FakeTreeStore()
    return CloudMemoryQueryService(
        manifest_store_factory=lambda tenant_id, user_id: FakeManifestStore(manifest),
        fact_store_factory=lambda tenant_id, user_id: fact_store,
        tree_node_store_factory=lambda tenant_id, user_id, forest_version: tree_store,
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
async def test_cloud_query_filters_future_facts_when_query_time_is_provided() -> None:
    response = await _service(_manifest(), fact_store=FakeAsOfFactStore()).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="How much coffee?", query_time=15.0, max_facts=2),
    )

    assert [fact.fact_id for fact in response.facts] == ["fact-a"]


@pytest.mark.asyncio
async def test_cloud_query_as_of_filter_keeps_facts_that_started_before_query_time() -> None:
    response = await _service(_manifest(), fact_store=FakeAsOfFactStore(interval_future_end=True)).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="How much coffee?", query_time=15.0, max_facts=2),
    )

    assert [fact.fact_id for fact in response.facts] == ["fact-a"]


def test_cloud_query_recency_scores_cap_future_interval_at_query_time() -> None:
    closed_fact = _managed_fact("closed", time_start=1.0, time_end=30.0)
    ongoing_fact = _managed_fact("ongoing", time_start=1.0, time_end=100.0)

    scores = _normalized_recency_scores([closed_fact, ongoing_fact], query_time=50.0)

    assert _fact_time_for_rerank(closed_fact, query_time=50.0) == 30.0
    assert _fact_time_for_rerank(ongoing_fact, query_time=50.0) == 50.0
    assert _query_time_capped_fact_count([closed_fact, ongoing_fact], 50.0) == 1
    assert scores == {"closed": 0.0, "ongoing": 1.0}


def test_cloud_query_recency_scores_anchor_open_interval_at_query_time() -> None:
    closed_fact = _managed_fact("closed", time_start=1.0, time_end=30.0)
    open_fact = _managed_fact("open", time_start=1.0, time_end=None)

    scores = _normalized_recency_scores([closed_fact, open_fact], query_time=50.0)

    assert _fact_time_for_rerank(open_fact, query_time=50.0) == 50.0
    assert _query_time_open_interval_fact_count([closed_fact, open_fact], 50.0) == 1
    assert scores == {"closed": 0.0, "open": 1.0}


@pytest.mark.asyncio
async def test_cloud_query_trace_records_query_time_rerank_anchor() -> None:
    recorder = FakeTraceRecorder()
    await _service(_manifest(), trace_recorder=recorder).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="Where?", query_time=1685006880.0, max_facts=2),
    )

    rerank_events = [
        event
        for call in recorder.calls
        for event in call["events"]
        if str(event.event_type) == "facts_reranked"
    ]
    assert rerank_events
    assert rerank_events[-1].attributes["query_time_rerank_anchor_present"] is True
    assert rerank_events[-1].attributes["query_time_capped_fact_count"] == 0
    assert rerank_events[-1].attributes["query_time_open_interval_fact_count"] == 0


@pytest.mark.asyncio
async def test_cloud_query_handles_leaf_recall_candidate() -> None:
    response = await _service(_manifest(), recall=FakeLeafRecallIndex()).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="Where?", max_facts=2),
    )

    assert [fact.fact_id for fact in response.facts] == ["fact-b"]


@pytest.mark.asyncio
async def test_cloud_query_rejects_retrieved_dateless_facts() -> None:
    with pytest.raises(ValueError, match="missing required temporal metadata"):
        await _service(_manifest(), fact_store=FakeDatelessFactStore()).query(
            ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
            user_id="user-1",
            request=QueryRequest(question="Where?", max_facts=1),
        )


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


def test_cloud_query_detects_preference_activity_questions() -> None:
    assert _is_preference_activity_question("Can you suggest some activities that I can do in the evening?")
    assert _is_preference_activity_question("What can I do before bed to relax?")
    assert not _is_preference_activity_question("What concerts did I attend in the evening?")


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
        request=QueryRequest(question="What is the order of the concerts and musical events I attended?", max_facts=3),
    )

    assert [fact.fact_id for fact in response.facts] == ["billie", "outdoor", "brooklyn", "jazz", "queen"]


@pytest.mark.asyncio
async def test_cloud_query_preference_activity_questions_drop_unrelated_event_noise() -> None:
    response = await _service(
        _manifest(),
        recall=FakeRecallIndex(),
        fact_store=FakePreferenceActivityFactStore(),
        tree_store=FakePreferenceTreeStore(),
        config=CloudQueryConfig(embedding_model_version="embed-v1", default_beam_width=2),
    ).query(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        request=QueryRequest(question="Can you suggest some activities that I can do in the evening?", max_facts=5),
    )

    fact_text = "\n".join(fact.text for fact in response.facts)
    assert [fact.fact_id for fact in response.facts] == ["screens", "winddown", "relax"]
    assert "phones, tablets, laptops, or TVs" in fact_text
    assert "dance events or battles" not in fact_text
    assert "summer vacation" not in fact_text


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
