from __future__ import annotations

from dataclasses import replace

import pytest

from src.cloud.contracts import StoredTreeNode
from src.config.config import load_default_config
from src.forest.memforest import _content_key
from src.forest.user_forest import UserForest
from src.utils.types import FactOccurrence, ManagedFact, MemoryItem
from src.worker.cloud_hydration import (
    CloudStateHydratingUserForestCore,
    CloudUserForestState,
    apply_cloud_state_to_user_forest,
    load_fact_ids_from_tree_store,
    load_cloud_hydration_prerequisites,
    load_cloud_user_forest_state,
    load_facts_for_memtrees,
    load_memtrees_from_tree_store,
    load_summary_cache_for_memtrees,
)
from src.worker.snapshot_hydration import build_user_forest_archive


class FakeTreeStore:
    def __init__(self, nodes_by_tree):
        self.nodes_by_tree = nodes_by_tree

    async def list_tree_ids(self, *, limit=10000):
        return list(self.nodes_by_tree)

    async def list_nodes_in_tree(self, tree_id, *, limit=100000):
        return list(self.nodes_by_tree.get(tree_id, []))[:limit]


class FailingTreeStore:
    async def list_tree_ids(self, *, limit=10000):
        raise RuntimeError("transient tree read failure")


class FakeFactStore:
    def __init__(self, facts_by_id):
        self.facts_by_id = facts_by_id
        self.requested = []

    async def get_facts(self, fact_ids):
        self.requested.append(list(fact_ids))
        return [self.facts_by_id[fact_id] for fact_id in reversed(fact_ids) if fact_id in self.facts_by_id]


class FakeSummaryCache:
    def __init__(self, summaries):
        self.summaries = summaries
        self.requested = []

    async def get(self, content_hash):
        self.requested.append(content_hash)
        return self.summaries.get(content_hash)


class FakeBrowseEmbeddingStore:
    def __init__(self, embeddings):
        self.embeddings = embeddings
        self.requested = []

    async def get_embeddings(self, node_ids):
        self.requested.append(list(node_ids))
        return {node_id: self.embeddings[node_id] for node_id in node_ids if node_id in self.embeddings}


class FakeModelClient:
    def __init__(self, dimension):
        self.dimension = dimension
        self.embed_calls = 0

    def embed_texts(self, texts, *, batch_size=256):
        self.embed_calls += 1
        return [[1.0] + [0.0] * (self.dimension - 1) for _ in texts]

    def generate_json(self, **kwargs):
        return {}


class FakeSnapshotStore:
    def __init__(self, payloads):
        self.payloads = payloads

    def read_snapshot(self, snapshot_id):
        return self.payloads[snapshot_id]


class FakeManifest:
    def __init__(self, metadata):
        self.metadata = metadata
        self.forest_version = str(metadata.get("forest_version") or "v1")


def _node(node_id, *, parent_id=None, level="L1", child_ids=(), fact_ids=(), tree_id="tree-1", tree_type="entity", tree_key="entity:alice", tree_label="Alice"):
    return StoredTreeNode(
        tree_id=tree_id,
        node_id=node_id,
        level=level,
        summary=f"summary {node_id}",
        child_ids=tuple(child_ids),
        fact_ids=tuple(fact_ids),
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        metadata={
            "parent_id": parent_id,
            "tree_type": tree_type,
            "tree_key": tree_key,
            "tree_label": tree_label,
            "time_start": 1.0,
            "time_end": 2.0,
            "item_count": 1,
            "generation": 3,
        },
    )


def _node_with_time(node_id, time_value, **kwargs):
    node = _node(node_id, **kwargs)
    metadata = dict(node.metadata)
    metadata["time_start"] = time_value
    metadata["time_end"] = time_value
    return replace(node, metadata=metadata)


def _embedding(dim):
    return [1.0] + [0.0] * (dim - 1)


def _fact(fact_id, dim):
    return ManagedFact(
        fact_id=fact_id,
        fact_text=f"fact text {fact_id}",
        embedding_id=f"emb-{fact_id}",
        origin="assistant",
        semantic_role="preference",
        entities=["alice"],
        topics=["topic"],
        attribute_keys=[],
        domain_keys=[],
        collection_keys=[],
        detail_level="atomic",
        confidence=1.0,
        first_session_id="session-1",
        first_cell_id="cell-1",
        occurrences=[
            FactOccurrence(
                session_id="session-1",
                cell_id="cell-1",
                item_id=f"item-{fact_id}",
                source_turn_ids=["turn-1"],
                source_spans=[],
                participants=["alice"],
                time_text="",
                time_start=1.0,
                time_end=2.0,
            )
        ],
        metadata={"embedding": _embedding(dim)},
        time_start=1.0,
        time_end=2.0,
    )


def _fact_for_cell(fact_id, dim, *, session_id, cell_id, turn_id=None):
    fact = _fact(fact_id, dim)
    occurrence = replace(
        fact.occurrences[0],
        session_id=session_id,
        cell_id=cell_id,
        source_turn_ids=[turn_id or f"{session_id}:0"],
    )
    return replace(fact, occurrences=[occurrence])


def _memory_item(item_id="item-1"):
    return MemoryItem(
        item_id=item_id,
        session_id="session-1",
        cell_id="cell-1",
        fact_text="Alice likes trail running.",
        source_turn_ids=["turn-1"],
        source_spans=[],
        participants=["alice"],
        origin="user",
        semantic_role="preference",
        entities=["alice"],
        topics=["running"],
        time_text="",
        time_start=1.0,
        time_end=2.0,
        attribute_keys=[],
        domain_keys=[],
        collection_keys=[],
        detail_level="atomic",
        confidence=1.0,
        metadata={},
    )


def _user_forest(tmp_path, dim):
    config = load_default_config()
    config = replace(config, api=replace(config.api, embedding=replace(config.api.embedding, dimension=dim)))
    client = FakeModelClient(dim)
    return UserForest(
        "user-1",
        tmp_path / "user-1",
        config,
        chat_client=client,
        embedding_client=client,
    )


def _user_forest_with_client(tmp_path, dim):
    config = load_default_config()
    config = replace(config, api=replace(config.api, embedding=replace(config.api.embedding, dimension=dim)))
    client = FakeModelClient(dim)
    forest = UserForest(
        "user-1",
        tmp_path / "user-1",
        config,
        chat_client=client,
        embedding_client=client,
    )
    return forest, client


@pytest.mark.asyncio
async def test_cloud_hydration_loads_memtrees_from_tree_store() -> None:
    store = FakeTreeStore(
        {
            "tree-1": [
                _node("root", child_ids=("leaf",)),
                _node("leaf", parent_id="root", level="L0", child_ids=("fact-1",), fact_ids=("fact-1",)),
            ]
        }
    )

    trees = await load_memtrees_from_tree_store(store)

    assert len(trees) == 1
    tree = trees[0]
    assert tree.tree_id == "tree-1"
    assert tree.tree_type == "entity"
    assert tree.tree_key == "entity:alice"
    assert tree.label == "Alice"
    assert tree.root_node_id == "root"
    assert set(tree.nodes) == {"root", "leaf"}
    assert tree.fact_ids_ordered == ["fact-1"]
    assert tree.version == 3


@pytest.mark.asyncio
async def test_cloud_hydration_uses_leaf_fact_ids_when_child_ids_absent() -> None:
    store = FakeTreeStore(
        {
            "tree-1": [
                _node("root", child_ids=("leaf",)),
                _node("leaf", parent_id="root", level="L0", fact_ids=("fact-1",)),
            ]
        }
    )

    tree = (await load_memtrees_from_tree_store(store))[0]

    assert tree.nodes["leaf"].child_ids == ["fact-1"]
    assert tree.fact_ids_ordered == ["fact-1"]


@pytest.mark.asyncio
async def test_cloud_hydration_rejects_ambiguous_roots() -> None:
    store = FakeTreeStore({"tree-1": [_node("root-a"), _node("root-b")]})

    with pytest.raises(ValueError, match="exactly one root"):
        await load_memtrees_from_tree_store(store)


@pytest.mark.asyncio
async def test_cloud_hydration_loads_referenced_facts_in_tree_order() -> None:
    trees = await load_memtrees_from_tree_store(
        FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("leaf-a", "leaf-b")),
                    _node("leaf-a", parent_id="root", level="L0", child_ids=("fact-1",)),
                    _node("leaf-b", parent_id="root", level="L0", child_ids=("fact-2",)),
                ],
                "tree-2": [
                    _node("root-2", tree_id="tree-2", child_ids=("leaf-c",)),
                    _node("leaf-c", tree_id="tree-2", parent_id="root-2", level="L0", child_ids=("fact-1", "fact-3")),
                ],
            }
        )
    )
    fact_store = FakeFactStore(
        {
            "fact-1": {"fact_id": "fact-1", "fact_text": "one"},
            "fact-2": {"fact_id": "fact-2", "fact_text": "two"},
        }
    )

    facts, missing = await load_facts_for_memtrees(fact_store, trees)

    assert fact_store.requested == [["fact-1", "fact-2", "fact-3"]]
    assert [fact["fact_id"] for fact in facts] == ["fact-1", "fact-2"]
    assert missing == ("fact-3",)


@pytest.mark.asyncio
async def test_cloud_hydration_fact_ids_from_tree_store_prefer_fact_ids_over_cell_payloads() -> None:
    fact_ids = await load_fact_ids_from_tree_store(
        FakeTreeStore(
            {
                "session:one": [
                    _node("root", child_ids=("cell-leaf",)),
                    _node(
                        "cell-leaf",
                        parent_id="root",
                        level="L0",
                        child_ids=("cell-1",),
                        fact_ids=("fact-1", "fact-2"),
                    ),
                ]
            }
        )
    )

    assert fact_ids == ["fact-1", "fact-2"]


@pytest.mark.asyncio
async def test_cloud_hydration_loads_summary_cache_for_internal_node_hashes() -> None:
    trees = await load_memtrees_from_tree_store(
        FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("internal",)),
                    _node("internal", parent_id="root", child_ids=("leaf-a", "leaf-b")),
                    _node("leaf-a", parent_id="internal", level="L0", child_ids=("fact-1",)),
                    _node("leaf-b", parent_id="internal", level="L0", child_ids=("fact-2",)),
                ]
            }
        )
    )
    content_hash = _content_key(frozenset({"fact-1", "fact-2"}))
    cache = FakeSummaryCache({content_hash: "cached summary"})

    summaries, missing = await load_summary_cache_for_memtrees(cache, trees)

    assert summaries == {content_hash: "cached summary"}
    assert missing == ()
    assert cache.requested == [content_hash]


@pytest.mark.asyncio
async def test_cloud_hydration_summary_cache_handles_multi_level_tree() -> None:
    trees = await load_memtrees_from_tree_store(
        FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("internal-a", "internal-b")),
                    _node("internal-a", parent_id="root", child_ids=("leaf-a", "leaf-b")),
                    _node("internal-b", parent_id="root", child_ids=("leaf-c",)),
                    _node("leaf-a", parent_id="internal-a", level="L0", child_ids=("fact-1",)),
                    _node("leaf-b", parent_id="internal-a", level="L0", child_ids=("fact-2",)),
                    _node("leaf-c", parent_id="internal-b", level="L0", child_ids=("fact-3",)),
                ]
            }
        )
    )
    internal_a_hash = _content_key(frozenset({"fact-1", "fact-2"}))
    internal_b_hash = _content_key(frozenset({"fact-3"}))
    root_hash = _content_key(frozenset({"fact-1", "fact-2", "fact-3"}))
    cache = FakeSummaryCache(
        {
            internal_a_hash: "A",
            internal_b_hash: "B",
            root_hash: "root",
        }
    )

    summaries, missing = await load_summary_cache_for_memtrees(cache, trees)

    assert summaries == {
        internal_a_hash: "A",
        internal_b_hash: "B",
        root_hash: "root",
    }
    assert missing == ()


@pytest.mark.asyncio
async def test_cloud_hydration_prerequisites_reports_missing_state_without_mutating_hot_path() -> None:
    state = await load_cloud_hydration_prerequisites(
        tree_store=FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("leaf",)),
                    _node("leaf", parent_id="root", level="L0", child_ids=("fact-1",)),
                ]
            }
        ),
        fact_store=FakeFactStore({}),
        summary_cache=FakeSummaryCache({}),
    )

    assert len(state.trees) == 1
    assert state.facts == []
    assert state.summary_cache == {}
    assert state.missing_fact_ids == ("fact-1",)
    assert state.missing_summary_cache_keys == (_content_key(frozenset({"fact-1"})),)


@pytest.mark.asyncio
async def test_cloud_hydration_loads_user_forest_state_with_browse_embeddings() -> None:
    store = FakeTreeStore(
        {
            "tree-1": [
                _node("root", child_ids=("leaf",)),
                _node("leaf", parent_id="root", level="L0", child_ids=("fact_1",)),
            ]
        }
    )
    dim = 4

    state = await load_cloud_user_forest_state(
        tree_store=store,
        fact_store=FakeFactStore({"fact_1": _fact_for_cell("fact_1", dim, session_id="s1", cell_id="s1#cell-1")}),
        summary_cache=FakeSummaryCache({_content_key(frozenset({"fact_1"})): "cached root"}),
        browse_embedding_store=FakeBrowseEmbeddingStore({"root": _embedding(dim)}),
    )

    assert [fact.fact_id for fact in state.facts] == ["fact_1"]
    assert set(state.node_embeddings) == {"root"}
    assert state.missing_fact_ids == ()
    assert state.missing_node_embedding_ids == ()


@pytest.mark.asyncio
async def test_cloud_hydration_marks_absent_required_stores_incomplete() -> None:
    state = await load_cloud_user_forest_state(
        tree_store=FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("leaf",)),
                    _node("leaf", parent_id="root", level="L0", child_ids=("fact_1",)),
                ]
            }
        )
    )

    assert state.missing_fact_ids == ("fact_1",)
    assert state.missing_summary_cache_keys == (_content_key(frozenset({"fact_1"})),)
    assert state.missing_node_embedding_ids == ("root",)


@pytest.mark.asyncio
async def test_cloud_hydration_does_not_load_session_cell_ids_as_facts() -> None:
    state = await load_cloud_user_forest_state(
        tree_store=FakeTreeStore(
            {
                "session:s1": [
                    _node("session-leaf", level="L0", child_ids=("s1#cell-1",), fact_ids=("fact_1",), tree_id="session:s1", tree_type="session", tree_key="s1", tree_label="s1"),
                ]
            }
        ),
        fact_store=FakeFactStore({"fact_1": _fact("fact_1", 4)}),
    )

    assert state.trees[0].tree_type == "session"
    assert state.trees[0].fact_ids_ordered == []
    assert state.trees[0].session_leaves["s1#cell-1"].fact_ids == ["fact_1"]
    assert state.missing_fact_ids == ()


@pytest.mark.asyncio
async def test_cloud_hydration_applies_session_tree_with_reconstructed_leaves(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    state = await load_cloud_user_forest_state(
        tree_store=FakeTreeStore(
            {
                "session:s1": [
                    _node("session-leaf", level="L0", child_ids=("s1#cell-1",), fact_ids=("fact_1",), tree_id="session:s1", tree_type="session", tree_key="s1", tree_label="s1"),
                ]
            }
        ),
        fact_store=FakeFactStore({"fact_1": _fact_for_cell("fact_1", dim, session_id="s1", cell_id="s1#cell-1")}),
    )

    result = apply_cloud_state_to_user_forest(user_forest, state)

    assert result.tree_count == 1
    assert user_forest._tree_store.get("session:s1").session_leaves["s1#cell-1"].fact_ids == ["fact_1"]


@pytest.mark.asyncio
async def test_cloud_hydration_applies_multi_leaf_session_tree(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    state = await load_cloud_user_forest_state(
        tree_store=FakeTreeStore(
            {
                "session:s1": [
                    _node("session-root", child_ids=("session-leaf-a", "session-leaf-b"), tree_id="session:s1", tree_type="session", tree_key="s1", tree_label="s1"),
                    _node_with_time("session-leaf-a", 1.0, parent_id="session-root", level="L0", child_ids=("s1#cell-1",), fact_ids=("fact_1",), tree_id="session:s1", tree_type="session", tree_key="s1", tree_label="s1"),
                    _node_with_time("session-leaf-b", 2.0, parent_id="session-root", level="L0", child_ids=("s1#cell-2",), fact_ids=("fact_2",), tree_id="session:s1", tree_type="session", tree_key="s1", tree_label="s1"),
                ]
            }
        ),
        fact_store=FakeFactStore({"fact_1": _fact("fact_1", dim), "fact_2": _fact("fact_2", dim)}),
        summary_cache=FakeSummaryCache({_content_key(frozenset({"s1#cell-1", "s1#cell-2"})): "cached root"}),
        browse_embedding_store=FakeBrowseEmbeddingStore({"session-root": _embedding(dim)}),
    )

    apply_cloud_state_to_user_forest(user_forest, state)

    tree = user_forest._tree_store.get("session:s1")
    assert tree.nodes["session-leaf-a"].next_leaf_id == "session-leaf-b"
    assert tree.session_leaves["s1#cell-2"].fact_ids == ["fact_2"]


@pytest.mark.asyncio
async def test_cloud_hydration_applies_state_to_real_user_forest(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    state = await load_cloud_user_forest_state(
        tree_store=FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("leaf",)),
                    _node("leaf", parent_id="root", level="L0", child_ids=("fact_1",)),
                ]
            }
        ),
        fact_store=FakeFactStore({"fact_1": _fact_for_cell("fact_1", dim, session_id="s1", cell_id="s1#cell-1")}),
        summary_cache=FakeSummaryCache({_content_key(frozenset({"fact_1"})): "cached root"}),
        browse_embedding_store=FakeBrowseEmbeddingStore({"root": _embedding(dim)}),
    )

    result = apply_cloud_state_to_user_forest(user_forest, state)
    snapshot = user_forest.snapshot_for_materialization()

    assert result.tree_count == 1
    assert result.fact_count == 1
    assert result.node_embedding_count == 1
    assert user_forest._fact_manager.get_fact("fact_1") is not None
    assert user_forest._tree_store.get("tree-1") is not None
    assert user_forest._node_index.get_embedding("root") is not None
    assert [fact.fact_id for fact in snapshot.facts] == ["fact_1"]
    assert [tree.tree_id for tree in snapshot.trees] == ["tree-1"]


@pytest.mark.asyncio
async def test_cloud_hydration_applies_multi_leaf_entity_tree(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    state = await load_cloud_user_forest_state(
        tree_store=FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("leaf-a", "leaf-b")),
                    _node_with_time("leaf-a", 1.0, parent_id="root", level="L0", child_ids=("fact_a",), tree_id="tree-1"),
                    _node_with_time("leaf-b", 2.0, parent_id="root", level="L0", child_ids=("fact_b",), tree_id="tree-1"),
                ]
            }
        ),
        fact_store=FakeFactStore({"fact_a": _fact("fact_a", dim), "fact_b": _fact("fact_b", dim)}),
        summary_cache=FakeSummaryCache({_content_key(frozenset({"fact_a", "fact_b"})): "cached root"}),
        browse_embedding_store=FakeBrowseEmbeddingStore({"root": _embedding(dim)}),
    )

    apply_cloud_state_to_user_forest(user_forest, state)

    tree = user_forest._tree_store.get("tree-1")
    assert tree.nodes["leaf-a"].next_leaf_id == "leaf-b"
    assert tree.nodes["leaf-b"].prev_leaf_id == "leaf-a"


@pytest.mark.asyncio
async def test_cloud_hydration_infers_tree_k_from_committed_fanout(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    state = await load_cloud_user_forest_state(
        tree_store=FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("leaf-a", "leaf-b", "leaf-c", "leaf-d")),
                    _node_with_time("leaf-a", 1.0, parent_id="root", level="L0", child_ids=("fact_a",), tree_id="tree-1"),
                    _node_with_time("leaf-b", 2.0, parent_id="root", level="L0", child_ids=("fact_b",), tree_id="tree-1"),
                    _node_with_time("leaf-c", 3.0, parent_id="root", level="L0", child_ids=("fact_c",), tree_id="tree-1"),
                    _node_with_time("leaf-d", 4.0, parent_id="root", level="L0", child_ids=("fact_d",), tree_id="tree-1"),
                ]
            }
        ),
        fact_store=FakeFactStore({fact_id: _fact(fact_id, dim) for fact_id in ("fact_a", "fact_b", "fact_c", "fact_d")}),
        summary_cache=FakeSummaryCache({_content_key(frozenset({"fact_a", "fact_b", "fact_c", "fact_d"})): "cached root"}),
        browse_embedding_store=FakeBrowseEmbeddingStore({"root": _embedding(dim)}),
    )

    apply_cloud_state_to_user_forest(user_forest, state)

    assert user_forest._tree_store.get("tree-1").k == 4


@pytest.mark.asyncio
async def test_cloud_hydration_honors_persisted_k_for_fanout_validation(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    root = _node("root", child_ids=("leaf-a", "leaf-b", "leaf-c", "leaf-d"))
    root = replace(root, metadata={**root.metadata, "k": 3})

    trees = await load_memtrees_from_tree_store(
        FakeTreeStore(
            {
                "tree-1": [
                    root,
                    _node_with_time("leaf-a", 1.0, parent_id="root", level="L0", child_ids=("fact_a",), tree_id="tree-1"),
                    _node_with_time("leaf-b", 2.0, parent_id="root", level="L0", child_ids=("fact_b",), tree_id="tree-1"),
                    _node_with_time("leaf-c", 3.0, parent_id="root", level="L0", child_ids=("fact_c",), tree_id="tree-1"),
                    _node_with_time("leaf-d", 4.0, parent_id="root", level="L0", child_ids=("fact_d",), tree_id="tree-1"),
                ]
            }
        )
    )

    with pytest.raises(ValueError, match="fanout 4 exceeds k=3"):
        apply_cloud_state_to_user_forest(user_forest, CloudUserForestState(trees=trees))


@pytest.mark.asyncio
async def test_cloud_hydration_repairs_duplicate_child_links_from_split_artifact(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    root = _node("root", level="L2", child_ids=("entity:user:L1:13", "entity:user:L1:14"))
    old_parent = _node("entity:user:L1:13", parent_id="root", child_ids=("entity:user:L0:63", "entity:user:L0:64"))
    new_parent = _node("entity:user:L1:14", parent_id="root", child_ids=("entity:user:L0:64", "entity:user:L0:65"))
    leaves = [
        _node_with_time("entity:user:L0:63", 63.0, parent_id="entity:user:L1:13", level="L0", child_ids=("fact_63",), tree_id="tree-1"),
        _node_with_time("entity:user:L0:64", 64.0, parent_id="entity:user:L1:13", level="L0", child_ids=("fact_64",), tree_id="tree-1"),
        _node_with_time("entity:user:L0:65", 65.0, parent_id="entity:user:L1:14", level="L0", child_ids=("fact_65",), tree_id="tree-1"),
    ]

    trees = await load_memtrees_from_tree_store(FakeTreeStore({"tree-1": [root, old_parent, new_parent, *leaves]}))
    apply_cloud_state_to_user_forest(user_forest, CloudUserForestState(trees=trees))
    tree = user_forest._tree_store.get("tree-1")

    assert tree.nodes["entity:user:L1:13"].child_ids == ["entity:user:L0:63"]
    assert tree.nodes["entity:user:L1:14"].child_ids == ["entity:user:L0:64", "entity:user:L0:65"]
    assert tree.nodes["entity:user:L0:64"].parent_id == "entity:user:L1:14"


@pytest.mark.asyncio
async def test_cloud_hydration_prunes_empty_parent_after_duplicate_child_repair(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    root = _node("root", level="L2", child_ids=("entity:user:L1:13", "entity:user:L1:14"))
    empty_after_repair = _node("entity:user:L1:13", parent_id="root", child_ids=("entity:user:L0:64",))
    survivor = _node("entity:user:L1:14", parent_id="root", child_ids=("entity:user:L0:64", "entity:user:L0:65"))
    leaves = [
        _node_with_time("entity:user:L0:64", 64.0, parent_id="entity:user:L1:13", level="L0", child_ids=("fact_64",), tree_id="tree-1"),
        _node_with_time("entity:user:L0:65", 65.0, parent_id="entity:user:L1:14", level="L0", child_ids=("fact_65",), tree_id="tree-1"),
    ]

    trees = await load_memtrees_from_tree_store(FakeTreeStore({"tree-1": [root, empty_after_repair, survivor, *leaves]}))
    apply_cloud_state_to_user_forest(user_forest, CloudUserForestState(trees=trees))
    tree = user_forest._tree_store.get("tree-1")

    assert "entity:user:L1:13" not in tree.nodes
    assert tree.nodes["root"].child_ids == ["entity:user:L1:14"]
    assert tree.nodes["entity:user:L0:64"].parent_id == "entity:user:L1:14"


def test_cloud_hydration_uses_stored_fact_embeddings_without_reembedding(tmp_path) -> None:
    dim = 4
    user_forest, client = _user_forest_with_client(tmp_path, dim)
    state = CloudUserForestState(
        trees=[],
        facts=[_fact("fact_1", dim)],
        summary_cache={},
        node_embeddings={},
    )

    apply_cloud_state_to_user_forest(user_forest, state)

    assert client.embed_calls == 0
    assert user_forest._fact_manager.get_fact("fact_1") is not None


def test_materialization_snapshot_injects_fact_embeddings_without_mutating_local_fact(tmp_path) -> None:
    dim = 4
    user_forest, _client = _user_forest_with_client(tmp_path, dim)

    write = user_forest._fact_manager.add_memory_items([_memory_item()])
    fact = user_forest._fact_manager.get_fact(write.canonical_fact_ids[0])
    snapshot = user_forest.snapshot_for_materialization()

    assert fact is not None
    assert "embedding" not in fact.metadata
    assert snapshot.facts[0].metadata["embedding"] == _embedding(dim)


def test_materialization_snapshot_preserves_cloud_hydration_baseline_after_delta(tmp_path) -> None:
    user_forest = _user_forest(tmp_path, 4)

    user_forest._record_projection_baseline("g7-baseline")
    user_forest._mark_projection_delta_unknown()
    user_forest._record_projection_delta(changed_tree_ids={"tree-1"}, deleted_tree_ids=set())
    snapshot = user_forest.snapshot_for_materialization()

    assert snapshot.baseline_forest_version == "g7-baseline"
    assert snapshot.changed_tree_ids == frozenset({"tree-1"})


@pytest.mark.asyncio
async def test_cloud_hydration_rebuilds_session_registry_for_delete(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    state = await load_cloud_user_forest_state(
        tree_store=FakeTreeStore(
            {
                "session:s1": [
                    _node("session-leaf", level="L0", child_ids=("s1#cell-1",), fact_ids=("fact_1",), tree_id="session:s1", tree_type="session", tree_key="s1", tree_label="s1"),
                ],
                "entity:user": [
                    _node("entity-leaf", level="L0", child_ids=("fact_1",), tree_id="entity:user", tree_key="user", tree_label="user"),
                ],
            }
        ),
        fact_store=FakeFactStore({"fact_1": _fact_for_cell("fact_1", dim, session_id="s1", cell_id="s1#cell-1")}),
    )
    apply_cloud_state_to_user_forest(user_forest, state)

    user_forest.delete_session("s1")

    assert user_forest._fact_manager.get_fact("fact_1") is None


@pytest.mark.asyncio
async def test_cloud_hydration_registry_strips_session_tree_key_prefix(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    state = await load_cloud_user_forest_state(
        tree_store=FakeTreeStore(
            {
                "session:s1": [
                    _node("session-leaf", level="L0", child_ids=("s1#cell-1",), fact_ids=("fact_1",), tree_id="session:s1", tree_type="session", tree_key="session:s1", tree_label="s1"),
                ]
            }
        ),
        fact_store=FakeFactStore({"fact_1": _fact_for_cell("fact_1", dim, session_id="public-session", cell_id="public-session#cell_0000_abcd")}),
    )
    apply_cloud_state_to_user_forest(user_forest, state)

    user_forest.delete_session("s1")

    assert user_forest._registry._sessions["s1"].deleted is True


@pytest.mark.asyncio
async def test_cloud_hydration_registry_uses_public_session_id_from_cell_id(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    state = await load_cloud_user_forest_state(
        tree_store=FakeTreeStore(
            {
                "session:sess_0001": [
                    _node("session-leaf", level="L0", child_ids=("public-session#cell_0000_abcd",), fact_ids=("fact_1",), tree_id="session:sess_0001", tree_type="session", tree_key="sess_0001", tree_label="sess_0001"),
                ],
                "entity:user": [
                    _node("entity-leaf", level="L0", child_ids=("fact_1",), tree_id="entity:user", tree_key="user", tree_label="user"),
                ],
            }
        ),
        fact_store=FakeFactStore({"fact_1": _fact_for_cell("fact_1", dim, session_id="public-session", cell_id="public-session#cell_0000_abcd")}),
    )
    apply_cloud_state_to_user_forest(user_forest, state)

    user_forest.delete_session("public-session")

    assert user_forest._fact_manager.get_fact("fact_1") is None


@pytest.mark.asyncio
async def test_cloud_hydration_registry_uses_fact_occurrences_for_orphans(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    fact = _fact("fact_1", dim)
    occurrence = replace(
        fact.occurrences[0],
        session_id="public-session",
        cell_id="public-session#cell_0000_abcd",
        source_turn_ids=["public-session:0"],
    )
    fact = replace(fact, occurrences=[occurrence])
    state = await load_cloud_user_forest_state(
        tree_store=FakeTreeStore(
            {
                "session:sess_0001": [
                    _node("session-leaf", level="L0", child_ids=("public-session#cell_0000_abcd",), fact_ids=(), tree_id="session:sess_0001", tree_type="session", tree_key="sess_0001", tree_label="sess_0001"),
                ],
                "entity:user": [
                    _node("entity-leaf", level="L0", child_ids=("fact_1",), tree_id="entity:user", tree_key="user", tree_label="user"),
                ],
            }
        ),
        fact_store=FakeFactStore({"fact_1": fact}),
    )
    apply_cloud_state_to_user_forest(user_forest, state)

    user_forest.delete_session("public-session")

    assert user_forest._fact_manager.get_fact("fact_1") is None


@pytest.mark.asyncio
async def test_cloud_hydration_restores_session_alias_map_for_future_ingest(tmp_path) -> None:
    dim = 4
    user_forest = _user_forest(tmp_path, dim)
    state = await load_cloud_user_forest_state(
        tree_store=FakeTreeStore(
            {
                "session:sess_0001": [
                    _node("session-leaf-a", level="L0", child_ids=("alice-session#cell_0000_a",), fact_ids=("fact_1",), tree_id="session:sess_0001", tree_type="session", tree_key="sess_0001", tree_label="sess_0001"),
                ],
                "session:sess_0002": [
                    _node("session-leaf-b", level="L0", child_ids=("bob-session#cell_0000_b",), fact_ids=("fact_2",), tree_id="session:sess_0002", tree_type="session", tree_key="sess_0002", tree_label="sess_0002"),
                ],
                "session:sess_0003": [
                    _node("session-leaf-c", level="L0", child_ids=("carol-session#cell_0000_c",), fact_ids=("fact_3",), tree_id="session:sess_0003", tree_type="session", tree_key="sess_0003", tree_label="sess_0003"),
                ],
                "entity:user": [
                    _node("entity-root", child_ids=("entity-leaf-a", "entity-leaf-b", "entity-leaf-c"), tree_id="entity:user", tree_key="user", tree_label="user"),
                    _node("entity-leaf-a", parent_id="entity-root", level="L0", child_ids=("fact_1",), tree_id="entity:user", tree_key="user", tree_label="user"),
                    _node("entity-leaf-b", parent_id="entity-root", level="L0", child_ids=("fact_2",), tree_id="entity:user", tree_key="user", tree_label="user"),
                    _node("entity-leaf-c", parent_id="entity-root", level="L0", child_ids=("fact_3",), tree_id="entity:user", tree_key="user", tree_label="user"),
                ],
            }
        ),
        fact_store=FakeFactStore({"fact_1": _fact("fact_1", dim), "fact_2": _fact("fact_2", dim), "fact_3": _fact("fact_3", dim)}),
    )
    apply_cloud_state_to_user_forest(user_forest, state)

    assert user_forest._tree_builder.session_alias_map["alice-session"] == "sess_0001"
    assert user_forest._tree_builder.session_alias_map["bob-session"] == "sess_0002"
    del user_forest._tree_builder._session_alias_map["alice-session"]
    assert user_forest._tree_builder._public_session_id("new-session") == "sess_0004"


def test_cloud_hydration_reembeds_fact_when_stored_embedding_dim_mismatches(tmp_path) -> None:
    dim = 4
    user_forest, client = _user_forest_with_client(tmp_path, dim)
    fact = _fact("fact_1", dim)
    fact = replace(fact, metadata={"embedding": [1.0, 0.0]})
    state = CloudUserForestState(
        trees=[],
        facts=[fact],
        summary_cache={},
        node_embeddings={},
    )

    apply_cloud_state_to_user_forest(user_forest, state)

    assert client.embed_calls == 1
    assert user_forest._fact_manager.get_fact("fact_1") is not None


@pytest.mark.asyncio
async def test_cloud_state_core_defaults_to_snapshot_fallback(tmp_path) -> None:
    dim = 4
    source = _user_forest(tmp_path / "source-root", dim)
    source.save()
    payload = build_user_forest_archive(source)
    import hashlib

    manifest = FakeManifest(
        {
            "snapshot_id": "snapshot-1",
            "snapshot_content_sha256": hashlib.sha256(payload).hexdigest(),
            "embedding_model_version": "embed-v1",
            "materialization_fingerprint": "fingerprint",
        }
    )
    target = _user_forest(tmp_path / "target-root", dim)
    core = CloudStateHydratingUserForestCore(
        user_forest=target,
        tree_store=FakeTreeStore({}),
        snapshot_store=FakeSnapshotStore({"snapshot-1": payload}),
    )

    await core.hydrate_from_manifest(manifest)

    assert core.last_hydration_result is not None
    assert core.last_hydration_result.used_snapshot_fallback


@pytest.mark.asyncio
async def test_cloud_state_core_hydrates_complete_cloud_state_without_snapshot(tmp_path) -> None:
    dim = 4
    target = _user_forest(tmp_path / "target-root", dim)
    fact = _fact("fact_1", dim)
    core = CloudStateHydratingUserForestCore(
        user_forest=target,
        tree_store_factory=lambda forest_version: FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("leaf",)),
                    _node("leaf", parent_id="root", level="L0", child_ids=("fact_1",)),
                ]
            }
        ),
        fact_store=FakeFactStore({"fact_1": fact}),
        summary_cache=FakeSummaryCache({_content_key(frozenset({"fact_1"})): "cached root"}),
        browse_embedding_store_factory=lambda forest_version: FakeBrowseEmbeddingStore({"root": _embedding(dim)}),
        snapshot_store=None,
        enable_cloud_state_hydration=True,
    )

    await core.hydrate_from_manifest(FakeManifest({"forest_version": "v7", "embedding_model_version": "embed-v1"}))

    assert core.last_hydration_result is not None
    assert core.last_hydration_result.used_snapshot_fallback is False
    assert target.snapshot_for_materialization().baseline_forest_version == "v7"
    assert core.last_hydration_result.tree_count == 1
    assert core.last_hydration_result.fact_count == 1
    assert target._fact_manager.get_fact("fact_1") is not None
    assert target._node_index.get_embedding("root") is not None


@pytest.mark.asyncio
async def test_cloud_state_core_without_snapshot_surfaces_cloud_read_errors(tmp_path) -> None:
    target = _user_forest(tmp_path / "target-root", 4)
    core = CloudStateHydratingUserForestCore(
        user_forest=target,
        tree_store=FailingTreeStore(),
        snapshot_store=None,
        enable_cloud_state_hydration=True,
        cloud_state_read_attempts=2,
    )

    with pytest.raises(RuntimeError, match="transient tree read failure"):
        await core.hydrate_from_manifest(FakeManifest({"forest_version": "v7", "embedding_model_version": "embed-v1"}))


@pytest.mark.asyncio
async def test_cloud_state_core_falls_back_when_cloud_state_is_incomplete(tmp_path) -> None:
    dim = 4
    source = _user_forest(tmp_path / "source-root", dim)
    source.save()
    payload = build_user_forest_archive(source)
    import hashlib

    manifest = FakeManifest(
        {
            "snapshot_id": "snapshot-1",
            "snapshot_content_sha256": hashlib.sha256(payload).hexdigest(),
            "embedding_model_version": "embed-v1",
            "materialization_fingerprint": "fingerprint",
        }
    )
    target = _user_forest(tmp_path / "target-root", dim)
    core = CloudStateHydratingUserForestCore(
        user_forest=target,
        tree_store=FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("leaf",)),
                    _node("leaf", parent_id="root", level="L0", child_ids=("missing-fact",)),
                ]
            }
        ),
        fact_store=FakeFactStore({}),
        summary_cache=FakeSummaryCache({}),
        browse_embedding_store=FakeBrowseEmbeddingStore({}),
        snapshot_store=FakeSnapshotStore({"snapshot-1": payload}),
        enable_cloud_state_hydration=True,
    )

    await core.hydrate_from_manifest(manifest)

    assert core.last_hydration_result is not None
    assert core.last_hydration_result.used_snapshot_fallback


@pytest.mark.asyncio
async def test_cloud_state_core_allows_missing_summary_cache_entries(tmp_path) -> None:
    dim = 4
    target = _user_forest(tmp_path / "target-root", dim)
    core = CloudStateHydratingUserForestCore(
        user_forest=target,
        tree_store=FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("leaf",)),
                    _node("leaf", parent_id="root", level="L0", child_ids=("fact_1",)),
                ]
            }
        ),
        fact_store=FakeFactStore({"fact_1": _fact("fact_1", dim)}),
        summary_cache=FakeSummaryCache({}),
        browse_embedding_store=FakeBrowseEmbeddingStore({"root": _embedding(dim)}),
        snapshot_store=None,
        enable_cloud_state_hydration=True,
    )

    await core.hydrate_from_manifest(FakeManifest({"forest_version": "v7", "embedding_model_version": "embed-v1"}))

    assert core.last_hydration_result is not None
    assert core.last_hydration_result.used_snapshot_fallback is False
    assert core.last_hydration_result.missing_summary_cache_keys
