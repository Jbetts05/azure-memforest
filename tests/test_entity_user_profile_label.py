from src.build.entity_router import ENTITY_USER_TREE_ID, _normalise_entity
from src.build.tree_builder import TreeBuilder
from src.config.config import load_default_config
from src.utils.types import ManagedFact


class FakeClient:
    def embed_texts(self, texts):
        return [[1.0, 0.0, 0.0] for _ in texts]


def test_generic_entities_are_filtered() -> None:
    for label in ["user", "assistant", "listener", "the user", "me", "you", "unknown"]:
        assert _normalise_entity(label) is None


def test_user_origin_tree_uses_profile_label_not_raw_user() -> None:
    builder = TreeBuilder(config=load_default_config(), chat_client=FakeClient(), embedding_client=FakeClient())
    fact = ManagedFact(
        fact_id="fact_1",
        fact_text="Josh works at OpenAI as an FDE.",
        embedding_id="emb-1",
        origin="user",
        semantic_role="state",
        entities=["user", "Josh", "OpenAI"],
        topics=["work"],
        attribute_keys=["role"],
        domain_keys=["work"],
        collection_keys=[],
        detail_level="atomic",
        confidence=1.0,
        first_session_id="session-1",
        first_cell_id="cell-1",
    )

    builder.ingest_structure(new_facts=[fact], cells=[], cell_to_facts={})

    tree = builder.trees[ENTITY_USER_TREE_ID]
    assert tree.label == "User profile"
