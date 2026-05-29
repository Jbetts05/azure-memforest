from src.forest.user_forest import UserForest
from src.config.config import load_default_config
from src.utils.types import MemCell, SessionExtractionResult


class FakeClient:
    pass


class EmptyExtractionPipeline:
    def extract_session(self, session_id, turns):
        return SessionExtractionResult(
            session_id=session_id,
            cells=[
                MemCell(
                    cell_id=f"{session_id}#cell_0000",
                    session_id=session_id,
                    cell_index=0,
                    start_turn_index=0,
                    end_turn_index=0,
                    turn_ids=["turn-1"],
                    content_ids=["turn-1"],
                    speaker_names=["user"],
                    time_start=1.0,
                    time_end=1.0,
                    text="user: how can you help me",
                )
            ],
            memory_items=[],
        )


class EmptyFactManager:
    def add_session_result(self, result):
        class Result:
            canonical_fact_ids = []
            inserted_count = 0
            merged_count = 0

        return Result()

    def get_fact(self, fact_id):
        return None

    def iter_facts(self):
        return []

    def fact_embeddings(self):
        return {}


def test_factless_ingest_does_not_create_trees(tmp_path):
    forest = UserForest(
        user_id="user-1",
        user_dir=tmp_path,
        config=load_default_config(),
        chat_client=FakeClient(),
        embedding_client=FakeClient(),
    )
    forest._extraction_pipeline = EmptyExtractionPipeline()
    forest._fact_manager = EmptyFactManager()

    result = forest.ingest_session("session-1", [{"role": "user", "content": "how can you help me"}])
    snapshot = forest.snapshot_for_materialization()

    assert result.cells_extracted == 1
    assert result.facts_inserted == 0
    assert result.trees_updated == []
    assert snapshot.facts == []
    assert snapshot.trees == []
