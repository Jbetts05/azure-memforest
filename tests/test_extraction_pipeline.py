from __future__ import annotations

from src.extraction.pipeline import ChunkExtractionPipeline


class QuantityDroppingBackend:
    def generate_json(self, **kwargs):
        return {
            "cell_summary": "The user discussed a collection.",
            "memory_items": [
                {
                    "fact_text": "The user has been collecting rare books for a while.",
                    "participants": ["user"],
                    "origin": "user",
                    "semantic_role": "state",
                    "entities": [],
                    "topics": ["collection"],
                    "time_text": "",
                    "time_start": None,
                    "time_end": None,
                    "attribute_keys": [],
                    "domain_keys": [],
                    "collection_keys": ["rare_books"],
                }
            ],
        }


class ZeroTimeBackend:
    def generate_json(self, **kwargs):
        return {
            "cell_summary": "The user stated a timed fact.",
            "memory_items": [
                {
                    "fact_text": "The user has 1300 followers on Instagram.",
                    "participants": ["user"],
                    "origin": "user",
                    "semantic_role": "state",
                    "entities": ["user"],
                    "topics": ["social media"],
                    "time_text": "",
                    "time_start": 0,
                    "time_end": 0,
                    "attribute_keys": [],
                    "domain_keys": [],
                    "collection_keys": [],
                }
            ],
        }


def test_extraction_pipeline_falls_back_to_cell_dates_for_zero_llm_times() -> None:
    pipeline = ChunkExtractionPipeline(backend=ZeroTimeBackend())

    result = pipeline.extract_session(
        "session-1",
        [
            {
                "role": "user",
                "content_id": "turn-1",
                "content": "I have close to 1300 followers on Instagram now.",
                "timestamp": "2023/05/25 (Thu) 09:28",
            }
        ],
    )

    [item] = result.memory_items
    [cell] = result.cells
    assert item.time_start == cell.time_start
    assert item.time_end == cell.time_end
    assert item.time_start and item.time_start > 0
    assert "2023" in item.time_text


def test_extraction_pipeline_supplements_countable_collection_quantities() -> None:
    pipeline = ChunkExtractionPipeline(backend=QuantityDroppingBackend())

    result = pipeline.extract_session(
        "session-1",
        [
            {
                "role": "user",
                "content_id": "turn-1",
                "content": 'I have been collecting rare books for a while and have a small but valuable collection of 5 books.',
                "timestamp": "2023/05/22 (Mon) 10:00",
            }
        ],
    )

    facts = [item.fact_text for item in result.memory_items]
    assert "The user has been collecting rare books for a while." in facts
    assert "The user has 5 rare books." in facts


def test_extraction_pipeline_does_not_apply_rare_to_unrelated_count() -> None:
    pipeline = ChunkExtractionPipeline(backend=QuantityDroppingBackend())

    result = pipeline.extract_session(
        "session-1",
        [
            {
                "role": "user",
                "content_id": "turn-1",
                "content": "I have 5 books on my desk. Separately, I collect rare coins.",
                "timestamp": "2023/05/22 (Mon) 10:00",
            }
        ],
    )

    facts = [item.fact_text for item in result.memory_items]
    assert "The user has 5 rare books." not in facts
    assert "The user has 5 books." not in facts


def test_extraction_pipeline_does_not_turn_generic_experience_into_collection() -> None:
    pipeline = ChunkExtractionPipeline(backend=QuantityDroppingBackend())

    result = pipeline.extract_session(
        "session-1",
        [
            {
                "role": "user",
                "content_id": "turn-1",
                "content": "I have 12 years of experience working with antiques.",
                "timestamp": "2023/05/22 (Mon) 10:00",
            }
        ],
    )

    facts = [item.fact_text for item in result.memory_items]
    assert not any("12 years of experience" in fact for fact in facts)
