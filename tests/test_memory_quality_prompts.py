from src.build.summary_manager import _max_words_for
from src.prompt.extraction_prompt import UNIFIED_MEMORY_EXTRACTION_PROMPT
from src.prompt.tree_prompts import TREE_SUMMARY_ENTITY_USER, build_tree_prompt_manager


def test_extraction_prompt_treats_observation_timestamps_as_provenance() -> None:
    prompt = UNIFIED_MEMORY_EXTRACTION_PROMPT.system

    assert "Observation timestamps are provenance" in prompt
    assert "do NOT rewrite the cell timestamp as" in prompt
    assert "Josh works at Microsoft." in prompt
    assert "cell timestamp inferred as a start date" in prompt


def test_entity_summary_prompt_blocks_inferred_start_dates_and_filler() -> None:
    system, _ = build_tree_prompt_manager().render(
        TREE_SUMMARY_ENTITY_USER.name,
        {
            "max_words": _max_words_for("entity"),
            "time_range_text": "May 29, 2026",
            "input_text": "Josh works at Microsoft.",
        },
    )

    assert "1-2 short sentences" in system
    assert "observation timestamps are provenance only" in system
    assert "Do not write negative filler" in system
    _, user = build_tree_prompt_manager().render(
        TREE_SUMMARY_ENTITY_USER.name,
        {
            "max_words": _max_words_for("entity"),
            "time_range_text": "May 29, 2026",
            "input_text": "Josh works at Microsoft.",
        },
    )
    assert "Facts observed between May 29, 2026" in user
    assert _max_words_for("entity") == 110
    assert _max_words_for("session") == 80
