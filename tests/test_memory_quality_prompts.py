from src.build.summary_manager import _max_words_for
from src.build.summary_manager import _prompt_name_for
from src.prompt.answer_prompts import ANSWER_SYSTEM_CLOUD
from src.prompt.extraction_prompt import UNIFIED_MEMORY_EXTRACTION_PROMPT
from src.prompt.tree_prompts import TREE_SUMMARY_ENTITY_USER, build_tree_prompt_manager


def test_extraction_prompt_treats_observation_timestamps_as_provenance() -> None:
    prompt = UNIFIED_MEMORY_EXTRACTION_PROMPT.system

    assert "Observation timestamps are provenance" in prompt
    assert "do NOT rewrite the cell timestamp as" in prompt
    assert "Josh works at Microsoft." in prompt
    assert "cell timestamp inferred as a start date" in prompt
    assert "Josh works at OpenAI as an FDE." in prompt
    assert "expanded without definition" in prompt


def test_extraction_prompt_allows_marked_assistant_advice_context() -> None:
    prompt = UNIFIED_MEMORY_EXTRACTION_PROMPT.system

    assert "assistant_advice_context=true" in prompt
    assert "later user turns accepted or acted on the advice" in prompt
    assert "preserve concrete examples verbatim" in prompt
    assert "screens (phones, tablets, laptops, or TVs)" in prompt
    assert "blue light suppressing melatonin" in prompt
    assert "Do NOT rewrite them as user-authored preferences" in prompt


def test_extraction_prompt_preserves_countable_collection_quantities() -> None:
    prompt = UNIFIED_MEMORY_EXTRACTION_PROMPT.system

    assert "Countable collections and inventories" in prompt
    assert "preserve the exact number and item type" in prompt
    assert "The user owns 3 vintage guitars." in prompt
    assert "Do not drop a quantity" in prompt


def test_cloud_answer_prompt_preserves_preference_avoidance_examples_without_weakening_abstention() -> None:
    prompt = ANSWER_SYSTEM_CLOUD

    assert "For preference, recommendation, or activity-suggestion questions" in prompt
    assert "constraints or avoidances, and stated reasons" in prompt
    assert "phones, tablets, laptops, or TVs" in prompt
    assert "This does not lower the evidence bar" in prompt
    assert "if required evidence is absent, follow the abstention rule" in prompt
    assert "The information provided is not enough to answer this question." in prompt


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
    assert "Observation window for ordering only" in user
    assert "May 29, 2026" not in user
    assert "Do not expand FDE" in system
    assert _max_words_for("entity") == 110
    assert _max_words_for("session") == 80


def test_user_profile_label_still_uses_user_entity_prompt() -> None:
    assert _prompt_name_for("entity", "User profile", tree_id="entity:user") == "tree_summary_entity_user"
    assert _prompt_name_for("entity", "OpenAI", tree_id="entity:openai") == "tree_summary_entity_generic"
