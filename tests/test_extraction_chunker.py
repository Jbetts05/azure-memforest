from __future__ import annotations

from src.extraction.chunker import chunk_session


def test_chunker_renders_assistant_advice_marker_without_changing_cell_id() -> None:
    base_turns = [
        {
            "role": "assistant",
            "content_id": "a1",
            "content": "Avoid screens before bed.",
            "timestamp": "2023/05/30 (Tue) 15:46",
            "metadata": {},
        }
    ]
    marked_turns = [
        {
            **base_turns[0],
            "metadata": {"assistant_advice_context": True},
        }
    ]

    base = chunk_session("session-1", base_turns)[0]
    marked = chunk_session("session-1", marked_turns)[0]

    assert base.cell_id == marked.cell_id
    assert "[meta] assistant_advice_context=true" not in base.text
    assert "[meta] assistant_advice_context=true" in marked.text


def test_chunker_does_not_render_advice_marker_for_user_or_string_flags() -> None:
    user_turn = {
        "role": "user",
        "content": "I avoid screens before bed.",
        "metadata": {"assistant_advice_context": True},
    }
    string_flag_turn = {
        "role": "assistant",
        "content": "Avoid screens before bed.",
        "metadata": {"assistant_advice_context": "true"},
    }

    assert "[meta] assistant_advice_context=true" not in chunk_session("s1", [user_turn])[0].text
    assert "[meta] assistant_advice_context=true" not in chunk_session("s2", [string_flag_turn])[0].text
