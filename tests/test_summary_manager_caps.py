from __future__ import annotations

from src.build.summary_manager import SummaryManager
from src.build.tree_types import SummaryRequest
from src.config.tree_config import SummaryManagerConfig


class FakeChatClient:
    def __init__(self) -> None:
        self.calls = 0

    def generate_json(self, **kwargs):
        self.calls += 1
        return {"summary": "ok"}


def _request(index: int) -> SummaryRequest:
    return SummaryRequest(
        request_id=f"r{index}",
        tree_id="tree",
        node_id=f"n{index}",
        tree_type="entity",
        tree_label="User profile",
        level=1,
        input_text="input",
        time_start=1.0,
        time_end=2.0,
    )


def test_summary_manager_can_skip_requests_when_capped_at_zero() -> None:
    client = FakeChatClient()
    manager = SummaryManager(
        chat_client=client,
        config=SummaryManagerConfig(max_requests_per_flush=0),
        model_name="model",
    )

    assert manager.generate_summaries([_request(1), _request(2)]) == []
    assert client.calls == 0


def test_summary_manager_limits_requests_per_flush() -> None:
    client = FakeChatClient()
    manager = SummaryManager(
        chat_client=client,
        config=SummaryManagerConfig(max_requests_per_flush=1),
        model_name="model",
    )

    results = manager.generate_summaries([_request(1), _request(2)])

    assert len(results) == 1
    assert client.calls == 1

