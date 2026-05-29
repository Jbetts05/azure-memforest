import pytest

from src.cloud.chat_service import CloudMemoryChatService
from src.cloud.query_prerequisites import CloudQueryNotReady
from src.service.auth import ActorContext, TenantContext
from src.service.schemas import ChatRequest, MemoryFactResponse, QueryResponse


def _actor() -> ActorContext:
    return ActorContext(tenant=TenantContext(tenant_id="tenant-1"), actor_id="user-1", scopes=(), roles=(), auth_scheme="test")


class FakeModel:
    def __init__(self) -> None:
        self.prompt = ""

    def generate_json(self, *, user_prompt: str, **_kwargs):
        self.prompt = user_prompt
        return {"reply": "Bob moved to Davis.", "reason": "Used recalled memory."}


@pytest.mark.asyncio
async def test_chat_service_uses_recalled_memory_facts() -> None:
    class FakeQuery:
        async def query(self, actor, *, user_id, request):
            return QueryResponse(
                query_id="query-1",
                answer="Bob moved to Davis.",
                facts=[MemoryFactResponse(fact_id="fact-1", text="Bob moved to Davis.")],
            )

    model = FakeModel()
    service = CloudMemoryChatService(query_service=FakeQuery(), model_client=model)

    response = await service.chat(_actor(), user_id="user-1", request=ChatRequest(message="Where did Bob move?"))

    assert response.reply == "Bob moved to Davis."
    assert response.query_id == "query-1"
    assert response.memory_status == "grounded"
    assert response.facts[0].fact_id == "fact-1"
    assert "Bob moved to Davis." in model.prompt


@pytest.mark.asyncio
async def test_chat_service_still_replies_for_first_run_without_forest() -> None:
    class MissingForestQuery:
        async def query(self, actor, *, user_id, request):
            raise CloudQueryNotReady("committed_manifest_missing")

    service = CloudMemoryChatService(query_service=MissingForestQuery(), model_client=FakeModel())

    response = await service.chat(_actor(), user_id="user-1", request=ChatRequest(message="My name is Alice."))

    assert response.reply == "Bob moved to Davis."
    assert response.query_id is None
    assert response.memory_status == "no_forest"
    assert response.facts == []
