"""Memory-aware chat service for the product UI."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Literal

from src.cloud.query_prerequisites import CloudQueryNotReady
from src.cloud.query_service import cloud_query_service_from_env
from src.service.auth import ActorContext
from src.service.schemas import ChatRequest, ChatResponse, MemoryFactResponse, QueryRequest

log = logging.getLogger(__name__)

CHAT_SYSTEM = """You are MemForest, a concise assistant with persistent memory.
Use recalled memory facts when they are relevant, but do not invent memories.
If no memory is available yet, answer naturally and mention that useful details
from the conversation will be extracted into memory in the background.
Preserve user-provided acronyms, role codes, team names, and organization names
verbatim. Do not expand abbreviations such as FDE or EAG unless the user defined
the expansion.
Return JSON with keys: reply, reason."""


class CloudMemoryChatService:
    def __init__(self, *, query_service: Any, model_client: Any) -> None:
        self._query_service = query_service
        self._model_client = model_client

    async def chat(self, actor: ActorContext, *, user_id: str, request: ChatRequest) -> ChatResponse:
        query_id: str | None = None
        reason: str | None = None
        facts: list[MemoryFactResponse] = []
        memory_status: Literal["grounded", "no_forest", "no_facts", "unavailable"] = "unavailable"
        try:
            query = await self._query_service.query(
                actor,
                user_id=user_id,
                request=QueryRequest(
                    question=_recall_question(request),
                    max_facts=request.max_facts,
                    trace_requested=request.trace_requested,
                ),
            )
            query_id = query.query_id
            reason = query.reason
            facts = list(query.facts)
            memory_status = "grounded" if facts else "no_facts"
        except CloudQueryNotReady as exc:
            memory_status = "no_forest" if exc.reason == "committed_manifest_missing" else "unavailable"
            if memory_status == "unavailable":
                log.warning("cloud chat memory recall unavailable: %s", exc.reason)

        try:
            payload = await asyncio.to_thread(
                self._model_client.generate_json,
                system_prompt=CHAT_SYSTEM,
                user_prompt=_build_chat_prompt(request, facts),
                model_name="answer",
                temperature=0.2,
                max_tokens=1200,
                step_label="cloud_chat_answer",
            )
        except Exception:
            log.exception("failed to generate cloud chat reply")
            payload = {}
        reply = str(payload.get("reply") or "").strip()
        model_reason = str(payload.get("reason") or "").strip()
        if not reply:
            reply = _fallback_reply(memory_status=memory_status, facts=facts)
        return ChatResponse(
            reply=reply,
            query_id=query_id,
            reason=model_reason or reason,
            memory_status=memory_status,  # type: ignore[arg-type]
            facts=facts,
        )


def cloud_chat_service_from_env(environ: dict[str, str] | None = None, *, query_service: Any | None = None) -> CloudMemoryChatService | None:
    values = dict(os.environ if environ is None else environ)
    query_service = query_service or cloud_query_service_from_env(values)
    required = [
        "AZURE_CLIENT_ID",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_VERSION",
        "AZURE_OPENAI_CHAT_DEPLOYMENT",
    ]
    if query_service is None or any(not str(values.get(name, "")).strip() for name in required):
        return None
    from azure.identity import ManagedIdentityCredential, get_bearer_token_provider

    from src.cloud.azure_models import AzureOpenAIModelClient
    from src.cloud.config import load_cloud_config

    client_id = values["AZURE_CLIENT_ID"]
    credential = ManagedIdentityCredential(client_id=client_id)
    cloud_config = load_cloud_config(values)
    if cloud_config.azure_openai is None:
        return None
    model_client = AzureOpenAIModelClient(
        cloud_config.azure_openai,
        azure_ad_token_provider=get_bearer_token_provider(credential, "https://cognitiveservices.azure.com/.default"),
    )
    return CloudMemoryChatService(query_service=query_service, model_client=model_client)


def _build_chat_prompt(request: ChatRequest, facts: list[MemoryFactResponse]) -> str:
    recent_turns = [
        {"role": turn.role, "content": turn.content}
        for turn in request.turns[-12:]
        if turn.content.strip()
    ]
    max_facts = request.max_facts if request.max_facts is not None else 8
    memory_facts = [{"fact_id": fact.fact_id, "text": fact.text} for fact in facts[:max_facts]]
    return json.dumps(
        {
            "current_user_message": request.message,
            "recent_conversation": recent_turns,
            "recalled_memory_facts": memory_facts,
        },
        ensure_ascii=False,
    )


def _fallback_reply(*, memory_status: str, facts: list[MemoryFactResponse]) -> str:
    if facts:
        return "I found relevant memory for this, but I could not compose a full response."
    if memory_status == "no_forest":
        return "I can chat now. As we continue, MemForest will extract useful details and build memory in the background."
    return "I can chat now, but memory recall is not available for this turn."


def _recall_question(request: ChatRequest) -> str:
    recent_user_turns = [turn.content.strip() for turn in request.turns[-6:] if turn.role == "user" and turn.content.strip()]
    context = "\n".join(recent_user_turns[-3:])
    return f"{context}\n{request.message}".strip() if context else request.message
