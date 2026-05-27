"""OpenAI-compatible chat and embedding clients for vNext."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from openai import OpenAI

from src.api.json_parse import parse_json_object
from src.config.api import ChatCompletionConfig, EmbeddingConfig
from src.extraction.pipeline import JsonExtractionBackend

if TYPE_CHECKING:
    from src.logger.api_log import ApiCallLogger


class OpenAIChatClient:
    """Thin wrapper over OpenAI-compatible chat completions."""

    def __init__(
        self,
        config: ChatCompletionConfig,
        *,
        client: OpenAI | None = None,
        api_logger: "ApiCallLogger | None" = None,
    ) -> None:
        self.config = config
        self.client = client or OpenAI(
            base_url=str(config.url).replace("://0.0.0.0", "://127.0.0.1"),
            api_key=config.key,
        )
        self._api_logger = api_logger

    def generate_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model_name: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        top_p: float | None = None,
        timeout: float | None = None,
        step_label: str = "llm",
        trace: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        used_model = model_name or self.config.model_name
        payload = {
            "model": used_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": self.config.temperature if temperature is None else temperature,
            "max_tokens": self.config.max_token if max_tokens is None else max_tokens,
            "top_p": self.config.top_p if top_p is None else top_p,
            "response_format": {"type": "json_object"},
        }
        if self.config.topk is not None:
            payload["extra_body"] = {"top_k": self.config.topk}

        start_time = time.time()
        create_kwargs: dict[str, Any] = {"timeout": timeout} if timeout is not None else {}
        response = self.client.chat.completions.create(**payload, **create_kwargs)
        end_time = time.time()

        content = response.choices[0].message.content or "{}"
        usage = getattr(response, "usage", None)
        prompt_tokens = usage.prompt_tokens if usage else -1
        completion_tokens = usage.completion_tokens if usage else -1

        if self._api_logger is not None:
            self._api_logger.log(
                step_label=step_label,
                model_name=used_model,
                request_id=None if trace is None else str(trace.get("request_id") or ""),
                session_id=None if trace is None else str(trace.get("session_id") or ""),
                cell_id=None if trace is None else str(trace.get("cell_id") or ""),
                cell_index=None if trace is None else _maybe_int(trace.get("cell_index")),
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_content=content,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                start_time=start_time,
                end_time=end_time,
            )

        return parse_json_object(content)


class OpenAIEmbeddingClient:
    """Thin wrapper over OpenAI-compatible embeddings."""

    def __init__(self, config: EmbeddingConfig, *, client: OpenAI | None = None) -> None:
        self.config = config
        self.client = client or OpenAI(
            base_url=str(config.url).replace("://0.0.0.0", "://127.0.0.1"),
            api_key=config.key,
        )

    def embed_texts(self, texts: list[str], *, batch_size: int = 256) -> list[list[float]]:
        if not texts:
            return []
        if len(texts) <= batch_size:
            response = self.client.embeddings.create(
                model=self.config.model_name,
                input=texts,
            )
            return [list(item.embedding) for item in response.data]
        # Batch to avoid overwhelming the embedding server
        all_embeddings: list[list[float]] = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            response = self.client.embeddings.create(
                model=self.config.model_name,
                input=batch,
            )
            all_embeddings.extend(list(item.embedding) for item in response.data)
        return all_embeddings


class OpenAIJsonBackend(JsonExtractionBackend):
    """Extraction backend backed by an OpenAI-compatible chat client."""

    def __init__(
        self,
        chat_client: OpenAIChatClient,
        *,
        model_name: str,
        temperature: float,
        max_tokens: int,
        top_p: float,
        step_label: str = "extraction",
    ) -> None:
        self.chat_client = chat_client
        self.model_name = model_name
        self.temperature = float(temperature)
        self.max_tokens = int(max_tokens)
        self.top_p = float(top_p)
        self.step_label = step_label

    def generate_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        trace: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.chat_client.generate_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            model_name=self.model_name,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            top_p=self.top_p,
            step_label=self.step_label,
            trace=trace,
        )


def _maybe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
