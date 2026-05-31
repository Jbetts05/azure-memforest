"""Azure OpenAI model client adapter."""

from __future__ import annotations

import os
import time
from typing import Any

from openai import AzureOpenAI

from src.api.json_parse import parse_json_object
from src.cloud.config import AzureOpenAIConfig
from src.observability.model_telemetry import record_model_call


class AzureOpenAIModelClient:
    """ModelClient adapter backed by Azure OpenAI deployments.

    `model_name` in `generate_json` is interpreted as a deployment role when it
    matches `config.chat_deployments`; otherwise it is treated as a deployment
    name directly. This keeps current call sites compatible while allowing
    role-based enterprise routing.
    """

    def __init__(
        self,
        config: AzureOpenAIConfig,
        *,
        openai_client: Any | None = None,
        azure_ad_token_provider: Any | None = None,
    ) -> None:
        self.config = config
        self.client = openai_client or self._build_client(azure_ad_token_provider)

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
        role = model_name or "default"
        deployment = self._chat_deployment_for(model_name)
        kwargs: dict[str, Any] = {
            "model": deployment,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.0 if temperature is None else temperature,
            "top_p": 1.0 if top_p is None else top_p,
            "response_format": {"type": "json_object"},
            "timeout": timeout,
        }
        if _uses_max_completion_tokens(deployment):
            token_limit = 4096 if max_tokens is None else max_tokens
            kwargs["max_completion_tokens"] = token_limit
        else:
            token_limit = 1024 if max_tokens is None else max_tokens
            kwargs["max_tokens"] = token_limit
        start = time.perf_counter()
        response = self.client.chat.completions.create(**kwargs)
        record_model_call(
            role=role,
            deployment=deployment,
            operation="chat",
            elapsed_seconds=time.perf_counter() - start,
        )
        content = response.choices[0].message.content or "{}"
        return parse_json_object(content)

    def embed_texts(self, texts: list[str], *, batch_size: int = 256) -> list[list[float]]:
        if not texts:
            return []
        if not self.config.embedding_deployment:
            raise ValueError("Azure OpenAI embedding deployment is not configured.")
        embeddings: list[list[float]] = []
        for index in range(0, len(texts), batch_size):
            batch = texts[index : index + batch_size]
            start = time.perf_counter()
            response = self.client.embeddings.create(
                model=self.config.embedding_deployment,
                input=batch,
            )
            record_model_call(
                role="embedding",
                deployment=self.config.embedding_deployment,
                operation="embedding",
                elapsed_seconds=time.perf_counter() - start,
                item_count=len(batch),
            )
            embeddings.extend(list(item.embedding) for item in response.data)
        if self.config.embedding_dimension is not None:
            for embedding in embeddings:
                if len(embedding) != self.config.embedding_dimension:
                    raise ValueError(
                        f"embedding dimension {len(embedding)} != configured "
                        f"{self.config.embedding_dimension}"
                    )
        return embeddings

    def _chat_deployment_for(self, model_name: str | None) -> str:
        role_or_deployment = model_name or "default"
        deployment = self.config.chat_deployments.get(role_or_deployment)
        if deployment is not None:
            return deployment.deployment_name
        if role_or_deployment != "default":
            return role_or_deployment
        raise ValueError("Azure OpenAI default chat deployment is not configured.")

    def _build_client(self, azure_ad_token_provider: Any | None) -> AzureOpenAI:
        if self.config.use_managed_identity:
            if azure_ad_token_provider is None:
                raise RuntimeError(
                    "use_managed_identity=True requires azure-identity integration; "
                    "install azure-identity and pass an azure_ad_token_provider, "
                    "or pass an openai_client explicitly."
                )
            return AzureOpenAI(
                azure_endpoint=self.config.endpoint,
                api_version=self.config.api_version,
                azure_ad_token_provider=azure_ad_token_provider,
            )
        api_key = os.getenv(self.config.api_key_env, "")
        if not api_key:
            raise RuntimeError(f"Azure OpenAI API key env var is not set: {self.config.api_key_env}")
        return AzureOpenAI(
            azure_endpoint=self.config.endpoint,
            api_version=self.config.api_version,
            api_key=api_key,
        )


def _uses_max_completion_tokens(deployment_name: str) -> bool:
    return deployment_name.lower().replace("_", "-").startswith("gpt-5")
