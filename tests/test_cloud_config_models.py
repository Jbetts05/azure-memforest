from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from src.api.json_parse import parse_json_object
from src.cloud.azure_models import AzureOpenAIModelClient
from src.cloud.config import AzureModelDeployment, AzureOpenAIConfig, load_cloud_config
from src.observability.model_telemetry import collect_model_telemetry, record_model_call


class _Message:
    content = '{"ok": true}'


class _Choice:
    message = _Message()


class _ChatResponse:
    choices = [_Choice()]


class _EmbeddingItem:
    def __init__(self, embedding):
        self.embedding = embedding


class _EmbeddingResponse:
    def __init__(self, embeddings):
        self.data = [_EmbeddingItem(embedding) for embedding in embeddings]


class _FakeCompletions:
    def __init__(self):
        self.last_model = ""
        self.last_kwargs = {}

    def create(self, **kwargs):
        self.last_model = kwargs["model"]
        self.last_kwargs = kwargs
        return _ChatResponse()


class _FakeEmbeddings:
    def __init__(self, embeddings):
        self._embeddings = embeddings
        self.last_model = ""

    def create(self, **kwargs):
        self.last_model = kwargs["model"]
        return _EmbeddingResponse(self._embeddings[: len(kwargs["input"])])


class _FakeChat:
    def __init__(self):
        self.completions = _FakeCompletions()


class _FakeClient:
    def __init__(self, embeddings=None):
        self.chat = _FakeChat()
        self.embeddings = _FakeEmbeddings(embeddings or [[1.0, 2.0]])


def test_load_cloud_config_reads_safe_env_without_api_key_value() -> None:
    config = load_cloud_config(
        {
            "AZURE_MEMFOREST_ENV": "dev",
            "AZURE_TENANT_ID": "tenant",
            "AZURE_SUBSCRIPTION_ID": "subscription",
            "AZURE_RESOURCE_GROUP": "azure-memforest",
            "AZURE_LOCATION": "eastus2",
            "AZURE_OPENAI_ENDPOINT": "https://example.openai.azure.com",
            "AZURE_OPENAI_API_VERSION": "2024-10-21",
            "AZURE_OPENAI_CHAT_DEPLOYMENT": "chat-deployment",
            "AZURE_OPENAI_EMBEDDING_DEPLOYMENT": "embedding-deployment",
            "AZURE_OPENAI_EMBEDDING_DIMENSION": "2",
            "AZURE_OPENAI_API_KEY_ENV": "CUSTOM_KEY_ENV",
        }
    )

    assert config.environment == "dev"
    assert config.resource_group == "azure-memforest"
    assert config.azure_openai is not None
    assert config.azure_openai.api_key_env == "CUSTOM_KEY_ENV"
    assert config.azure_openai.embedding_dimension == 2
    assert config.azure_openai.chat_deployments["default"].deployment_name == "chat-deployment"


def test_azure_model_client_routes_role_to_deployment_and_parses_json() -> None:
    fake_client = _FakeClient()
    config = AzureOpenAIConfig(
        endpoint="https://example.openai.azure.com",
        api_version="2024-10-21",
        chat_deployments={
            "planner": AzureModelDeployment(role="planner", deployment_name="planner-deployment"),
        },
        use_managed_identity=True,
    )
    client = AzureOpenAIModelClient(config, openai_client=fake_client)

    result = client.generate_json(
        system_prompt="system",
        user_prompt="user",
        model_name="planner",
    )

    assert result == {"ok": True}
    assert fake_client.chat.completions.last_model == "planner-deployment"
    assert fake_client.chat.completions.last_kwargs["max_tokens"] == 1024
    assert "max_completion_tokens" not in fake_client.chat.completions.last_kwargs
    assert fake_client.chat.completions.last_kwargs["timeout"] == 120.0


def test_azure_model_client_preserves_explicit_timeout() -> None:
    fake_client = _FakeClient()
    config = AzureOpenAIConfig(
        endpoint="https://example.openai.azure.com",
        api_version="2024-10-21",
        chat_deployments={
            "planner": AzureModelDeployment(role="planner", deployment_name="planner-deployment"),
        },
        use_managed_identity=True,
    )
    client = AzureOpenAIModelClient(config, openai_client=fake_client)

    client.generate_json(system_prompt="system", user_prompt="user", model_name="planner", timeout=7.5)

    assert fake_client.chat.completions.last_kwargs["timeout"] == 7.5


def test_azure_model_client_uses_max_completion_tokens_for_gpt5_deployments() -> None:
    fake_client = _FakeClient()
    config = AzureOpenAIConfig(
        endpoint="https://example.cognitiveservices.azure.com",
        api_version="2024-10-21",
        chat_deployments={
            "planner": AzureModelDeployment(role="planner", deployment_name="gpt-5.4"),
        },
        use_managed_identity=True,
    )
    client = AzureOpenAIModelClient(config, openai_client=fake_client)

    result = client.generate_json(
        system_prompt="system",
        user_prompt="user",
        model_name="planner",
        max_tokens=256,
    )

    assert result == {"ok": True}
    assert fake_client.chat.completions.last_model == "gpt-5.4"
    assert fake_client.chat.completions.last_kwargs["max_completion_tokens"] == 256
    assert "max_tokens" not in fake_client.chat.completions.last_kwargs


def test_azure_model_client_uses_larger_default_token_limit_for_gpt5_deployments() -> None:
    fake_client = _FakeClient()
    config = AzureOpenAIConfig(
        endpoint="https://example.cognitiveservices.azure.com",
        api_version="2024-10-21",
        chat_deployments={
            "planner": AzureModelDeployment(role="planner", deployment_name="gpt-5.4"),
        },
        use_managed_identity=True,
    )
    client = AzureOpenAIModelClient(config, openai_client=fake_client)

    client.generate_json(
        system_prompt="system",
        user_prompt="user",
        model_name="planner",
    )

    assert fake_client.chat.completions.last_kwargs["max_completion_tokens"] == 4096


def test_azure_model_client_embeddings_validate_dimension() -> None:
    fake_client = _FakeClient(embeddings=[[1.0, 2.0], [3.0, 4.0]])
    config = AzureOpenAIConfig(
        endpoint="https://example.openai.azure.com",
        api_version="2024-10-21",
        embedding_deployment="embedding-deployment",
        embedding_dimension=2,
    )
    client = AzureOpenAIModelClient(config, openai_client=fake_client)

    embeddings = client.embed_texts(["a", "b"])

    assert embeddings == [[1.0, 2.0], [3.0, 4.0]]
    assert fake_client.embeddings.last_model == "embedding-deployment"


def test_azure_model_client_records_model_call_telemetry() -> None:
    fake_client = _FakeClient(embeddings=[[1.0, 2.0]])
    config = AzureOpenAIConfig(
        endpoint="https://example.openai.azure.com",
        api_version="2024-10-21",
        chat_deployments={
            "extraction": AzureModelDeployment(role="extraction", deployment_name="fast-memory"),
        },
        embedding_deployment="embedding-deployment",
        embedding_dimension=2,
    )
    client = AzureOpenAIModelClient(config, openai_client=fake_client)

    with collect_model_telemetry() as telemetry:
        client.generate_json(system_prompt="system", user_prompt="user", model_name="extraction")
        client.embed_texts(["a"])

    assert telemetry["total_calls"] == 2
    assert telemetry["total_started_calls"] == 2
    assert telemetry["in_flight_calls"] == 0
    assert telemetry["calls"]["chat:extraction:fast-memory"]["count"] == 1
    assert telemetry["calls"]["embedding:embedding:embedding-deployment"]["item_count"] == 1


def test_model_telemetry_records_calls_from_child_threads() -> None:
    with collect_model_telemetry() as telemetry:
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(
                pool.map(
                    lambda _: record_model_call(
                        role="dedupe",
                        deployment="fast-memory",
                        operation="chat",
                        elapsed_seconds=0.01,
                    ),
                    range(5),
                )
            )

    assert telemetry["total_calls"] == 5
    assert telemetry["calls"]["chat:dedupe:fast-memory"]["count"] == 5


def test_azure_model_client_rejects_embedding_dimension_mismatch() -> None:
    fake_client = _FakeClient(embeddings=[[1.0, 2.0, 3.0]])
    config = AzureOpenAIConfig(
        endpoint="https://example.openai.azure.com",
        api_version="2024-10-21",
        embedding_deployment="embedding-deployment",
        embedding_dimension=2,
    )
    client = AzureOpenAIModelClient(config, openai_client=fake_client)

    with pytest.raises(ValueError, match="embedding dimension"):
        client.embed_texts(["a"])


def test_azure_model_client_managed_identity_requires_provider_or_client() -> None:
    config = AzureOpenAIConfig(
        endpoint="https://example.openai.azure.com",
        api_version="2024-10-21",
        use_managed_identity=True,
    )

    with pytest.raises(RuntimeError, match="azure-identity"):
        AzureOpenAIModelClient(config)


def test_azure_model_client_builds_client_with_timeout_and_retry_env(monkeypatch) -> None:
    created = {}

    class FakeAzureOpenAI:
        def __init__(self, **kwargs):
            created.update(kwargs)

    monkeypatch.setattr("src.cloud.azure_models.AzureOpenAI", FakeAzureOpenAI)
    monkeypatch.setenv("AZURE_MEMFOREST_MODEL_TIMEOUT_SECONDS", "33")
    monkeypatch.setenv("AZURE_MEMFOREST_MODEL_MAX_RETRIES", "0")
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "secret")
    config = AzureOpenAIConfig(
        endpoint="https://example.openai.azure.com",
        api_version="2024-10-21",
        chat_deployments={
            "default": AzureModelDeployment(role="default", deployment_name="gpt-5.4-mini-memory"),
        },
        api_key_env="AZURE_OPENAI_API_KEY",
        use_managed_identity=False,
    )

    AzureOpenAIModelClient(config)

    assert created["timeout"] == 33.0
    assert created["max_retries"] == 0


def test_parse_json_object_is_shared_and_handles_fenced_json() -> None:
    assert parse_json_object('```json\n{"ok": true}\n```') == {"ok": True}
