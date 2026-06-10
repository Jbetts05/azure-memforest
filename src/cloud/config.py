"""Cloud configuration models for Azure-MemForest."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class AzureModelDeployment:
    role: str
    deployment_name: str
    model_version: str = ""


@dataclass(frozen=True)
class AzureOpenAIConfig:
    endpoint: str
    api_version: str
    chat_deployments: dict[str, AzureModelDeployment] = field(default_factory=dict)
    embedding_deployment: str = ""
    embedding_dimension: int | None = None
    embedding_model_version: str = ""
    use_managed_identity: bool = True
    api_key_env: str = "AZURE_OPENAI_API_KEY"


@dataclass(frozen=True)
class ProjectionGcConfig:
    enabled: bool = False
    retention_count: int = 0
    live_delete_enabled: bool = False

    def __post_init__(self) -> None:
        if self.retention_count < 0:
            raise ValueError("retention_count must not be negative")
        if self.enabled and self.retention_count < 1:
            raise ValueError("retention_count must be positive when projection GC is enabled")
        if self.live_delete_enabled and not self.enabled:
            raise ValueError("live_delete_enabled requires projection GC to be enabled")


@dataclass(frozen=True)
class CloudConfig:
    environment: str
    tenant_id: str
    subscription_id: str
    resource_group: str
    location: str
    azure_openai: AzureOpenAIConfig | None = None
    projection_gc: ProjectionGcConfig = field(default_factory=ProjectionGcConfig)


def load_cloud_config(environ: Mapping[str, str] | None = None) -> CloudConfig:
    values = os.environ if environ is None else environ
    endpoint = str(values.get("AZURE_OPENAI_ENDPOINT", "")).strip()
    api_version = str(values.get("AZURE_OPENAI_API_VERSION", "2024-10-21")).strip()
    embedding_dimension = _optional_int(values.get("AZURE_OPENAI_EMBEDDING_DIMENSION"))
    azure_openai = None
    if endpoint:
        azure_openai = AzureOpenAIConfig(
            endpoint=endpoint,
            api_version=api_version,
            chat_deployments=_load_chat_deployments(values),
            embedding_deployment=str(values.get("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "")).strip(),
            embedding_dimension=embedding_dimension,
            embedding_model_version=str(values.get("AZURE_OPENAI_EMBEDDING_MODEL_VERSION", "")).strip(),
            use_managed_identity=_bool(values.get("AZURE_OPENAI_USE_MANAGED_IDENTITY"), default=True),
            api_key_env=str(values.get("AZURE_OPENAI_API_KEY_ENV", "AZURE_OPENAI_API_KEY")).strip(),
        )
    return CloudConfig(
        environment=str(values.get("AZURE_MEMFOREST_ENV", "local") or "local"),
        tenant_id=str(values.get("AZURE_TENANT_ID", "")).strip(),
        subscription_id=str(values.get("AZURE_SUBSCRIPTION_ID", "")).strip(),
        resource_group=str(values.get("AZURE_RESOURCE_GROUP", "")).strip(),
        location=str(values.get("AZURE_LOCATION", "")).strip(),
        azure_openai=azure_openai,
        projection_gc=ProjectionGcConfig(
            enabled=_bool(values.get("AZURE_MEMFOREST_PROJECTION_GC_ENABLED"), default=False),
            retention_count=_int(values.get("AZURE_MEMFOREST_PROJECTION_GC_RETENTION_COUNT"), default=0),
            live_delete_enabled=_bool(
                values.get("AZURE_MEMFOREST_PROJECTION_GC_LIVE_DELETE_ENABLED"),
                default=False,
            ),
        ),
    )


def _load_chat_deployments(values: Mapping[str, str]) -> dict[str, AzureModelDeployment]:
    deployments: dict[str, AzureModelDeployment] = {}
    for role in ("extraction", "dedupe", "summarization", "planner", "browse", "answer"):
        env_name = f"AZURE_OPENAI_{role.upper()}_DEPLOYMENT"
        deployment_name = str(values.get(env_name, "")).strip()
        if deployment_name:
            deployments[role] = AzureModelDeployment(
                role=role,
                deployment_name=deployment_name,
                model_version=str(values.get(f"AZURE_OPENAI_{role.upper()}_MODEL_VERSION", "")).strip(),
            )
    default_deployment = str(values.get("AZURE_OPENAI_CHAT_DEPLOYMENT", "")).strip()
    if default_deployment and "default" not in deployments:
        deployments["default"] = AzureModelDeployment(
            role="default",
            deployment_name=default_deployment,
            model_version=str(values.get("AZURE_OPENAI_CHAT_MODEL_VERSION", "")).strip(),
        )
    return deployments


def _optional_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _int(value: str | None, *, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _bool(value: str | None, *, default: bool) -> bool:
    if value is None or value == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
