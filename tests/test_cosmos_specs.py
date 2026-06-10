from __future__ import annotations

import re
from pathlib import Path

from src.cloud.cosmos_specs import cosmos_container_specs_by_name


def test_cosmos_specs_match_bicep_container_names_and_partition_keys() -> None:
    bicep = Path("infra/main.bicep").read_text(encoding="utf-8")
    specs = cosmos_container_specs_by_name()

    for name in ["sessions", "turns", "tree_nodes", "fact_locations", "browse_embeddings", "leases"]:
        assert name in specs
        assert specs[name].partition_key_path == "/tenant_user_key"
        assert f"'{name}'" in _array_block(bicep, "userScopedCosmosContainers")

    facts_resource = _resource_block(bicep, "factsCosmosContainer")
    assert "facts" in specs
    assert specs["facts"].partition_key_path == "/tenant_user_key"
    assert "name: 'facts'" in facts_resource
    assert "'/tenant_user_key'" in facts_resource
    assert "path: '/embedding/?'" in facts_resource
    assert "path: '/embedding/*'" in facts_resource

    for name in ["manifests", "deletions"]:
        assert name in specs
        assert specs[name].partition_key_path == "/tenant_id"
        assert f"'{name}'" in _array_block(bicep, "tenantScopedCosmosContainers")

    assert "name: 'jobs'" in bicep
    assert "defaultTtl: 2592000" in bicep
    assert specs["jobs"].partition_key_path == "/tenant_id"
    assert "'/tenant_id'" in _resource_block(bicep, "jobsCosmosContainer")
    assert specs["jobs"].default_ttl == 2592000

    assert "name: 'query_traces'" in bicep
    assert "defaultTtl: 604800" in bicep
    assert specs["query_traces"].partition_key_path == "/tenant_id"
    assert "'/tenant_id'" in _resource_block(bicep, "queryTracesCosmosContainer")
    assert specs["query_traces"].default_ttl == 604800


def test_durable_flex_memory_supports_full_session_materialization() -> None:
    bicep = Path("infra/main.bicep").read_text(encoding="utf-8")
    durable_app = _resource_block(bicep, "durableFunctionApp")

    assert "instanceMemoryMB: 4096" in durable_app


def test_durable_servicebus_offload_and_worker_sizing_are_configurable() -> None:
    bicep = Path("infra/main.bicep").read_text(encoding="utf-8")
    durable_settings = _variable_block(bicep, "durableBaseFunctionAppSettings")
    worker_app = _resource_block(bicep, "workerContainerApp")

    assert "param durableActivityOffloadMode string = 'local'" in bicep
    assert "name: 'AZURE_MEMFOREST_DURABLE_ACTIVITY_OFFLOAD_MODE'" in durable_settings
    assert "value: durableActivityOffloadMode" in durable_settings
    assert "name: 'AZURE_SERVICEBUS_FULLY_QUALIFIED_NAMESPACE'" in durable_settings
    assert "name: 'AZURE_SERVICEBUS_WRITES_QUEUE_NAME'" in durable_settings
    assert "serviceBusSenderAssignment" in _resource_block(bicep, "durableFunctionApp")
    assert "param workerCpu string = '0.5'" in bicep
    assert "param workerMemory string = '1Gi'" in bicep
    assert "param workerSessionLockRenewalSeconds int = 300" in bicep
    assert "param workerJobLeaseTtlSeconds int = 300" in bicep
    assert "param workerUserLeaseTtlSeconds int = 300" in bicep
    assert "param workerIngestTimeoutSeconds int = 1800" in bicep
    assert "cpu: json(workerCpu)" in worker_app
    assert "memory: workerMemory" in worker_app
    assert "name: 'AZURE_MEMFOREST_WORKER_SESSION_LOCK_RENEWAL_SECONDS'" in worker_app
    assert "value: string(workerSessionLockRenewalSeconds)" in worker_app
    assert "name: 'AZURE_MEMFOREST_JOB_LEASE_TTL_SECONDS'" in worker_app
    assert "value: string(workerJobLeaseTtlSeconds)" in worker_app
    assert "name: 'AZURE_MEMFOREST_USER_LEASE_TTL_SECONDS'" in worker_app
    assert "value: string(workerUserLeaseTtlSeconds)" in worker_app
    assert "name: 'AZURE_MEMFOREST_INGEST_TIMEOUT_SECONDS'" in worker_app
    assert "value: string(workerIngestTimeoutSeconds)" in worker_app


def _array_block(text: str, variable_name: str) -> str:
    match = re.search(rf"var {variable_name} = \[(.*?)\]", text, re.DOTALL)
    assert match is not None
    return match.group(1)


def _resource_block(text: str, resource_name: str) -> str:
    match = re.search(rf"resource {resource_name} .*?\n\}}", text, re.DOTALL)
    assert match is not None
    return match.group(0)


def _variable_block(text: str, variable_name: str) -> str:
    match = re.search(rf"var {variable_name} = \[(.*?)\]\n", text, re.DOTALL)
    assert match is not None
    return match.group(1)
