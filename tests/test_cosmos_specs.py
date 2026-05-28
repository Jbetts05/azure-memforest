from __future__ import annotations

import re
from pathlib import Path

from src.cloud.cosmos_specs import cosmos_container_specs_by_name


def test_cosmos_specs_match_bicep_container_names_and_partition_keys() -> None:
    bicep = Path("infra/main.bicep").read_text(encoding="utf-8")
    specs = cosmos_container_specs_by_name()

    for name in ["sessions", "turns", "facts", "tree_nodes", "fact_locations", "browse_embeddings", "leases"]:
        assert name in specs
        assert specs[name].partition_key_path == "/tenant_user_key"
        assert f"'{name}'" in _array_block(bicep, "userScopedCosmosContainers")

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


def _array_block(text: str, variable_name: str) -> str:
    match = re.search(rf"var {variable_name} = \[(.*?)\]", text, re.DOTALL)
    assert match is not None
    return match.group(1)


def _resource_block(text: str, resource_name: str) -> str:
    match = re.search(rf"resource {resource_name} .*?\n\}}", text, re.DOTALL)
    assert match is not None
    return match.group(0)
