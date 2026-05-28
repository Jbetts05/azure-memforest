"""Cosmos DB container specifications shared by IaC tests and adapters."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CosmosContainerSpec:
    name: str
    partition_key_path: str
    default_ttl: int | None = None


DEFAULT_COSMOS_CONTAINERS: tuple[CosmosContainerSpec, ...] = (
    CosmosContainerSpec(name="sessions", partition_key_path="/tenant_user_key"),
    CosmosContainerSpec(name="turns", partition_key_path="/tenant_user_key"),
    CosmosContainerSpec(name="facts", partition_key_path="/tenant_user_key"),
    CosmosContainerSpec(name="tree_nodes", partition_key_path="/tenant_user_key"),
    CosmosContainerSpec(name="fact_locations", partition_key_path="/tenant_user_key"),
    CosmosContainerSpec(name="browse_embeddings", partition_key_path="/tenant_user_key"),
    CosmosContainerSpec(name="leases", partition_key_path="/tenant_user_key"),
    CosmosContainerSpec(name="manifests", partition_key_path="/tenant_id"),
    CosmosContainerSpec(name="jobs", partition_key_path="/tenant_id", default_ttl=60 * 60 * 24 * 30),
    CosmosContainerSpec(name="deletions", partition_key_path="/tenant_id"),
    CosmosContainerSpec(name="query_traces", partition_key_path="/tenant_id", default_ttl=60 * 60 * 24 * 7),
)


def cosmos_container_specs_by_name() -> dict[str, CosmosContainerSpec]:
    return {spec.name: spec for spec in DEFAULT_COSMOS_CONTAINERS}
