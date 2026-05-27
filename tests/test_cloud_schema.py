from __future__ import annotations

import pytest

from src.cloud import (
    SCHEMA_VERSION,
    CloudRecordMetadata,
    DeletionMetadata,
    EmbeddingMetadata,
    FactLocation,
    RecallCandidate,
    StoredTreeNode,
    partition_key,
    require_tenant_scope,
)


def test_cloud_record_metadata_requires_tenant_and_forest_version() -> None:
    metadata = CloudRecordMetadata(
        schema_version=SCHEMA_VERSION,
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
    )

    require_tenant_scope(metadata)

    with pytest.raises(ValueError, match="tenant_id"):
        require_tenant_scope(
            CloudRecordMetadata(schema_version=SCHEMA_VERSION, tenant_id="", forest_version="v1")
        )
    with pytest.raises(ValueError, match="schema_version"):
        require_tenant_scope(
            CloudRecordMetadata(schema_version=0, tenant_id="tenant-1", forest_version="v1")
        )
    with pytest.raises(ValueError, match="forest_version"):
        require_tenant_scope(
            CloudRecordMetadata(schema_version=SCHEMA_VERSION, tenant_id="tenant-1", forest_version="")
        )


def test_partition_key_uses_tenant_and_optional_user() -> None:
    assert partition_key(tenant_id="tenant-1") == "tenant-1"
    assert partition_key(tenant_id="tenant-1", user_id="user-1") == "tenant-1:user-1"
    with pytest.raises(ValueError, match="tenant_id"):
        partition_key(tenant_id="")
    with pytest.raises(ValueError, match="tenant_id"):
        partition_key(tenant_id="tenant:1")
    with pytest.raises(ValueError, match="user_id"):
        partition_key(tenant_id="tenant-1", user_id="user:1")


def test_embedding_and_deletion_metadata_shapes() -> None:
    embedding = EmbeddingMetadata(
        embedding_model_version="embed-v1",
        embedding_dimension=1536,
        embedding_role="fact",
    )
    deletion = DeletionMetadata(
        tombstoned_at_unix=123.0,
        deletion_reason="session_deleted",
    )

    assert embedding.embedding_role == "fact"
    assert deletion.hard_delete_after_unix is None


def test_cloud_contract_records_include_schema_version() -> None:
    candidate = RecallCandidate(tree_id="tree", node_id="node", score=1.0)
    tree_node = StoredTreeNode(tree_id="tree", node_id="node", level="L1", summary="summary")
    location = FactLocation(
        fact_id="fact",
        tree_id="tree",
        node_id="node",
        tree_type="entity",
        level="L0",
    )

    assert candidate.schema_version == SCHEMA_VERSION
    assert tree_node.schema_version == SCHEMA_VERSION
    assert location.schema_version == SCHEMA_VERSION
