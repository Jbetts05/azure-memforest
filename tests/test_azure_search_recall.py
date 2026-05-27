from __future__ import annotations

import pytest

from src.cloud.azure_search import AzureSearchRecallIndex, TenantScopeRequired
from src.cloud.contracts import RecallDocument
from src.cloud.search_specs import recall_document_key


class FakeSearchClient:
    def __init__(self) -> None:
        self.uploaded = []
        self.deleted = []
        self.search_calls = []
        self.results = []

    def upload_documents(self, documents):
        self.uploaded.extend(documents)

    def delete_documents(self, documents):
        self.deleted.extend(documents)

    def search(self, **kwargs):
        self.search_calls.append(kwargs)
        return list(self.results)


def _document() -> RecallDocument:
    return RecallDocument(
        tenant_id="tenant-1",
        user_id="user-1",
        tree_id="tree-1",
        node_id="node-1",
        tree_type="entity",
        forest_version="v1",
        embedding_model_version="embed-v1",
        summary="summary",
        embedding=(0.1, 0.2),
    )


def test_azure_search_upsert_requires_embedding_and_dimension() -> None:
    client = FakeSearchClient()
    index = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=2,
        search_client=client,
    )

    index.upsert_documents([_document()])

    assert client.uploaded[0]["id"] == recall_document_key(
        tenant_id="tenant-1",
        tree_id="tree-1",
        node_id="node-1",
        forest_version="v1",
    )
    assert client.uploaded[0]["embedding"] == [0.1, 0.2]

    bad = RecallDocument(
        tenant_id="tenant-1",
        user_id="user-1",
        tree_id="tree-1",
        node_id="node-1",
        tree_type="entity",
        forest_version="v1",
        embedding_model_version="embed-v1",
        summary="summary",
        embedding=(0.1,),
    )
    with pytest.raises(ValueError, match="embedding dimension"):
        index.upsert_documents([bad])


def test_azure_search_search_requires_tenant_filter_and_maps_results(monkeypatch) -> None:
    client = FakeSearchClient()
    client.results = [
        {
            "@search.score": 0.75,
            "tenant_id": "tenant-1",
            "user_id": "user-1",
            "tree_id": "tree-1",
            "node_id": "node-1",
            "tree_type": "entity",
            "forest_version": "v1",
            "embedding_model_version": "embed-v1",
            "schema_version": 1,
        }
    ]
    index = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=2,
        search_client=client,
    )
    monkeypatch.setattr("src.cloud.azure_search._vector_query", lambda query_embedding, top_k: {"vector": query_embedding, "k": top_k})

    with pytest.raises(TenantScopeRequired):
        index.search([0.1, 0.2], top_k=1)

    candidates = index.search(
        [0.1, 0.2],
        top_k=1,
        filters={"tenant_id": "tenant-1", "tree_type": "entity"},
    )

    assert candidates[0].score == 0.75
    assert candidates[0].tree_id == "tree-1"
    assert "tenant_id eq 'tenant-1'" in client.search_calls[0]["filter"]
    assert "tree_type eq 'entity'" in client.search_calls[0]["filter"]


def test_azure_search_delete_by_tree_is_tenant_scoped() -> None:
    client = FakeSearchClient()
    client.results = [{"id": "doc-1"}, {"id": "doc-2"}]
    index = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=2,
        search_client=client,
    )

    with pytest.raises(TenantScopeRequired):
        index.delete_by_tree("tree-1")

    index.delete_by_tree("tree-1", tenant_id="tenant-1")

    assert client.deleted == [{"id": "doc-1"}, {"id": "doc-2"}]
    assert "tenant_id eq 'tenant-1'" in client.search_calls[0]["filter"]
    assert "tree_id eq 'tree-1'" in client.search_calls[0]["filter"]


def test_recall_document_key_is_length_prefixed_to_avoid_delimiter_collisions() -> None:
    first = recall_document_key(
        tenant_id="a|b",
        tree_id="c",
        node_id="d",
        forest_version="e",
    )
    second = recall_document_key(
        tenant_id="a",
        tree_id="b|c",
        node_id="d",
        forest_version="e",
    )

    assert first != second


def test_azure_search_upsert_rejects_missing_tenant() -> None:
    client = FakeSearchClient()
    index = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=2,
        search_client=client,
    )
    doc = RecallDocument(
        tenant_id="",
        user_id="user-1",
        tree_id="tree-1",
        node_id="node-1",
        tree_type="entity",
        forest_version="v1",
        embedding_model_version="embed-v1",
        summary="summary",
        embedding=(0.1, 0.2),
    )

    with pytest.raises(ValueError, match="tenant_id"):
        index.upsert_documents([doc])
