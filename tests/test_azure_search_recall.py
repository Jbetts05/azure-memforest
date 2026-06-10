from __future__ import annotations

import pytest

from src.cloud.azure_search import AzureSearchRecallIndex, TenantScopeRequired
from src.cloud.contracts import RecallDocument
from src.cloud.search_specs import recall_document_key


class FakeSearchClient:
    def __init__(self) -> None:
        self.uploaded = []
        self.deleted = []
        self.merged = []
        self.search_calls = []
        self.results = []

    def upload_documents(self, documents):
        self.uploaded.extend(documents)

    def delete_documents(self, documents):
        self.deleted.extend(documents)

    def merge_documents(self, documents):
        self.merged.extend(documents)

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
    assert client.uploaded[0]["content_sha256"]
    assert client.uploaded[0]["valid_from_generation"] == 0
    assert client.uploaded[0]["valid_from_forest_version"] == "v1"

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
        filters={"tenant_id": "tenant-1", "tree_type": "entity", "forest_version": "g7-current"},
    )

    assert candidates[0].score == 0.75
    assert candidates[0].tree_id == "tree-1"
    assert "tenant_id eq 'tenant-1'" in client.search_calls[0]["filter"]
    assert "tree_type eq 'entity'" in client.search_calls[0]["filter"]
    assert "valid_from_generation le 7" in client.search_calls[0]["filter"]
    assert "valid_to_generation gt 7" in client.search_calls[0]["filter"]


def test_azure_search_upsert_delta_closes_superseded_recall_documents() -> None:
    client = FakeSearchClient()
    index = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=2,
        search_client=client,
    )
    close_ref = type(
        "Ref",
        (),
        {
            "metadata": {
                "id": "old-doc",
                "forest_version": "g6-old",
            }
        },
    )()
    new_doc = RecallDocument(
        tenant_id="tenant-1",
        user_id="user-1",
        tree_id="tree-1",
        node_id="node-1",
        tree_type="entity",
        forest_version="g7-current",
        embedding_model_version="embed-v1",
        summary="summary",
        embedding=(0.1, 0.2),
    )

    index.upsert_documents_delta([new_doc], close_refs=[close_ref], forest_version="g7-current")

    assert client.uploaded[0]["valid_from_generation"] == 7
    assert client.merged == [
        {
            "id": "old-doc",
            "valid_to_generation": 7,
            "valid_to_forest_version": "g7-current",
        }
    ]


def test_azure_search_upsert_delta_does_not_close_current_upsert_id() -> None:
    client = FakeSearchClient()
    index = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=2,
        search_client=client,
    )
    new_doc = RecallDocument(
        tenant_id="tenant-1",
        user_id="user-1",
        tree_id="tree-1",
        node_id="node-1",
        tree_type="entity",
        forest_version="g7-current",
        embedding_model_version="embed-v1",
        summary="summary",
        embedding=(0.1, 0.2),
    )
    current_id = recall_document_key(tenant_id="tenant-1", tree_id="tree-1", node_id="node-1", forest_version="g7-current")
    close_ref = type("Ref", (), {"metadata": {"id": current_id}})()

    index.upsert_documents_delta([new_doc], close_refs=[close_ref], forest_version="g7-current")

    assert len(client.uploaded) == 1
    assert client.merged == []


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


def test_recall_content_hash_is_stable_across_forest_versions() -> None:
    first_client = FakeSearchClient()
    second_client = FakeSearchClient()
    index = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=2,
        search_client=first_client,
    )
    second = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=2,
        search_client=second_client,
    )
    doc_v1 = _document()
    doc_v2 = RecallDocument(
        tenant_id=doc_v1.tenant_id,
        user_id=doc_v1.user_id,
        tree_id=doc_v1.tree_id,
        node_id=doc_v1.node_id,
        tree_type=doc_v1.tree_type,
        forest_version="v2",
        embedding_model_version=doc_v1.embedding_model_version,
        summary=doc_v1.summary,
        embedding=doc_v1.embedding,
    )

    index.upsert_documents([doc_v1])
    second.upsert_documents([doc_v2])

    assert first_client.uploaded[0]["id"] != second_client.uploaded[0]["id"]
    assert first_client.uploaded[0]["content_sha256"] == second_client.uploaded[0]["content_sha256"]


def test_recall_content_hash_changes_with_embedding_model_or_vector() -> None:
    client = FakeSearchClient()
    index = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=2,
        search_client=client,
    )
    base = _document()
    model_changed = RecallDocument(
        tenant_id=base.tenant_id,
        user_id=base.user_id,
        tree_id=base.tree_id,
        node_id=base.node_id,
        tree_type=base.tree_type,
        forest_version=base.forest_version,
        embedding_model_version="embed-v2",
        summary=base.summary,
        embedding=base.embedding,
    )
    vector_changed = RecallDocument(
        tenant_id=base.tenant_id,
        user_id=base.user_id,
        tree_id=base.tree_id,
        node_id=base.node_id,
        tree_type=base.tree_type,
        forest_version=base.forest_version,
        embedding_model_version=base.embedding_model_version,
        summary=base.summary,
        embedding=(0.2, 0.3),
    )

    index.upsert_documents([base, model_changed, vector_changed])

    hashes = {item["content_sha256"] for item in client.uploaded}
    assert len(hashes) == 3
