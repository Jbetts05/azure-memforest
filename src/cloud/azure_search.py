"""Azure AI Search RecallIndex adapter."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from src.cloud.contracts import RecallCandidate, RecallDocument
from src.cloud.search_specs import VECTOR_FIELD_NAME, recall_document_key


class TenantScopeRequired(ValueError):
    pass


class AzureSearchRecallIndex:
    def __init__(
        self,
        *,
        endpoint: str,
        index_name: str,
        vector_dimensions: int,
        search_client: Any | None = None,
        index_client: Any | None = None,
        credential: Any | None = None,
    ) -> None:
        if vector_dimensions <= 0:
            raise ValueError("vector_dimensions must be positive.")
        self.endpoint = endpoint
        self.index_name = index_name
        self.vector_dimensions = vector_dimensions
        self._search_client = search_client
        self._index_client = index_client
        self._credential = credential

    def search(
        self,
        query_embedding: list[float],
        *,
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[RecallCandidate]:
        filters = filters or {}
        tenant_id = str(filters.get("tenant_id", "")).strip()
        if not tenant_id:
            raise TenantScopeRequired("tenant_id filter is required for Azure AI Search recall.")
        vector_queries = [_vector_query(query_embedding, top_k=top_k)]
        results = self._client().search(
            search_text=None,
            vector_queries=vector_queries,
            filter=_filter_expression(filters),
            top=top_k,
        )
        candidates: list[RecallCandidate] = []
        for item in results:
            candidates.append(
                RecallCandidate(
                    tree_id=str(item["tree_id"]),
                    node_id=str(item["node_id"]),
                    score=float(item.get("@search.score", 0.0)),
                    tree_type=item.get("tree_type"),
                    tenant_id=item.get("tenant_id"),
                    user_id=item.get("user_id"),
                    forest_version=item.get("forest_version"),
                    embedding_model_version=item.get("embedding_model_version"),
                    schema_version=int(item.get("schema_version", 1)),
                    metadata={"source": "azure_ai_search"},
                )
            )
        return candidates

    def upsert_documents(self, documents: list[RecallDocument]) -> None:
        payload = [_document_to_search_payload(document, self.vector_dimensions) for document in documents]
        if payload:
            self._client().upload_documents(payload)

    def delete_by_tree(self, tree_id: str, *, tenant_id: str | None = None) -> None:
        if not tenant_id:
            raise TenantScopeRequired("tenant_id is required when deleting recall documents by tree.")
        for _ in range(100):
            results = list(self._client().search(
                search_text="*",
                filter=_filter_expression({"tenant_id": tenant_id, "tree_id": tree_id}),
                select=["id"],
                top=1000,
            ))
            if not results:
                break
            keys = [{"id": item["id"]} for item in results]
            for chunk in _chunks(keys, 1000):
                self._client().delete_documents(chunk)
            if len(results) < 1000:
                break
        else:
            raise RuntimeError("delete_by_tree exceeded maximum Azure AI Search deletion iterations.")

    def delete_by_user(self, *, tenant_id: str, user_id: str) -> None:
        if not tenant_id or not user_id:
            raise TenantScopeRequired("tenant_id and user_id are required when deleting recall documents by user.")
        for _ in range(100):
            results = list(self._client().search(
                search_text="*",
                filter=_filter_expression({"tenant_id": tenant_id, "user_id": user_id}),
                select=["id"],
                top=1000,
            ))
            if not results:
                break
            keys = [{"id": item["id"]} for item in results]
            for chunk in _chunks(keys, 1000):
                self._client().delete_documents(chunk)
            if len(results) < 1000:
                break
        else:
            raise RuntimeError("delete_by_user exceeded maximum Azure AI Search deletion iterations.")

    def ensure_index(self) -> None:
        index_client = self._index_admin_client()
        try:
            index = index_client.get_index(self.index_name)
        except Exception as exc:
            if not _is_not_found(exc):
                raise
            try:
                index_client.create_index(build_recall_search_index(self.index_name, self.vector_dimensions))
                return
            except Exception as create_exc:
                if not _is_conflict(create_exc):
                    raise
                index = index_client.get_index(self.index_name)
        validate_recall_search_index(index, vector_dimensions=self.vector_dimensions)

    def _client(self):
        if self._search_client is not None:
            return self._search_client
        from azure.identity import DefaultAzureCredential
        from azure.search.documents import SearchClient

        credential = self._credential or DefaultAzureCredential(exclude_interactive_browser_credential=True)
        self._search_client = SearchClient(self.endpoint, self.index_name, credential)
        return self._search_client

    def _index_admin_client(self):
        if self._index_client is not None:
            return self._index_client
        from azure.identity import DefaultAzureCredential
        from azure.search.documents.indexes import SearchIndexClient

        credential = self._credential or DefaultAzureCredential(exclude_interactive_browser_credential=True)
        self._index_client = SearchIndexClient(self.endpoint, credential)
        return self._index_client


def build_recall_search_index(index_name: str, vector_dimensions: int):
    if vector_dimensions <= 1:
        raise ValueError("vector_dimensions must be configured from the embedding deployment.")
    from azure.search.documents.indexes.models import (
        HnswAlgorithmConfiguration,
        SearchField,
        SearchFieldDataType,
        SearchIndex,
        SearchableField,
        SimpleField,
        VectorSearch,
        VectorSearchProfile,
    )

    return SearchIndex(
        name=index_name,
        fields=[
            SimpleField(name="id", type=SearchFieldDataType.String, key=True, filterable=True),
            SimpleField(name="tenant_id", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="user_id", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="tree_id", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="node_id", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="tree_type", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="forest_version", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="embedding_model_version", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="schema_version", type=SearchFieldDataType.Int32, filterable=True),
            SimpleField(name="content_sha256", type=SearchFieldDataType.String, filterable=True),
            SearchableField(name="summary", type=SearchFieldDataType.String),
            SearchField(
                name=VECTOR_FIELD_NAME,
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                searchable=True,
                vector_search_dimensions=vector_dimensions,
                vector_search_profile_name="hnsw",
            ),
        ],
        vector_search=VectorSearch(
            algorithms=[HnswAlgorithmConfiguration(name="hnsw-algorithm")],
            profiles=[VectorSearchProfile(name="hnsw", algorithm_configuration_name="hnsw-algorithm")],
        ),
    )


def validate_recall_search_index(index: Any, *, vector_dimensions: int) -> None:
    fields = {field.name: field for field in index.fields}
    required = {
        "id",
        "tenant_id",
        "user_id",
        "tree_id",
        "node_id",
        "tree_type",
        "forest_version",
        "embedding_model_version",
        "schema_version",
        "summary",
        VECTOR_FIELD_NAME,
    }
    missing = sorted(required - set(fields))
    if missing:
        raise ValueError(f"Azure AI Search index is missing required fields: {missing}")
    vector_field = fields[VECTOR_FIELD_NAME]
    actual_dimensions = getattr(vector_field, "vector_search_dimensions", None)
    if actual_dimensions != vector_dimensions:
        raise ValueError(
            f"Azure AI Search vector dimension {actual_dimensions} != configured {vector_dimensions}"
        )


def _document_to_search_payload(document: RecallDocument, vector_dimensions: int) -> dict[str, Any]:
    for field_name in ("tenant_id", "user_id", "tree_id", "node_id", "forest_version"):
        if not str(getattr(document, field_name, "")).strip():
            raise ValueError(f"RecallDocument.{field_name} is required for Azure AI Search upsert.")
    if not document.embedding:
        raise ValueError("RecallDocument.embedding is required for Azure AI Search upsert.")
    if len(document.embedding) != vector_dimensions:
        raise ValueError(f"embedding dimension {len(document.embedding)} != index dimension {vector_dimensions}")
    return {
        "id": recall_document_key(
            tenant_id=document.tenant_id,
            tree_id=document.tree_id,
            node_id=document.node_id,
            forest_version=document.forest_version,
        ),
        "tenant_id": document.tenant_id,
        "user_id": document.user_id,
        "tree_id": document.tree_id,
        "node_id": document.node_id,
        "tree_type": document.tree_type,
        "forest_version": document.forest_version,
        "embedding_model_version": document.embedding_model_version,
        "schema_version": document.schema_version,
        "content_sha256": document.content_sha256 or _recall_content_sha256(document),
        "summary": document.summary,
        VECTOR_FIELD_NAME: list(document.embedding),
    }


def _recall_content_sha256(document: RecallDocument) -> str:
    payload = {
        "tenant_id": document.tenant_id,
        "user_id": document.user_id,
        "tree_id": document.tree_id,
        "node_id": document.node_id,
        "tree_type": document.tree_type,
        "embedding_model_version": document.embedding_model_version,
        "summary": document.summary,
        "embedding": [float(value) for value in document.embedding],
    }
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _filter_expression(filters: dict[str, Any]) -> str:
    clauses = []
    for field in ("tenant_id", "user_id", "tree_id", "tree_type", "forest_version", "embedding_model_version"):
        value = filters.get(field)
        if value:
            clauses.append(f"{field} eq '{_escape_filter_value(str(value))}'")
    return " and ".join(clauses)


def _escape_filter_value(value: str) -> str:
    return value.replace("'", "''")


def _vector_query(query_embedding: list[float], *, top_k: int):
    from azure.search.documents.models import VectorizedQuery

    return VectorizedQuery(
        vector=query_embedding,
        k_nearest_neighbors=top_k,
        fields=VECTOR_FIELD_NAME,
    )


def _chunks(values: list[dict[str, str]], size: int):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _is_not_found(exc: Exception) -> bool:
    return exc.__class__.__name__ == "ResourceNotFoundError" or getattr(exc, "status_code", None) == 404


def _is_conflict(exc: Exception) -> bool:
    return exc.__class__.__name__ == "ResourceExistsError" or getattr(exc, "status_code", None) == 409
