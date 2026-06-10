from __future__ import annotations

import pytest

from src.cloud.azure_search import (
    AzureSearchRecallIndex,
    build_recall_search_index,
    validate_recall_search_index,
)


class NotFoundError(Exception):
    status_code = 404


class ExistsError(Exception):
    status_code = 409


class FakeIndexClient:
    def __init__(self, existing=None, conflict_on_create=False):
        self.index = existing
        self.created = None
        self.updated = None
        self.conflict_on_create = conflict_on_create

    def get_index(self, name):
        if self.index is None:
            raise NotFoundError()
        return self.index

    def create_index(self, index):
        if self.conflict_on_create:
            raise ExistsError()
        self.created = index
        self.index = index

    def create_or_update_index(self, index):
        self.updated = index
        self.index = index


def test_build_recall_search_index_serializes_vector_schema() -> None:
    index = build_recall_search_index("memforest-recall", 1536)
    data = index.serialize() if hasattr(index, "serialize") else index.as_dict()

    fields = {field["name"]: field for field in data["fields"]}
    assert fields["embedding"]["dimensions"] == 1536
    assert fields["embedding"]["vectorSearchProfile"] == "hnsw"
    assert data["vectorSearch"]["profiles"][0]["algorithm"] == "hnsw-algorithm"


def test_ensure_index_creates_when_missing() -> None:
    fake = FakeIndexClient()
    recall = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=1536,
        search_client=object(),
        index_client=fake,
    )

    recall.ensure_index()

    assert fake.created is not None
    validate_recall_search_index(fake.created, vector_dimensions=1536)


def test_ensure_index_validates_existing_dimension() -> None:
    index = build_recall_search_index("memforest-recall", 1536)
    recall = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=3072,
        search_client=object(),
        index_client=FakeIndexClient(existing=index),
    )

    with pytest.raises(ValueError, match="vector dimension"):
        recall.ensure_index()


def test_ensure_index_updates_existing_index_missing_mvcc_fields() -> None:
    index = build_recall_search_index("memforest-recall", 1536)
    index.fields = [
        field
        for field in index.fields
        if field.name
        not in {
            "valid_from_generation",
            "valid_from_forest_version",
            "valid_to_generation",
            "valid_to_forest_version",
        }
    ]
    fake = FakeIndexClient(existing=index)
    recall = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=1536,
        search_client=object(),
        index_client=fake,
    )

    recall.ensure_index()

    assert fake.updated is not None
    validate_recall_search_index(fake.updated, vector_dimensions=1536)


def test_ensure_index_handles_create_race_by_refetching() -> None:
    index = build_recall_search_index("memforest-recall", 1536)
    fake = FakeIndexClient(existing=index, conflict_on_create=True)
    calls = {"count": 0}

    def get_index(name):
        calls["count"] += 1
        if calls["count"] == 1:
            raise NotFoundError()
        return index

    fake.get_index = get_index
    recall = AzureSearchRecallIndex(
        endpoint="https://search.example",
        index_name="memforest-recall",
        vector_dimensions=1536,
        search_client=object(),
        index_client=fake,
    )

    recall.ensure_index()

    assert calls["count"] == 2


def test_build_recall_search_index_rejects_placeholder_dimension() -> None:
    with pytest.raises(ValueError, match="embedding deployment"):
        build_recall_search_index("memforest-recall", 1)
