from __future__ import annotations

from src.cloud import InMemoryFactLocationIndex, InMemorySummaryCache
from tests.contracts.helpers import (
    assert_fact_location_index_contract,
    assert_summary_cache_contract,
)


def test_in_memory_fact_location_index_contract() -> None:
    assert_fact_location_index_contract(InMemoryFactLocationIndex)


def test_in_memory_summary_cache_contract() -> None:
    assert_summary_cache_contract(InMemorySummaryCache)
