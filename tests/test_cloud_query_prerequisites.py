from __future__ import annotations

import pytest

from src.cloud.query_prerequisites import CloudQueryNotReady, verify_cloud_query_prerequisites


def test_cloud_query_prerequisites_fail_closed_by_default() -> None:
    status = verify_cloud_query_prerequisites()

    assert status.ok is False
    assert "facts_store" in status.missing
    assert "parity_gate" in status.missing
    with pytest.raises(CloudQueryNotReady):
        status.raise_if_not_ready()


def test_cloud_query_prerequisites_pass_when_all_ready() -> None:
    status = verify_cloud_query_prerequisites(
        facts_store_ready=True,
        tree_node_store_ready=True,
        fact_location_index_ready=True,
        manifest_store_ready=True,
        recall_index_ready=True,
        browse_embedding_store_ready=True,
        model_client_ready=True,
        trace_sink_ready=True,
        embedding_dimension_configured=True,
        embedding_model_version_configured=True,
        committed_manifest_exists=True,
        parity_gate_defined=True,
    )

    assert status.ok is True
    assert status.missing == ()
    status.raise_if_not_ready()
