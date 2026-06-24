from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.service.auth import ActorContext, TenantContext
from src.service.jobs import build_write_job_envelope
from src.config.config import load_default_config
from src.worker.materialization import MaterializationHandlerConfig
from src.worker.factory import (
    ScopedMaterializationHandler,
    cosmos_materialization_store_factory,
    fixed_materialization_config_factory,
    hydrating_user_forest_core_factory,
    missing_worker_env,
)


class FakeDatabase:
    def __init__(self):
        self.requested = []

    def get_container_client(self, name):
        self.requested.append(name)
        return {"container": name}


class FakeCore:
    pass


class RecordingStoreFactory:
    def __init__(self):
        self.envelopes = []

    def __call__(self, envelope):
        self.envelopes.append(envelope)
        return "stores"


class RecordingCoreFactory:
    def __init__(self):
        self.envelopes = []

    def __call__(self, envelope):
        self.envelopes.append(envelope)
        return FakeCore()


def _envelope():
    return build_write_job_envelope(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        session_id="session-1",
        job_type="ingest_session",
        idempotency_key="idem-1",
        payload={"turns": [{"role": "user", "content": "hello"}]},
    )


def test_cosmos_materialization_store_factory_scopes_by_envelope() -> None:
    database = FakeDatabase()
    stores = cosmos_materialization_store_factory(
        database_client=database,
        recall_index="recall",
        embedding_model_version="embed-v1",
    )(_envelope())

    tree_store = stores.tree_node_store_factory("v1")
    location_index = stores.fact_location_index_factory("v1")
    browse_store = stores.browse_embedding_store_factory("v1")

    assert stores.recall_index == "recall"
    assert stores.lease_store.tenant_id == "tenant-1"
    assert stores.source_session_store.user_id == "user-1"
    assert tree_store.forest_version == "v1"
    assert location_index.forest_version == "v1"
    assert browse_store.forest_version == "v1"
    assert browse_store.embedding_model_version == "embed-v1"
    assert set(database.requested) >= {
        "leases",
        "sessions",
        "turns",
        "facts",
        "manifests",
        "deletions",
        "tree_nodes",
        "fact_locations",
        "browse_embeddings",
    }


def test_fixed_materialization_config_factory_sets_owner_and_embedding_version() -> None:
    config = fixed_materialization_config_factory(
        owner_id="worker-1",
        embedding_model_version="embed-v1",
        enable_tree_scoped_projection_writes=True,
        enable_partial_hydration_shadow=True,
        enable_partial_hydration_equivalence_shadow=True,
        enable_partial_hydration_guarded_switch=True,
        enable_pre_ingest_route_prediction_prepass=True,
        enable_partial_hydration_dual_compute_shadow=True,
    )(_envelope())

    assert config.owner_id == "worker-1"
    assert config.embedding_model_version == "embed-v1"
    assert config.snapshot_policy == "always"
    assert config.enable_tree_scoped_projection_writes is True
    assert config.enable_partial_hydration_shadow is True
    assert config.enable_partial_hydration_equivalence_shadow is True
    assert config.enable_partial_hydration_guarded_switch is True
    assert config.enable_pre_ingest_route_prediction_prepass is True
    assert config.enable_partial_hydration_dual_compute_shadow is True


def test_fixed_materialization_config_factory_requires_scoped_writes_for_guarded_switch() -> None:
    with pytest.raises(ValueError, match="guarded switch requires tree-scoped projection writes"):
        fixed_materialization_config_factory(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            enable_partial_hydration_guarded_switch=True,
            enable_tree_scoped_projection_writes=False,
        )

    with pytest.raises(ValueError, match="guarded switch requires tree-scoped projection writes"):
        MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            enable_partial_hydration_guarded_switch=True,
            enable_tree_scoped_projection_writes=False,
        )


def test_fixed_materialization_config_factory_fails_closed_for_checkpoint_policy() -> None:
    config_factory = fixed_materialization_config_factory(
        owner_id="worker-1",
        embedding_model_version="embed-v1",
        snapshot_policy="checkpoint",
    )

    with pytest.raises(ValueError, match="requires explicit cloud checkpoint hydration support"):
        config_factory(_envelope())


def test_fixed_materialization_config_factory_accepts_explicit_checkpoint_support() -> None:
    config = fixed_materialization_config_factory(
        owner_id="worker-1",
        embedding_model_version="embed-v1",
        snapshot_policy="checkpoint",
        cloud_checkpoint_hydration_supported=True,
    )(_envelope())

    assert config.snapshot_policy == "cloud_checkpoint"
    assert config.cloud_checkpoint_hydration_supported is True


def test_missing_worker_env_reports_absent_required_values() -> None:
    missing = missing_worker_env({"AZURE_CLIENT_ID": "client-id"})

    assert "AZURE_CLIENT_ID" not in missing
    assert "AZURE_COSMOS_ENDPOINT" in missing


def test_hydrating_core_factory_uses_safe_scope_directory(tmp_path) -> None:
    factory = hydrating_user_forest_core_factory(
        config=load_default_config(),
        chat_client="chat",
        embedding_client="embedding",
        snapshot_store="snapshots",
        scratch_root=tmp_path,
    )

    core = factory(_envelope())

    assert core.snapshot_store == "snapshots"
    assert core.user_forest.user_id == "user-1"
    assert core.user_forest.snapshot_dir.parent == tmp_path
    assert core.user_forest.snapshot_dir.name.startswith("user-")


def test_hydrating_core_factory_can_select_cloud_state_core(tmp_path) -> None:
    stores = SimpleNamespace(
        tree_node_store_factory=lambda forest_version: ("tree", forest_version),
        fact_store="facts",
        summary_cache="summaries",
        browse_embedding_store_factory=lambda forest_version: ("browse", forest_version),
    )
    store_factory = RecordingStoreFactory()
    store_factory.return_value = stores

    def build_stores(envelope):
        store_factory.envelopes.append(envelope)
        return stores

    factory = hydrating_user_forest_core_factory(
        config=load_default_config(),
        chat_client="chat",
        embedding_client="embedding",
        snapshot_store="snapshots",
        scratch_root=tmp_path,
        store_factory=build_stores,
        enable_cloud_state_hydration=True,
    )

    core = factory(_envelope())

    assert core.enable_cloud_state_hydration is True
    assert core.tree_store_factory("v1") == ("tree", "v1")
    assert core.fact_store == "facts"
    assert core.summary_cache == "summaries"
    assert core.browse_embedding_store_factory("v1") == ("browse", "v1")
    assert len(store_factory.envelopes) == 1
