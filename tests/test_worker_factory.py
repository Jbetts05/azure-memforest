from __future__ import annotations

from types import SimpleNamespace
import json
import sys
import time

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
        enable_partial_hydration_authoritative_switch=True,
        partial_hydration_authority_preflight_report_path="approved-preflight.json",
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
    assert config.enable_partial_hydration_authoritative_switch is True
    assert config.partial_hydration_authority_preflight_report_path == "approved-preflight.json"
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


def test_fixed_materialization_config_factory_requires_guarded_switch_for_authoritative_switch() -> None:
    with pytest.raises(ValueError, match="authoritative switch requires guarded switch"):
        fixed_materialization_config_factory(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            enable_tree_scoped_projection_writes=True,
            enable_partial_hydration_authoritative_switch=True,
        )

    with pytest.raises(ValueError, match="authoritative switch requires guarded switch"):
        MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            enable_tree_scoped_projection_writes=True,
            enable_partial_hydration_authoritative_switch=True,
        )


def test_fixed_materialization_config_factory_requires_report_for_authoritative_switch() -> None:
    with pytest.raises(ValueError, match="authoritative switch requires authority preflight report"):
        fixed_materialization_config_factory(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            enable_tree_scoped_projection_writes=True,
            enable_partial_hydration_guarded_switch=True,
            enable_partial_hydration_authoritative_switch=True,
        )

    with pytest.raises(ValueError, match="authoritative switch requires authority preflight report"):
        MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            enable_tree_scoped_projection_writes=True,
            enable_partial_hydration_guarded_switch=True,
            enable_partial_hydration_authoritative_switch=True,
        )


def _required_materialization_env() -> dict[str, str]:
    environ = {name: "value" for name in (
        "AZURE_CLIENT_ID",
        "AZURE_COSMOS_ENDPOINT",
        "AZURE_COSMOS_DATABASE_NAME",
        "AZURE_SEARCH_ENDPOINT",
        "AZURE_SEARCH_RECALL_INDEX_NAME",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_VERSION",
        "AZURE_OPENAI_CHAT_DEPLOYMENT",
        "AZURE_OPENAI_EMBEDDING_DEPLOYMENT",
        "AZURE_OPENAI_EMBEDDING_DIMENSION",
        "AZURE_OPENAI_EMBEDDING_MODEL_VERSION",
        "AZURE_STORAGE_BLOB_ENDPOINT",
        "AZURE_STORAGE_SNAPSHOT_CONTAINER",
    )}
    environ["AZURE_OPENAI_EMBEDDING_DIMENSION"] = "1536"
    environ["AZURE_MEMFOREST_GIT_SHA"] = "test-sha"
    return environ


def test_materialization_env_rejects_authoritative_switch_without_guarded_switch() -> None:
    environ = _required_materialization_env()
    environ["AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_AUTHORITATIVE_SWITCH"] = "true"

    from src.worker.factory import build_scoped_materialization_handler_from_env

    with pytest.raises(ValueError, match="authoritative switch requires guarded switch"):
        build_scoped_materialization_handler_from_env(environ)


def test_materialization_env_requires_approved_authority_preflight_report(tmp_path) -> None:
    from src.worker.factory import build_scoped_materialization_handler_from_env

    environ = _required_materialization_env()
    environ["AZURE_MEMFOREST_ENABLE_TREE_SCOPED_PROJECTION_WRITES"] = "true"
    environ["AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_GUARDED_SWITCH"] = "true"
    environ["AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_AUTHORITATIVE_SWITCH"] = "true"

    def approved_report(*, created_at: float | None = None, canary_ok: bool = True) -> str:
        return json.dumps(
            {
                "authority_allowed": True,
                "canary_promotion_allowed": canary_ok,
                "canary_report": {"promotion_allowed": canary_ok, "promotion_blockers": [] if canary_ok else ["failed"]},
                "authority_blockers": [],
                "missing_confirmations": [],
                "unknown_confirmations": [],
                "code_version_failures": [],
                "freshness_failures": [],
                "expected_code_version": "test-sha",
                "artifact_code_version": "test-sha",
                "artifact_created_at_unix": time.time() if created_at is None else created_at,
                "max_age_seconds": 3600,
            }
        )

    with pytest.raises(ValueError, match="requires authority preflight report"):
        build_scoped_materialization_handler_from_env(environ)

    rejected = tmp_path / "rejected.json"
    rejected.write_text('{"authority_allowed": false}', encoding="utf-8")
    environ["AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_REPORT"] = str(rejected)
    with pytest.raises(ValueError, match="not approved"):
        build_scoped_materialization_handler_from_env(environ)

    mismatch = tmp_path / "mismatch.json"
    mismatch.write_text(
        json.dumps({**json.loads(approved_report()), "expected_code_version": "other", "artifact_code_version": "other"}),
        encoding="utf-8",
    )
    environ["AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_REPORT"] = str(mismatch)
    with pytest.raises(ValueError, match="code version mismatch"):
        build_scoped_materialization_handler_from_env(environ)

    stale = tmp_path / "stale.json"
    stale.write_text(approved_report(created_at=1), encoding="utf-8")
    environ["AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_REPORT"] = str(stale)
    with pytest.raises(ValueError, match="preflight report is stale"):
        build_scoped_materialization_handler_from_env(environ)

    zero_window = tmp_path / "zero_window.json"
    zero_window.write_text(json.dumps({**json.loads(approved_report()), "max_age_seconds": 0}), encoding="utf-8")
    environ["AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_REPORT"] = str(zero_window)
    with pytest.raises(ValueError, match="freshness metadata is invalid"):
        build_scoped_materialization_handler_from_env(environ)

    env_capped = tmp_path / "env_capped.json"
    env_capped.write_text(approved_report(created_at=time.time() - 10), encoding="utf-8")
    environ["AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_REPORT"] = str(env_capped)
    environ["AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_MAX_AGE_SECONDS"] = "1"
    with pytest.raises(ValueError, match="preflight report is stale"):
        build_scoped_materialization_handler_from_env(environ)

    tampered = tmp_path / "tampered.json"
    tampered.write_text(approved_report(canary_ok=False), encoding="utf-8")
    environ["AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_REPORT"] = str(tampered)
    environ.pop("AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_MAX_AGE_SECONDS", None)
    with pytest.raises(ValueError, match="canary is not approved"):
        build_scoped_materialization_handler_from_env(environ)

    nested_blockers = tmp_path / "nested_blockers.json"
    nested_payload = json.loads(approved_report())
    nested_payload["canary_report"]["promotion_blockers"] = ["live_canary:under_selection_count"]
    nested_blockers.write_text(json.dumps(nested_payload), encoding="utf-8")
    environ["AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_REPORT"] = str(nested_blockers)
    with pytest.raises(ValueError, match="canary report has blockers"):
        build_scoped_materialization_handler_from_env(environ)

    missing_confirmation = tmp_path / "missing_confirmation.json"
    missing_payload = json.loads(approved_report())
    missing_payload["missing_confirmations"] = ["rollback_switch_confirmed"]
    missing_confirmation.write_text(json.dumps(missing_payload), encoding="utf-8")
    environ["AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_REPORT"] = str(missing_confirmation)
    with pytest.raises(ValueError, match="has blockers: missing_confirmations"):
        build_scoped_materialization_handler_from_env(environ)


def test_materialization_env_accepts_fresh_authority_preflight_report_then_builds_clients(tmp_path, monkeypatch) -> None:
    from src.worker.factory import build_scoped_materialization_handler_from_env

    approved = tmp_path / "approved.json"
    approved.write_text(
        json.dumps(
            {
                "authority_allowed": True,
                "canary_promotion_allowed": True,
                "canary_report": {"promotion_allowed": True, "promotion_blockers": []},
                "authority_blockers": [],
                "missing_confirmations": [],
                "unknown_confirmations": [],
                "code_version_failures": [],
                "freshness_failures": [],
                "expected_code_version": "test-sha",
                "artifact_code_version": "test-sha",
                "artifact_created_at_unix": time.time(),
                "max_age_seconds": 3600,
            }
        ),
        encoding="utf-8",
    )
    environ = _required_materialization_env()
    environ["AZURE_MEMFOREST_ENABLE_TREE_SCOPED_PROJECTION_WRITES"] = "true"
    environ["AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_GUARDED_SWITCH"] = "true"
    environ["AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_AUTHORITATIVE_SWITCH"] = "true"
    environ["AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_REPORT"] = str(approved)

    class BoomCosmosClient:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("preflight passed and azure construction began")

    monkeypatch.setitem(sys.modules, "azure.cosmos.aio", SimpleNamespace(CosmosClient=BoomCosmosClient))
    monkeypatch.setitem(
        sys.modules,
        "azure.identity",
        SimpleNamespace(
            ManagedIdentityCredential=lambda **kwargs: object(),
            get_bearer_token_provider=lambda *args, **kwargs: "token-provider",
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "azure.identity.aio",
        SimpleNamespace(ManagedIdentityCredential=lambda **kwargs: object()),
    )

    with pytest.raises(RuntimeError, match="preflight passed and azure construction began"):
        build_scoped_materialization_handler_from_env(environ)


def test_materialization_env_requires_git_sha_for_authoritative_switch(tmp_path) -> None:
    from src.worker.factory import build_scoped_materialization_handler_from_env

    report = tmp_path / "approved.json"
    report.write_text(
        '{"authority_allowed": true, "expected_code_version": "unknown", "artifact_code_version": "unknown"}',
        encoding="utf-8",
    )
    environ = _required_materialization_env()
    environ["AZURE_MEMFOREST_GIT_SHA"] = "unknown"
    environ["AZURE_MEMFOREST_ENABLE_TREE_SCOPED_PROJECTION_WRITES"] = "true"
    environ["AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_GUARDED_SWITCH"] = "true"
    environ["AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_AUTHORITATIVE_SWITCH"] = "true"
    environ["AZURE_MEMFOREST_PARTIAL_HYDRATION_AUTHORITY_PREFLIGHT_REPORT"] = str(report)

    with pytest.raises(ValueError, match="requires AZURE_MEMFOREST_GIT_SHA"):
        build_scoped_materialization_handler_from_env(environ)


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
