from __future__ import annotations

from src.config.config import load_default_config


def test_load_default_config_applies_worker_concurrency_env_overrides(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_MEMFOREST_EXTRACTION_MAX_WORKERS", "2")
    monkeypatch.setenv("AZURE_MEMFOREST_EXTRACTION_MAX_INFLIGHT", "1")
    monkeypatch.setenv("AZURE_MEMFOREST_SUMMARY_MAX_INFLIGHT", "1")
    monkeypatch.setenv("AZURE_MEMFOREST_SUMMARY_MAX_RETRIES", "0")
    monkeypatch.setenv("AZURE_MEMFOREST_SUMMARY_TIMEOUT_SECONDS", "45")

    config = load_default_config()

    assert config.extraction.max_workers == 2
    assert config.extraction.max_inflight_requests == 1
    assert config.tree.summary_manager.max_inflight == 1
    assert config.tree.summary_manager.max_retries == 0
    assert config.tree.summary_manager.timeout_sec == 45.0

