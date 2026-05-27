from __future__ import annotations

from src.service.azure_readiness import build_azure_readiness_checks


def test_azure_readiness_builder_returns_empty_without_azure_env() -> None:
    assert build_azure_readiness_checks({}) == []


def test_azure_readiness_builder_reports_missing_values() -> None:
    checks = build_azure_readiness_checks({"AZURE_COSMOS_ENDPOINT": "https://example"})

    by_name = {check.name: check for check in checks}
    assert by_name["cosmos"].status == "not_configured"
    assert "AZURE_CLIENT_ID" in by_name["cosmos"].detail
    assert "AZURE_COSMOS_DATABASE_NAME" in by_name["cosmos"].detail


def test_azure_readiness_builder_configures_servicebus_without_management_role() -> None:
    checks = build_azure_readiness_checks(
        {
            "AZURE_SERVICEBUS_FULLY_QUALIFIED_NAMESPACE": "example.servicebus.windows.net",
            "AZURE_SERVICEBUS_WRITES_QUEUE_NAME": "memforest-writes",
        }
    )

    by_name = {check.name: check for check in checks}
    assert by_name["servicebus"].status == "ok"
    assert by_name["servicebus"].detail == "configured"
