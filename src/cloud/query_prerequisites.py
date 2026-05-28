"""Runtime gate for cloud query composition."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class CloudQueryPrerequisiteStatus:
    ok: bool
    missing: tuple[str, ...] = field(default_factory=tuple)

    def raise_if_not_ready(self) -> None:
        if not self.ok:
            raise CloudQueryNotReady(self.missing)


class CloudQueryNotReady(RuntimeError):
    def __init__(self, missing: tuple[str, ...] | str) -> None:
        if isinstance(missing, str):
            self.missing = (missing,)
            self.reason = missing
        else:
            self.missing = missing
            self.reason = ",".join(missing)
        super().__init__("Cloud query prerequisites are not ready: " + ", ".join(self.missing))


def verify_cloud_query_prerequisites(
    *,
    facts_store_ready: bool = False,
    tree_node_store_ready: bool = False,
    fact_location_index_ready: bool = False,
    manifest_store_ready: bool = False,
    recall_index_ready: bool = False,
    browse_embedding_store_ready: bool = False,
    model_client_ready: bool = False,
    trace_sink_ready: bool = False,
    embedding_dimension_configured: bool = False,
    embedding_model_version_configured: bool = False,
    committed_manifest_exists: bool = False,
    parity_gate_defined: bool = False,
) -> CloudQueryPrerequisiteStatus:
    missing = []
    checks = {
        "facts_store": facts_store_ready,
        "tree_node_store": tree_node_store_ready,
        "fact_location_index": fact_location_index_ready,
        "manifest_store": manifest_store_ready,
        "recall_index": recall_index_ready,
        "browse_embedding_store": browse_embedding_store_ready,
        "model_client": model_client_ready,
        "trace_sink": trace_sink_ready,
        "embedding_dimension": embedding_dimension_configured,
        "embedding_model_version": embedding_model_version_configured,
        "committed_manifest": committed_manifest_exists,
        "parity_gate": parity_gate_defined,
    }
    for name, ready in checks.items():
        if not ready:
            missing.append(name)
    return CloudQueryPrerequisiteStatus(ok=not missing, missing=tuple(missing))
