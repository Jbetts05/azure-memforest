"""Azure AI Search recall index schema constants."""

from __future__ import annotations

import base64

DEFAULT_RECALL_INDEX_NAME = "memforest-recall"
VECTOR_FIELD_NAME = "embedding"


def recall_document_key(*, tenant_id: str, tree_id: str, node_id: str, forest_version: str) -> str:
    values = [tenant_id, tree_id, node_id, forest_version]
    raw = "".join(f"{len(value)}:{value}" for value in values)

    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")
