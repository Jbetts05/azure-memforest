"""Cloud-facing contracts for Azure-MemForest adapters."""

from src.cloud.contracts import (
    BrowseEmbeddingStore,
    FactLocation,
    FactLocationIndex,
    FactStore,
    Lease,
    LeaseManager,
    ModelClient,
    RecallCandidate,
    RecallIndex,
    StoredTreeNode,
    SummaryCache,
    TraceSink,
    TreeNodeStore,
    TreeSnapshotStore,
)
from src.cloud.local_adapters import (
    FactManagerStoreAdapter,
    InMemoryFactLocationIndex,
    InMemorySummaryCache,
    NodeIndexBrowseEmbeddingAdapter,
    NodeIndexRecallAdapter,
    TreeStoreNodeAdapter,
)

__all__ = [
    "BrowseEmbeddingStore",
    "FactLocation",
    "FactLocationIndex",
    "FactStore",
    "Lease",
    "LeaseManager",
    "ModelClient",
    "RecallCandidate",
    "RecallIndex",
    "StoredTreeNode",
    "SummaryCache",
    "TraceSink",
    "TreeNodeStore",
    "TreeSnapshotStore",
    "FactManagerStoreAdapter",
    "InMemoryFactLocationIndex",
    "InMemorySummaryCache",
    "NodeIndexBrowseEmbeddingAdapter",
    "NodeIndexRecallAdapter",
    "TreeStoreNodeAdapter",
]
