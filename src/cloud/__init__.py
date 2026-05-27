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
from src.cloud.azure_models import AzureOpenAIModelClient
from src.cloud.config import AzureModelDeployment, AzureOpenAIConfig, CloudConfig, load_cloud_config
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
    "AzureModelDeployment",
    "AzureOpenAIConfig",
    "AzureOpenAIModelClient",
    "CloudConfig",
    "FactManagerStoreAdapter",
    "InMemoryFactLocationIndex",
    "InMemorySummaryCache",
    "NodeIndexBrowseEmbeddingAdapter",
    "NodeIndexRecallAdapter",
    "TreeStoreNodeAdapter",
    "load_cloud_config",
]
