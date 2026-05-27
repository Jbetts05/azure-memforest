"""HTTP service layer for Azure-MemForest.

This package is separate from ``src.api``, which contains model-client wrappers.
"""

from src.service.app import create_app

__all__ = ["create_app"]
