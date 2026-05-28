"""HTTP service layer for Azure-MemForest.

This package is separate from ``src.api``, which contains model-client wrappers.
"""

__all__ = ["create_app"]


def __getattr__(name: str):
    if name == "create_app":
        from src.service.app import create_app

        return create_app
    raise AttributeError(name)
