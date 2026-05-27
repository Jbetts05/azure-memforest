"""Process entrypoint for the Azure-MemForest service."""

from __future__ import annotations

import os

import uvicorn

from src.service.logging import configure_service_logging


def main() -> None:
    configure_service_logging(level=os.getenv("AZURE_MEMFOREST_LOG_LEVEL", "INFO"))
    uvicorn.run(
        "src.service.app:create_app",
        factory=True,
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        log_config=None,
    )


if __name__ == "__main__":
    main()
