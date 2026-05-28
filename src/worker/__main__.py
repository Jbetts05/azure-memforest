"""Worker entrypoint.

The live Azure receive loop is intentionally gated until the environment
factory can build real cloud stores, model clients, and a UserForest core.
"""

from __future__ import annotations

import asyncio
import logging
import os


async def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log = logging.getLogger("src.worker")
    enabled = os.environ.get("AZURE_MEMFOREST_WORKER_ENABLED", "").strip().lower() == "true"
    if not enabled:
        log.info("worker disabled; idling without connecting to Service Bus")
        await _idle_forever()
        return 0
    factory_ready = os.environ.get("AZURE_MEMFOREST_WORKER_FACTORY_READY", "").strip().lower() == "true"
    if not factory_ready:
        log.error("worker enabled but factory readiness gate is closed; idling without consuming queue messages")
        await _idle_forever()
        return 0
    from src.worker.factory import build_worker_runtime_from_env

    runtime = build_worker_runtime_from_env(dict(os.environ))
    try:
        await runtime.run_forever()
    finally:
        await runtime.close()
    return 0


async def _idle_forever() -> None:
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
