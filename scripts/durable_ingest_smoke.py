from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

import httpx


DEFAULT_OUTPUT_ROOT = Path("artifacts") / "durable-smoke"


def run_smoke(args: argparse.Namespace) -> dict[str, Any]:
    if os.getenv("AZURE_MEMFOREST_LIVE_DURABLE_SMOKE") != "1":
        raise RuntimeError("Set AZURE_MEMFOREST_LIVE_DURABLE_SMOKE=1 to run live Durable smoke calls.")
    function_key = args.function_key or os.getenv("AZURE_MEMFOREST_DURABLE_FUNCTION_KEY")
    if not function_key:
        raise RuntimeError("Provide --function-key or AZURE_MEMFOREST_DURABLE_FUNCTION_KEY.")
    benchmark_secret = args.benchmark_secret or _read_secret(args.benchmark_secret_path)
    started_at = time.time()
    run_id = uuid.uuid4().hex
    job_id = f"durable-smoke-{run_id}"
    session_id = f"durable-smoke-session-{run_id}"
    user_id = args.user_id or f"durable-smoke-{run_id}"
    envelope = {
        "job_id": job_id,
        "job_type": "ingest_session",
        "tenant_id": args.tenant_id,
        "user_id": user_id,
        "session_id": session_id,
        "actor_id": user_id,
        "idempotency_key": f"durable-smoke-{run_id}",
        "payload": {
            "turns": [
                {
                    "role": "user",
                    "content": f"Durable smoke memory marker {run_id}. My durable smoke code is {run_id}.",
                    "metadata": {"source": "durable-smoke", "run_id": run_id},
                }
            ],
            "metadata": {"source": "durable-smoke", "run_id": run_id},
        },
    }
    start = _post_json(
        args.durable_start_url,
        envelope,
        headers={"x-functions-key": function_key, "Content-Type": "application/json", "x-ms-durable-instance-id": job_id},
        timeout=args.timeout_seconds,
    )
    if start.get("deduped"):
        raise RuntimeError(f"Durable starter deduped fresh smoke job {job_id}: {start}")
    if start.get("id") != job_id:
        raise RuntimeError(f"Durable starter returned unexpected id: {start}")
    status = _wait_job(args.api_url, args.tenant_id, user_id, benchmark_secret, job_id, args.timeout_seconds)
    query = _post_json(
        f"{args.api_url.rstrip('/')}/v1/users/{user_id}/query",
        {"question": "What is my durable smoke code?", "top_k": 3, "max_facts": 3, "trace_requested": True},
        headers=_api_headers(args.tenant_id, user_id, benchmark_secret),
        timeout=args.timeout_seconds,
    )
    artifact = {
        "run_id": run_id,
        "started_at_unix": started_at,
        "completed_at_unix": time.time(),
        "durable_hub": args.durable_hub,
        "durable_start": _redact_start(start),
        "job": status,
        "query": query,
        "assertions": {
            "job_succeeded": status.get("status") == "succeeded",
            "query_mentions_marker": run_id.lower() in str(query.get("answer") or "").lower(),
        },
    }
    if not artifact["assertions"]["job_succeeded"]:
        raise RuntimeError(f"Durable smoke job did not succeed: {status}")
    if not artifact["assertions"]["query_mentions_marker"]:
        raise RuntimeError(f"Durable smoke query did not recover marker {run_id}: {query}")
    return artifact


def _wait_job(api_url: str, tenant_id: str, user_id: str, benchmark_secret: str, job_id: str, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last: dict[str, Any] = {}
    while time.time() < deadline:
        last = _get_json(
            f"{api_url.rstrip('/')}/v1/users/{user_id}/jobs/{job_id}",
            headers=_api_headers(tenant_id, user_id, benchmark_secret),
            timeout=min(60, timeout_seconds),
        )
        if last.get("status") in {"succeeded", "failed", "cancelled"}:
            return last
        time.sleep(10)
    raise TimeoutError(f"Timed out waiting for job {job_id}: {last}")


def _api_headers(tenant_id: str, user_id: str, benchmark_secret: str) -> dict[str, str]:
    return {
        "x-benchmark-auth": benchmark_secret,
        "x-tenant-id": tenant_id,
        "x-actor-id": user_id,
        "Content-Type": "application/json",
    }


def _post_json(url: str, body: dict[str, Any], *, headers: dict[str, str], timeout: int) -> dict[str, Any]:
    response = httpx.post(url, headers=headers, json=body, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _get_json(url: str, *, headers: dict[str, str], timeout: int) -> dict[str, Any]:
    response = httpx.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _read_secret(path: Path | None) -> str:
    if path is None:
        raise RuntimeError("Provide --benchmark-secret or --benchmark-secret-path.")
    return path.read_text(encoding="utf-8").strip()


def _redact_start(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if "uri" not in key.lower()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one opt-in live Durable ingest smoke.")
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--durable-start-url", required=True)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--user-id")
    parser.add_argument("--function-key")
    parser.add_argument("--benchmark-secret")
    parser.add_argument("--benchmark-secret-path", type=Path)
    parser.add_argument("--durable-hub", default="MemForestWrites")
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    artifact = run_smoke(args)
    out_dir = args.output_root / time.strftime("%Y%m%d-%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "durable_smoke.json"
    out_path.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
