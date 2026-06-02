from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any

import httpx


DEFAULT_OUTPUT_ROOT = Path("artifacts") / "durable-smoke"
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def run_smoke(args: argparse.Namespace) -> dict[str, Any]:
    if os.getenv("AZURE_MEMFOREST_LIVE_DURABLE_SMOKE") != "1":
        raise RuntimeError("Set AZURE_MEMFOREST_LIVE_DURABLE_SMOKE=1 to run live Durable smoke calls.")
    function_key = args.function_key or os.getenv("AZURE_MEMFOREST_DURABLE_FUNCTION_KEY")
    if not function_key:
        raise RuntimeError("Provide --function-key or AZURE_MEMFOREST_DURABLE_FUNCTION_KEY.")
    if not str(args.durable_start_url).lower().startswith("https://"):
        raise RuntimeError("--durable-start-url must use https.")
    durable_status_key = args.durable_status_key or os.getenv("AZURE_MEMFOREST_DURABLE_STATUS_KEY") or function_key
    benchmark_secret = args.benchmark_secret or _read_secret(args.benchmark_secret_path) if args.auth_mode == "benchmark" else ""
    started_at = time.time()
    run_id = uuid.uuid4().hex
    session_id = f"durable-smoke-session-{run_id}"
    user_id = args.user_id or f"durable-smoke-{run_id}"
    idempotency_key = f"durable-smoke-{run_id}"
    ingest_payload = {
        "turns": [
            {
                "role": "user",
                "content": f"Durable smoke memory marker {run_id}. My durable smoke code is {run_id}.",
                "metadata": {"source": "durable-smoke", "run_id": run_id},
            }
        ],
        "metadata": {"source": "durable-smoke", "run_id": run_id},
    }
    if args.job_source == "cosmos":
        accepted = asyncio.run(_create_accepted_job_cosmos(args, user_id, session_id, idempotency_key, ingest_payload))
    else:
        accepted = _post_json(
            f"{args.api_url.rstrip('/')}/v1/users/{user_id}/sessions/{session_id}:ingest",
            ingest_payload,
            headers={**_api_headers(args.tenant_id, user_id, benchmark_secret, auth_mode=args.auth_mode), "Idempotency-Key": idempotency_key},
            timeout=args.timeout_seconds,
        )
    job_id = str(accepted.get("job_id") or "").strip()
    if not job_id:
        raise RuntimeError(f"API ingest did not return a job id: {accepted}")
    envelope = accepted.get("envelope") or {
        "job_id": job_id,
        "job_type": "ingest_session",
        "tenant_id": args.tenant_id,
        "user_id": user_id,
        "session_id": session_id,
        "actor_id": user_id,
        "idempotency_key": idempotency_key,
        "payload": ingest_payload,
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
    if args.status_source == "cosmos":
        status = asyncio.run(_wait_job_cosmos(args, job_id, user_id))
    else:
        status = _wait_job_api(args.api_url, args.tenant_id, user_id, benchmark_secret, job_id, args.timeout_seconds, auth_mode=args.auth_mode)
    query = None if args.skip_query else _post_json(
        f"{args.api_url.rstrip('/')}/v1/users/{user_id}/query",
        {"question": "What is my durable smoke code?", "top_k": 3, "max_facts": 3, "trace_requested": True},
        headers=_api_headers(args.tenant_id, user_id, benchmark_secret, auth_mode=args.auth_mode),
        timeout=args.timeout_seconds,
    )
    durable_instance = _get_durable_instance_status(
        args.durable_start_url,
        durable_status_key,
        job_id,
        task_hub=args.durable_hub,
        timeout=args.timeout_seconds,
    ) if args.verify_durable_instance else None
    artifact = {
        "run_id": run_id,
        "started_at_unix": started_at,
        "completed_at_unix": time.time(),
        "durable_hub": args.durable_hub,
        "accepted_job": accepted,
        "durable_start": _redact_start(start),
        "durable_instance": _redact_durable_instance(durable_instance) if durable_instance is not None else None,
        "job": status,
        "query": query,
        "assertions": {
            "job_succeeded": status.get("status") == "succeeded",
            "durable_instance_completed": durable_instance is None or _durable_instance_completed(durable_instance),
            "query_mentions_marker": args.skip_query or run_id.lower() in str((query or {}).get("answer") or "").lower(),
        },
    }
    if not artifact["assertions"]["job_succeeded"]:
        raise RuntimeError(f"Durable smoke job did not succeed: {status}")
    if not artifact["assertions"]["query_mentions_marker"]:
        raise RuntimeError(f"Durable smoke query did not recover marker {run_id}: {query}")
    return artifact


def _wait_job_api(
    api_url: str,
    tenant_id: str,
    user_id: str,
    benchmark_secret: str,
    job_id: str,
    timeout_seconds: int,
    *,
    auth_mode: str,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last: dict[str, Any] = {}
    while time.time() < deadline:
        last = _get_json(
            f"{api_url.rstrip('/')}/v1/users/{user_id}/jobs/{job_id}",
            headers=_api_headers(tenant_id, user_id, benchmark_secret, auth_mode=auth_mode),
            timeout=min(60, timeout_seconds),
        )
        if last.get("status") in {"succeeded", "failed", "cancelled"}:
            return last
        time.sleep(10)
    raise TimeoutError(f"Timed out waiting for job {job_id}: {last}")


async def _wait_job_cosmos(args: argparse.Namespace, job_id: str, user_id: str) -> dict[str, Any]:
    deadline = time.time() + args.timeout_seconds
    last: dict[str, Any] = {}
    while time.time() < deadline:
        last = await _read_job_cosmos(args, job_id, user_id)
        if last.get("status") in {"succeeded", "failed", "cancelled"}:
            return last
        time.sleep(10)
    raise TimeoutError(f"Timed out waiting for job {job_id}: {last}")


async def _read_job_cosmos(args: argparse.Namespace, job_id: str, user_id: str) -> dict[str, Any]:
    credential, client = _cosmos_client(args)
    try:
        container = client.get_database_client(args.cosmos_database).get_container_client("jobs")
        item = container.read_item(item=job_id, partition_key=args.tenant_id)
        if str(item.get("user_id")) != user_id:
            raise RuntimeError(f"Cosmos job {job_id} user mismatch.")
        return dict(item)
    finally:
        _close_cosmos_client(credential, client)


async def _create_accepted_job_cosmos(
    args: argparse.Namespace,
    user_id: str,
    session_id: str,
    idempotency_key: str,
    ingest_payload: dict[str, Any],
) -> dict[str, Any]:
    from src.cloud.cosmos_jobs import CosmosJobStore
    from src.service.auth import ActorContext, TenantContext
    from src.service.jobs import build_write_job_envelope

    if not args.cosmos_endpoint or not args.cosmos_database or not args.azure_client_id:
        raise RuntimeError("--job-source cosmos requires --cosmos-endpoint, --cosmos-database, and --azure-client-id.")
    credential, client = _cosmos_client(args)
    try:
        database = client.get_database_client(args.cosmos_database)
        job_store = CosmosJobStore(database.get_container_client("jobs"))
        envelope = build_write_job_envelope(
            ActorContext(actor_id=user_id, tenant=TenantContext(args.tenant_id)),
            user_id=user_id,
            session_id=session_id,
            job_type="ingest_session",
            idempotency_key=idempotency_key,
            payload=ingest_payload,
        )
        recorded = await job_store.create_accepted_job(envelope)
        return {
            "job_id": recorded.job_id,
            "status": recorded.status,
            "tenant_id": recorded.tenant_id,
            "user_id": recorded.user_id,
            "session_id": session_id,
            "envelope": asdict(envelope),
        }
    finally:
        _close_cosmos_client(credential, client)


def _cosmos_client(args: argparse.Namespace):
    from azure.cosmos import CosmosClient
    from azure.identity import DefaultAzureCredential

    if not args.cosmos_endpoint or not args.cosmos_database or not args.azure_client_id:
        raise RuntimeError("Cosmos access requires --cosmos-endpoint, --cosmos-database, and --azure-client-id.")
    credential = DefaultAzureCredential(
        managed_identity_client_id=args.azure_client_id,
        exclude_interactive_browser_credential=True,
    )
    return credential, CosmosClient(args.cosmos_endpoint, credential=credential)


def _close_cosmos_client(credential: Any, client: Any) -> None:
    close_client = getattr(client, "close", None)
    if close_client is not None:
        close_client()
    close = getattr(credential, "close", None)
    if close is not None:
        close()


def _api_headers(tenant_id: str, user_id: str, benchmark_secret: str, *, auth_mode: str) -> dict[str, str]:
    headers = {
        "x-tenant-id": tenant_id,
        "x-actor-id": user_id,
        "Content-Type": "application/json",
    }
    if auth_mode == "benchmark":
        headers["x-benchmark-auth"] = benchmark_secret
    return headers


def _post_json(url: str, body: dict[str, Any], *, headers: dict[str, str], timeout: int) -> dict[str, Any]:
    response = httpx.post(url, headers=headers, json=body, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _get_json(url: str, *, headers: dict[str, str], timeout: int) -> dict[str, Any]:
    response = httpx.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _get_durable_instance_status(
    durable_start_url: str,
    durable_status_key: str,
    job_id: str,
    *,
    task_hub: str,
    timeout: int,
) -> dict[str, Any]:
    base_url = durable_start_url.split("/api/", 1)[0].rstrip("/")
    response = httpx.get(
        f"{base_url}/runtime/webhooks/durabletask/instances/{job_id}",
        params={"taskHub": task_hub, "connection": "Storage", "code": durable_status_key},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def _durable_instance_completed(instance: dict[str, Any]) -> bool:
    return str(instance.get("runtimeStatus") or "").lower() == "completed"


def _redact_durable_instance(instance: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in instance.items()
        if key not in {"input", "output"}
    }


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
    parser.add_argument("--auth-mode", choices=["dev", "benchmark"], default="dev")
    parser.add_argument("--job-source", choices=["cosmos", "api"], default="cosmos")
    parser.add_argument("--status-source", choices=["cosmos", "api"], default="cosmos")
    parser.add_argument("--skip-query", action="store_true")
    parser.add_argument("--cosmos-endpoint")
    parser.add_argument("--cosmos-database")
    parser.add_argument("--azure-client-id")
    parser.add_argument("--benchmark-secret")
    parser.add_argument("--benchmark-secret-path", type=Path)
    parser.add_argument("--durable-hub", default="MemForestWrites")
    parser.add_argument("--durable-status-key")
    parser.add_argument("--verify-durable-instance", action=argparse.BooleanOptionalAction, default=True)
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
