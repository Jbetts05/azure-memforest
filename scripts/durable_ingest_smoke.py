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
    if args.session_count < 1 or args.session_count > 50:
        raise RuntimeError("--session-count must be between 1 and 50.")
    function_key = args.function_key or os.getenv("AZURE_MEMFOREST_DURABLE_FUNCTION_KEY")
    if not function_key:
        raise RuntimeError("Provide --function-key or AZURE_MEMFOREST_DURABLE_FUNCTION_KEY.")
    if not str(args.durable_start_url).lower().startswith("https://"):
        raise RuntimeError("--durable-start-url must use https.")
    durable_status_key = args.durable_status_key or os.getenv("AZURE_MEMFOREST_DURABLE_STATUS_KEY") or function_key
    benchmark_secret = args.benchmark_secret or _read_secret(args.benchmark_secret_path) if args.auth_mode == "benchmark" else ""
    started_at = time.time()
    run_id = uuid.uuid4().hex
    user_id = args.user_id or f"durable-smoke-{run_id}"
    sessions = []
    for index in range(1, args.session_count + 1):
        sessions.append(_run_ingest_smoke_session(args, run_id, user_id, index, benchmark_secret, function_key, durable_status_key))
    query_results = [] if args.skip_query else [
        _query_marker_with_retry(args, user_id, benchmark_secret, session["code"])
        for session in sessions
    ]
    projection = None
    if args.verify_projection and args.cosmos_endpoint and args.cosmos_database and args.azure_client_id:
        projection = _latest_projection_contains_codes(args, user_id, [session["code"] for session in sessions])
    manifest_metadata = None
    if args.export_manifest_metadata:
        manifest_metadata = _latest_manifest_metadata_records(args, user_id, limit=args.session_count)
    manifest_metadata_summary = _manifest_metadata_summary(manifest_metadata)
    artifact = {
        "run_id": run_id,
        "started_at_unix": started_at,
        "completed_at_unix": time.time(),
        "durable_hub": args.durable_hub,
        "user_id": user_id,
        "session_count": args.session_count,
        "sessions": sessions,
        "accepted_job": sessions[0]["accepted_job"] if len(sessions) == 1 else None,
        "durable_start": sessions[0]["durable_start"] if len(sessions) == 1 else None,
        "durable_instance": sessions[0]["durable_instance"] if len(sessions) == 1 else None,
        "job": sessions[0]["job"] if len(sessions) == 1 else None,
        "query": query_results[0]["response"] if len(query_results) == 1 else None,
        "queries": query_results,
        "projection": projection,
        "manifest_metadata": manifest_metadata,
        "manifest_metadata_summary": manifest_metadata_summary,
        "assertions": {
            "jobs_succeeded": all(session["job"].get("status") == "succeeded" for session in sessions),
            "durable_instances_completed": all(
                session["durable_instance"] is None or _durable_instance_completed(session["durable_instance"])
                for session in sessions
            ),
            "queries_mention_markers": args.skip_query or all(query["mentions_marker"] for query in query_results),
            "latest_projection_contains_all_markers": projection is None or projection["contains_all_codes"],
            "manifest_metadata_exported": manifest_metadata is None or manifest_metadata_summary["record_count"] >= args.session_count,
        },
    }
    failures = [name for name, passed in artifact["assertions"].items() if not passed]
    if failures:
        raise RuntimeError(f"Durable smoke assertions failed: {failures}; artifact={json.dumps(_redact_artifact_for_error(artifact), ensure_ascii=False)[:2000]}")
    return artifact


def _run_ingest_smoke_session(
    args: argparse.Namespace,
    run_id: str,
    user_id: str,
    index: int,
    benchmark_secret: str,
    function_key: str,
    durable_status_key: str,
) -> dict[str, Any]:
    session_id = f"durable-smoke-session-{run_id}-{index}"
    idempotency_key = f"durable-smoke-{run_id}-{index}"
    code = f"{run_id}-{index}"
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    ingest_payload = {
        "turns": [
            {
                "role": "user",
                "content": f"Durable smoke memory marker {code}. My durable smoke code number {index} is {code}.",
                "timestamp": timestamp,
                "metadata": {"source": "durable-smoke", "run_id": run_id, "session_index": index, "code": code},
            }
        ],
        "metadata": {"source": "durable-smoke", "run_id": run_id, "session_index": index, "code": code},
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
    if args.job_source == "api":
        start = {"id": job_id, "status": "started_by_api", "deduped": False}
    else:
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
    durable_instance = _get_durable_instance_status(
        args.durable_start_url,
        durable_status_key,
        job_id,
        task_hub=args.durable_hub,
        timeout=args.timeout_seconds,
    ) if args.verify_durable_instance else None
    return {
        "index": index,
        "code": code,
        "session_id": session_id,
        "accepted_job": accepted,
        "durable_start": _redact_start(start),
        "durable_instance": _redact_durable_instance(durable_instance) if durable_instance is not None else None,
        "job": status,
    }


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


def _query_marker_with_retry(args: argparse.Namespace, user_id: str, benchmark_secret: str, code: str) -> dict[str, Any]:
    deadline = time.time() + args.query_timeout_seconds
    last: dict[str, Any] = {}
    while time.time() < deadline:
        response = _post_json(
            f"{args.api_url.rstrip('/')}/v1/users/{user_id}/query",
            {
                "question": f"What is my durable smoke code number {code.rsplit('-', 1)[-1]}?",
                "top_k": max(args.session_count + 2, 8),
                "max_facts": max(args.session_count + 2, 8),
                "trace_requested": True,
            },
            headers=_api_headers(args.tenant_id, user_id, benchmark_secret, auth_mode=args.auth_mode),
            timeout=args.timeout_seconds,
        )
        last = {
            "code": code,
            "mentions_marker": code.lower() in str(response.get("answer") or "").lower()
            or any(code.lower() in str(fact.get("text") or "").lower() for fact in response.get("facts") or []),
            "response": response,
        }
        if last["mentions_marker"]:
            return last
        time.sleep(args.query_poll_seconds)
    return last


def _latest_projection_contains_codes(args: argparse.Namespace, user_id: str, codes: list[str]) -> dict[str, Any]:
    credential, client = _cosmos_client(args)
    try:
        database = client.get_database_client(args.cosmos_database)
        manifests = list(
            database.get_container_client("manifests").query_items(
                query=(
                    "SELECT TOP 1 * FROM c WHERE c.tenant_id=@tenant AND c.user_id=@user "
                    "AND c.status='committed' ORDER BY c.generation DESC"
                ),
                parameters=[
                    {"name": "@tenant", "value": args.tenant_id},
                    {"name": "@user", "value": user_id},
                ],
                enable_cross_partition_query=True,
            )
        )
        if not manifests:
            return {"contains_all_codes": False, "missing_codes": codes, "reason": "latest_manifest_missing"}
        manifest = manifests[0]
        forest_version = str(manifest.get("forest_version") or "")
        locations = list(
            database.get_container_client("fact_locations").query_items(
                query=(
                    "SELECT DISTINCT c.fact_id FROM c WHERE c.tenant_id=@tenant AND c.user_id=@user "
                    "AND c.forest_version=@forest_version"
                ),
                parameters=[
                    {"name": "@tenant", "value": args.tenant_id},
                    {"name": "@user", "value": user_id},
                    {"name": "@forest_version", "value": forest_version},
                ],
                enable_cross_partition_query=True,
            )
        )
        fact_ids = sorted({str(location.get("fact_id") or "") for location in locations if location.get("fact_id")})
        fact_docs = []
        if fact_ids:
            fact_docs = list(
                database.get_container_client("facts").query_items(
                    query=(
                        "SELECT c.fact_id, c.fact_text FROM c WHERE c.tenant_id=@tenant AND c.user_id=@user "
                        "AND ARRAY_CONTAINS(@fact_ids, c.fact_id)"
                    ),
                    parameters=[
                        {"name": "@tenant", "value": args.tenant_id},
                        {"name": "@user", "value": user_id},
                        {"name": "@fact_ids", "value": fact_ids},
                    ],
                    enable_cross_partition_query=True,
                )
            )
        fact_text = "\n".join(str(fact.get("fact_text") or "") for fact in fact_docs).lower()
        missing = [code for code in codes if code.lower() not in fact_text]
        return {
            "contains_all_codes": not missing,
            "missing_codes": missing,
            "forest_version": forest_version,
            "manifest_id": manifest.get("manifest_id") or manifest.get("id"),
            "fact_location_count": len(locations),
            "fact_count": len(fact_docs),
        }
    finally:
        _close_cosmos_client(credential, client)


def _latest_manifest_metadata_records(args: argparse.Namespace, user_id: str, *, limit: int) -> dict[str, Any]:
    credential, client = _cosmos_client(args)
    safe_limit = max(1, min(int(limit), 50))
    try:
        database = client.get_database_client(args.cosmos_database)
        manifests = list(
            database.get_container_client("manifests").query_items(
                query=(
                    f"SELECT TOP {safe_limit} c.manifest_id, c.id, c.forest_version, c.generation, "
                    "c.created_at_unix, c.metadata FROM c WHERE c.tenant_id=@tenant AND c.user_id=@user "
                    "AND c.status='committed' ORDER BY c.generation DESC"
                ),
                parameters=[
                    {"name": "@tenant", "value": args.tenant_id},
                    {"name": "@user", "value": user_id},
                ],
                enable_cross_partition_query=True,
            )
        )
        return {
            "records": [
                {
                    "manifest_id": manifest.get("manifest_id") or manifest.get("id"),
                    "forest_version": manifest.get("forest_version"),
                    "generation": manifest.get("generation"),
                    "created_at_unix": manifest.get("created_at_unix"),
                    "metadata": manifest.get("metadata") or {},
                }
                for manifest in sorted(manifests, key=lambda item: int(item.get("generation") or 0))
            ]
        }
    finally:
        _close_cosmos_client(credential, client)


def _manifest_metadata_summary(manifest_metadata: dict[str, Any] | None) -> dict[str, Any]:
    records = manifest_metadata.get("records") if isinstance(manifest_metadata, dict) else None
    if not isinstance(records, list):
        records = []
    supported = 0
    unsupported = 0
    missing = 0
    for record in records:
        metadata = record.get("metadata") if isinstance(record, dict) else None
        prediction = metadata.get("pre_ingest_scope_prediction") if isinstance(metadata, dict) else None
        if isinstance(prediction, dict) and prediction.get("supported") is True:
            supported += 1
        elif isinstance(prediction, dict):
            unsupported += 1
        else:
            missing += 1
    return {
        "record_count": len(records),
        "pre_ingest_prediction_supported_count": supported,
        "pre_ingest_prediction_unsupported_count": unsupported,
        "pre_ingest_prediction_missing_count": missing,
    }


def _redact_artifact_for_error(artifact: dict[str, Any]) -> dict[str, Any]:
    redacted = dict(artifact)
    for session in redacted.get("sessions") or []:
        accepted = session.get("accepted_job")
        if isinstance(accepted, dict) and "envelope" in accepted:
            accepted["envelope"] = "<redacted>"
    return redacted


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
    parser.add_argument("--session-count", type=int, default=1, help="Number of sequential live sessions to ingest (1-50).")
    parser.add_argument("--verify-projection", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--export-manifest-metadata",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Export latest committed manifest metadata for the smoke user. Use a fresh user_id for canaries so "
            "the latest records correspond to this run; supported prediction counts are reported but not asserted here."
        ),
    )
    parser.add_argument("--query-timeout-seconds", type=int, default=300)
    parser.add_argument("--query-poll-seconds", type=int, default=10)
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
