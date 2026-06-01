from __future__ import annotations

import json
from typing import Any

import azure.durable_functions as df
import azure.functions as func

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="writes/start", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_write(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    payload = req.get_json()
    instance_id = str(
        payload.get("instance_id")
        or req.headers.get("x-ms-durable-instance-id")
        or payload.get("job_id")
        or ""
    ).strip()
    if not instance_id:
        return func.HttpResponse(
            json.dumps({"error": "instance_id or job_id is required"}),
            status_code=400,
            mimetype="application/json",
        )

    existing = await client.get_status(instance_id)
    if existing is not None:
        # Restart semantics for terminal instances must be explicit before this path becomes live.
        return func.HttpResponse(
            json.dumps({"id": instance_id, "status": str(existing.runtime_status), "deduped": True}),
            status_code=202,
            mimetype="application/json",
        )

    await client.start_new("write_orchestrator", instance_id=instance_id, client_input=payload)
    return func.HttpResponse(
        json.dumps({"id": instance_id, "status": "started", "deduped": False}),
        status_code=202,
        mimetype="application/json",
    )


@app.orchestration_trigger(context_name="context")
def write_orchestrator(context: df.DurableOrchestrationContext):
    payload = context.get_input()
    activity_input = {
        "envelope": payload,
        "owner_id": f"durable:{context.instance_id}",
        "instance_id": context.instance_id,
    }
    retry_options = df.RetryOptions(5000, 3)
    try:
        return (yield context.call_activity_with_retry("materialize_write_job", retry_options, activity_input))
    except Exception as exc:
        return (
            yield context.call_activity_with_retry(
                "fail_write_job",
                retry_options,
                {
                    **activity_input,
                    "reason": f"durable_activity_failed:{type(exc).__name__}",
                },
            )
        )


@app.activity_trigger(input_name="payload")
async def materialize_write_job(payload: dict[str, Any]) -> dict[str, Any]:
    from src.worker.durable_activity import materialize_write_job_from_env

    return await materialize_write_job_from_env(payload)


@app.activity_trigger(input_name="payload")
async def fail_write_job(payload: dict[str, Any]) -> dict[str, Any]:
    from src.worker.durable_activity import fail_write_job_from_env

    return await fail_write_job_from_env(payload)
