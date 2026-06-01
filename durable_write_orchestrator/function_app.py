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
    result = yield context.call_activity("record_write_started", payload)
    return result


@app.activity_trigger(input_name="payload")
def record_write_started(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "job_id": payload.get("job_id"),
        "job_type": payload.get("job_type"),
        "tenant_id": payload.get("tenant_id"),
        "user_id": payload.get("user_id"),
        "status": "durable_orchestrator_placeholder",
    }
