"""Export Azure Service Bus queue DLQ messages before optional clearing.

The default mode is non-destructive. Use ``--complete-after-export`` only after
the JSON artifact has been written successfully and reviewed for the target
queue.
"""

from __future__ import annotations

import argparse
import base64
import json
import time
from pathlib import Path
from typing import Any

from azure.identity import AzureCliCredential
from azure.servicebus import ServiceBusClient, ServiceBusSubQueue


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Service Bus dead-letter messages to JSON.")
    parser.add_argument("--namespace", required=True, help="Fully qualified namespace, e.g. name.servicebus.windows.net")
    parser.add_argument("--queue", required=True, help="Queue name")
    parser.add_argument("--out", required=True, type=Path, help="JSON artifact path")
    parser.add_argument("--max-messages", type=int, default=1000)
    parser.add_argument("--complete-after-export", action="store_true", help="Complete exported DLQ messages after writing the artifact")
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    credential = AzureCliCredential()
    exported: list[dict[str, Any]] = []
    completed: list[dict[str, Any]] = []

    with ServiceBusClient(args.namespace, credential=credential) as client:
        with client.get_queue_receiver(queue_name=args.queue, sub_queue=ServiceBusSubQueue.DEAD_LETTER, max_wait_time=5) as receiver:
            for message in receiver.peek_messages(max_message_count=args.max_messages):
                exported.append(_message_to_record(message))

        artifact = {
            "exported_at_unix": time.time(),
            "namespace": args.namespace,
            "queue": args.queue,
            "message_count": len(exported),
            "complete_after_export_requested": bool(args.complete_after_export),
            "messages": exported,
            "completion": {
                "completed_at_unix": None,
                "completed_count": 0,
                "completed_sequence_numbers": [],
                "errors": [],
            },
        }
        _write_json(args.out, artifact)

        if args.complete_after_export and exported:
            exported_sequences = {record["sequence_number"] for record in exported}
            errors: list[dict[str, Any]] = []
            with client.get_queue_receiver(queue_name=args.queue, sub_queue=ServiceBusSubQueue.DEAD_LETTER, max_wait_time=5) as receiver:
                while len(completed) < len(exported_sequences):
                    messages = receiver.receive_messages(max_message_count=50, max_wait_time=5)
                    if not messages:
                        break
                    for message in messages:
                        record = _message_to_record(message)
                        sequence_number = record["sequence_number"]
                        if sequence_number not in exported_sequences:
                            errors.append({"sequence_number": sequence_number, "error": "not_in_export"})
                            continue
                        try:
                            receiver.complete_message(message)
                        except Exception as exc:  # pragma: no cover - SDK/network dependent
                            errors.append({"sequence_number": sequence_number, "error": type(exc).__name__, "message": str(exc)})
                            continue
                        completed.append(record)
            completed_sequences = [record["sequence_number"] for record in completed]
            missing = sorted(exported_sequences - set(completed_sequences))
            if missing:
                errors.append({"error": "exported_messages_not_completed", "sequence_numbers": missing})
            artifact["completion"] = {
                "completed_at_unix": time.time(),
                "completed_count": len(completed),
                "completed_sequence_numbers": completed_sequences,
                "errors": errors,
            }
            _write_json(args.out, artifact)
            if errors:
                print(json.dumps({"out": str(args.out), "exported": len(exported), "completed": len(completed), "errors": errors}, sort_keys=True))
                return 1

    print(json.dumps({"out": str(args.out), "exported": len(exported), "completed": len(completed)}, sort_keys=True))
    return 0


def _message_to_record(message: Any) -> dict[str, Any]:
    body = b"".join(bytes(part) for part in message.body)
    body_text = body.decode("utf-8", errors="replace")
    return {
        "sequence_number": message.sequence_number,
        "message_id": message.message_id,
        "session_id": message.session_id,
        "correlation_id": message.correlation_id,
        "subject": getattr(message, "subject", None),
        "content_type": message.content_type,
        "delivery_count": message.delivery_count,
        "enqueued_time_utc": message.enqueued_time_utc.isoformat() if message.enqueued_time_utc else None,
        "expires_at_utc": message.expires_at_utc.isoformat() if message.expires_at_utc else None,
        "dead_letter_reason": message.dead_letter_reason,
        "dead_letter_error_description": message.dead_letter_error_description,
        "application_properties": _decode_mapping(dict(message.application_properties or {})),
        "body_base64": base64.b64encode(body).decode("ascii"),
        "body_utf8_preview": body_text[:4096],
        "body_sha256": _sha256(body),
    }


def _decode_mapping(values: dict[Any, Any]) -> dict[str, Any]:
    return {_decode_value(key): _decode_value(value) for key, value in values.items()}


def _decode_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _sha256(value: bytes) -> str:
    import hashlib

    return hashlib.sha256(value).hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
