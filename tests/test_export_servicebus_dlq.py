from __future__ import annotations

import base64
from datetime import datetime, timezone

from scripts.export_servicebus_dlq import _decode_mapping, _message_to_record


class FakeMessage:
    sequence_number = 42
    message_id = "message-1"
    session_id = "session-1"
    correlation_id = "corr-1"
    subject = "subject"
    content_type = "application/json"
    delivery_count = 3
    enqueued_time_utc = datetime(2026, 1, 1, tzinfo=timezone.utc)
    expires_at_utc = None
    dead_letter_reason = "reason"
    dead_letter_error_description = "description"
    application_properties = {b"job_id": b"job-1", "attempt": 2}
    body = [b'{"hello":"world"}']


def test_decode_mapping_decodes_byte_keys_and_values() -> None:
    assert _decode_mapping({b"job_id": b"job-1", "count": 2}) == {"job_id": "job-1", "count": 2}


def test_message_to_record_preserves_body() -> None:
    record = _message_to_record(FakeMessage())

    assert record["application_properties"] == {"job_id": "job-1", "attempt": 2}
    assert record["body_base64"] == base64.b64encode(b'{"hello":"world"}').decode("ascii")
    assert record["body_utf8_preview"] == '{"hello":"world"}'
    assert record["body_sha256"]
