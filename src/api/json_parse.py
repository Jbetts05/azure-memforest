"""JSON parsing helpers shared by model clients."""

from __future__ import annotations

import json
import re
from typing import Any


def parse_json_object(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        if len(lines) >= 3 and lines[-1].strip().startswith("```"):
            raw = "\n".join(lines[1:-1]).strip()
            if raw.startswith("json"):
                raw = raw[4:].strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start < 0 or end <= start:
            return _extract_summary_from_truncated(raw)
        try:
            parsed = json.loads(raw[start : end + 1])
        except json.JSONDecodeError:
            return _extract_summary_from_truncated(raw)
    if not isinstance(parsed, dict):
        return {}
    if "summary" not in parsed:
        fallback = _extract_summary_from_truncated(raw)
        if fallback.get("summary"):
            return fallback
    return parsed


def _extract_summary_from_truncated(raw: str) -> dict[str, Any]:
    match = re.search(r'"summary"\s*:\s*"(.*)', raw, re.DOTALL)
    if not match:
        return {}
    raw_value = match.group(1)
    end = _find_json_string_end(raw_value)
    if end >= 0:
        value = raw_value[:end]
    else:
        value = raw_value.rstrip()
        last_stop = max(value.rfind("."), value.rfind("!"), value.rfind("?"))
        if last_stop > len(value) // 2:
            value = value[: last_stop + 1]
    value = value.replace('\\"', '"').replace("\\n", " ").strip()
    return {"summary": value} if value else {}


def _find_json_string_end(value: str) -> int:
    index = 0
    while index < len(value):
        if value[index] == "\\" and index + 1 < len(value):
            index += 2
        elif value[index] == '"':
            return index
        else:
            index += 1
    return -1
