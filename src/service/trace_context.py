"""W3C trace context parsing for service log correlation."""

from __future__ import annotations

import re
from dataclasses import dataclass

TRACEPARENT_HEADER = "traceparent"
_TRACEPARENT_PATTERN = re.compile(
    r"^(?P<version>[0-9a-f]{2})-"
    r"(?P<trace_id>[0-9a-f]{32})-"
    r"(?P<parent_id>[0-9a-f]{16})-"
    r"(?P<trace_flags>[0-9a-f]{2})"
    r"(?:-.+)?$"
)


@dataclass(frozen=True)
class TraceContext:
    trace_id: str
    parent_id: str
    trace_flags: str


def parse_traceparent(value: str | None) -> TraceContext | None:
    """Parse a W3C traceparent header for correlation only."""

    if value is None:
        return None
    match = _TRACEPARENT_PATTERN.fullmatch(str(value).strip().lower())
    if not match:
        return None
    version = match.group("version")
    trace_id = match.group("trace_id")
    parent_id = match.group("parent_id")
    trace_flags = match.group("trace_flags")
    if version == "ff":
        return None
    if trace_id == "0" * 32 or parent_id == "0" * 16:
        return None
    return TraceContext(
        trace_id=trace_id,
        parent_id=parent_id,
        trace_flags=trace_flags,
    )
