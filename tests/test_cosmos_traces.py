from __future__ import annotations

import pytest

from src.cloud.cosmos_traces import BufferingTraceSink, CosmosTraceRecorder, TraceNotFound
from src.observability.tracing import TraceEvent, TraceEventType


class NotFoundError(Exception):
    status_code = 404


class FakeTraceContainer:
    def __init__(self):
        self.items = {}

    def upsert_item(self, item, *, partition_key):
        self.items[(partition_key, item["id"])] = dict(item)
        return item

    def read_item(self, *, item, partition_key):
        key = (partition_key, item)
        if key not in self.items:
            raise NotFoundError()
        return dict(self.items[key])


def _event(query_id="query-1", event_type=TraceEventType.QUERY_STARTED, **kwargs):
    return TraceEvent(query_id=query_id, event_type=event_type, **kwargs)


@pytest.mark.asyncio
async def test_cosmos_trace_recorder_round_trips_trace() -> None:
    container = FakeTraceContainer()
    recorder = CosmosTraceRecorder(container=container)

    document = await recorder.record_events(
        tenant_id="tenant-1",
        user_id="user-1",
        query_id="query-1",
        forest_version="v1",
        events=[
            _event(attributes={"count": 1, "safe": "value"}),
            _event(event_type=TraceEventType.QUERY_COMPLETED),
        ],
    )
    read = await recorder.get_trace(
        tenant_id="tenant-1",
        user_id="user-1",
        query_id="query-1",
    )

    assert document["event_count"] == 2
    assert read["events"][0]["attributes"]["safe"] == "value"


@pytest.mark.asyncio
async def test_cosmos_trace_recorder_hides_cross_user_trace() -> None:
    recorder = CosmosTraceRecorder(container=FakeTraceContainer())
    await recorder.record_events(
        tenant_id="tenant-1",
        user_id="user-1",
        query_id="query-1",
        forest_version="v1",
        events=[_event()],
    )

    with pytest.raises(TraceNotFound):
        await recorder.get_trace(tenant_id="tenant-1", user_id="other-user", query_id="query-1")


@pytest.mark.asyncio
async def test_cosmos_trace_recorder_truncates_event_count() -> None:
    recorder = CosmosTraceRecorder(container=FakeTraceContainer(), max_events_per_query=1)

    document = await recorder.record_events(
        tenant_id="tenant-1",
        user_id="user-1",
        query_id="query-1",
        forest_version="v1",
        events=[_event(), _event(event_type=TraceEventType.QUERY_COMPLETED)],
    )

    assert document["event_count"] == 1
    assert document["trace_truncated"] is True


@pytest.mark.asyncio
async def test_buffering_trace_sink_flushes_to_recorder() -> None:
    recorder = CosmosTraceRecorder(container=FakeTraceContainer())
    sink = BufferingTraceSink(recorder)
    sink.emit(_event())
    sink.emit(_event(event_type=TraceEventType.QUERY_COMPLETED))

    await sink.flush(
        query_id="query-1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
    )

    assert sink.events("query-1") == []
    trace = await recorder.get_trace(tenant_id="tenant-1", user_id="user-1", query_id="query-1")
    assert trace["event_count"] == 2


@pytest.mark.asyncio
async def test_buffering_trace_sink_bounds_events_and_marks_truncated() -> None:
    recorder = CosmosTraceRecorder(container=FakeTraceContainer())
    sink = BufferingTraceSink(recorder, max_events_per_query=1)
    sink.emit(_event())
    sink.emit(_event(event_type=TraceEventType.QUERY_COMPLETED))

    await sink.flush(
        query_id="query-1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
    )

    trace = await recorder.get_trace(tenant_id="tenant-1", user_id="user-1", query_id="query-1")
    assert trace["event_count"] == 1
    assert trace["trace_truncated"] is True


@pytest.mark.asyncio
async def test_buffering_trace_sink_does_not_raise_on_recorder_failure() -> None:
    class BrokenRecorder:
        async def record_events(self, **kwargs):
            raise RuntimeError("cosmos down")

    sink = BufferingTraceSink(BrokenRecorder())
    sink.emit(_event())

    await sink.flush(
        query_id="query-1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
    )

    assert sink.events("query-1") == []


@pytest.mark.asyncio
async def test_trace_recorder_sanitizes_attributes() -> None:
    recorder = CosmosTraceRecorder(container=FakeTraceContainer())

    document = await recorder.record_events(
        tenant_id="tenant-1",
        user_id="user-1",
        query_id="query-1",
        forest_version="v1",
        events=[
            _event(attributes={"ok": True, "long": "x" * 300, "bad": {"nested": "no"}}),
        ],
    )

    attrs = document["events"][0]["attributes"]
    assert attrs["ok"] is True
    assert len(attrs["long"]) == 256
    assert "bad" not in attrs
    assert document["dropped_attribute_count"] == 1
