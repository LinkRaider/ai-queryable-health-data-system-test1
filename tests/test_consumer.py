import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import health_data.consumer as consumer_module
from health_data.generator import HealthDataGenerator


def make_record(value: bytes) -> SimpleNamespace:
    return SimpleNamespace(
        value=value,
        key=b"user-0001",
        topic="health-metrics",
        partition=0,
        offset=1,
    )


def valid_record() -> SimpleNamespace:
    generator = HealthDataGenerator(seed=42, user_count=1)
    start = datetime(2026, 10, 1, tzinfo=UTC)
    event = next(generator.generate_events(start, start + timedelta(hours=1)))
    return make_record(event.model_dump_json().encode())


@pytest.mark.asyncio
async def test_batch_commits_only_after_database_and_dlq_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []

    async def store(_pool: object, events: list[object]) -> None:
        assert len(events) == 1
        order.append("database")

    async def send(*_args: object, **_kwargs: object) -> asyncio.Future:
        order.append("dlq_send")

        async def acknowledge() -> None:
            order.append("dlq_ack")

        return asyncio.create_task(acknowledge())

    async def commit() -> None:
        order.append("offset_commit")

    monkeypatch.setattr(consumer_module, "store_events", store)
    dlq_producer = SimpleNamespace(send=AsyncMock(side_effect=send))
    consumer = SimpleNamespace(commit=AsyncMock(side_effect=commit))

    outcome = await consumer_module.process_batch(
        pool=object(),
        records=[valid_record(), make_record(b"{}")],
        dlq_producer=dlq_producer,
        consumer=consumer,
        dlq_topic="health-metrics-dlq",
    )

    assert outcome.valid_count == 1
    assert outcome.invalid_count == 1
    assert order == ["database", "dlq_send", "dlq_ack", "offset_commit"]


@pytest.mark.asyncio
async def test_database_failure_prevents_dlq_and_offset_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = AsyncMock(side_effect=RuntimeError("database unavailable"))
    monkeypatch.setattr(consumer_module, "store_events", store)
    dlq_producer = SimpleNamespace(send=AsyncMock())
    consumer = SimpleNamespace(commit=AsyncMock())

    with pytest.raises(RuntimeError, match="database unavailable"):
        await consumer_module.process_batch(
            pool=object(),
            records=[valid_record(), make_record(b"{}")],
            dlq_producer=dlq_producer,
            consumer=consumer,
            dlq_topic="health-metrics-dlq",
        )

    dlq_producer.send.assert_not_awaited()
    consumer.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_dlq_failure_prevents_offset_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(consumer_module, "store_events", AsyncMock())

    async def send(*_args: object, **_kwargs: object) -> asyncio.Future:
        async def fail() -> None:
            raise RuntimeError("dlq unavailable")

        return asyncio.create_task(fail())

    dlq_producer = SimpleNamespace(send=AsyncMock(side_effect=send))
    consumer = SimpleNamespace(commit=AsyncMock())

    with pytest.raises(RuntimeError, match="dlq unavailable"):
        await consumer_module.process_batch(
            pool=object(),
            records=[make_record(b"{}")],
            dlq_producer=dlq_producer,
            consumer=consumer,
            dlq_topic="health-metrics-dlq",
        )

    consumer.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_offset_commit_failure_leaves_batch_replayable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = AsyncMock()
    monkeypatch.setattr(consumer_module, "store_events", store)
    dlq_producer = SimpleNamespace(send=AsyncMock())
    consumer = SimpleNamespace(
        commit=AsyncMock(side_effect=RuntimeError("commit unavailable"))
    )

    with pytest.raises(RuntimeError, match="commit unavailable"):
        await consumer_module.process_batch(
            pool=object(),
            records=[valid_record()],
            dlq_producer=dlq_producer,
            consumer=consumer,
            dlq_topic="health-metrics-dlq",
        )

    store.assert_awaited_once()
    consumer.commit.assert_awaited_once()
