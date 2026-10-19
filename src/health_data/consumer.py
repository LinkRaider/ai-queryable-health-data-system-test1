import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from health_data.config import get_settings
from health_data.db import create_pool_with_retry, store_events
from health_data.logging_config import configure_logging
from health_data.models import HealthMetricEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchOutcome:
    valid_count: int
    invalid_count: int


def parse_record(record: ConsumerRecord) -> HealthMetricEvent:
    return HealthMetricEvent.model_validate_json(record.value)


def dlq_payload(record: ConsumerRecord, error: ValidationError) -> bytes:
    payload: dict[str, Any] = {
        "source_topic": record.topic,
        "source_partition": record.partition,
        "source_offset": record.offset,
        "error": error.errors(include_url=False),
        "original_value": record.value.decode(errors="replace"),
    }
    return json.dumps(payload, default=str, separators=(",", ":")).encode()


async def process_batch(
    *,
    pool: Any,
    records: list[ConsumerRecord],
    dlq_producer: AIOKafkaProducer,
    consumer: AIOKafkaConsumer,
    dlq_topic: str,
) -> BatchOutcome:
    valid_events: list[HealthMetricEvent] = []
    invalid_records: list[tuple[ConsumerRecord, ValidationError]] = []
    for record in records:
        try:
            valid_events.append(parse_record(record))
        except ValidationError as error:
            invalid_records.append((record, error))

    await store_events(pool, valid_events)

    dlq_acks: list[asyncio.Future] = []
    for record, error in invalid_records:
        acknowledgement = await dlq_producer.send(
            dlq_topic,
            key=record.key,
            value=dlq_payload(record, error),
        )
        dlq_acks.append(acknowledgement)
    if dlq_acks:
        await asyncio.gather(*dlq_acks)

    await consumer.commit()
    return BatchOutcome(
        valid_count=len(valid_events),
        invalid_count=len(invalid_records),
    )


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    pool = await create_pool_with_retry(
        settings.postgres_dsn,
        min_size=settings.database_pool_min_size,
        max_size=settings.database_pool_max_size,
    )
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        client_id="health-data-ingestor",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=settings.consumer_batch_size,
    )
    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="health-data-dlq-producer",
        enable_idempotence=True,
    )
    await consumer.start()
    await dlq_producer.start()
    logger.info("ingestor started", extra={"event": "ingestor_started"})
    try:
        while True:
            polled = await consumer.getmany(
                timeout_ms=settings.consumer_poll_timeout_ms,
                max_records=settings.consumer_batch_size,
            )
            records = [record for batch in polled.values() for record in batch]
            if not records:
                continue

            started_at = asyncio.get_running_loop().time()
            outcome = await process_batch(
                pool=pool,
                records=records,
                dlq_producer=dlq_producer,
                consumer=consumer,
                dlq_topic=settings.kafka_dlq_topic,
            )
            duration_ms = int((asyncio.get_running_loop().time() - started_at) * 1000)
            logger.info(
                "batch ingested",
                extra={
                    "event": "batch_ingested",
                    "count": outcome.valid_count,
                    "duration_ms": duration_ms,
                    "topic": settings.kafka_topic,
                },
            )
            if outcome.invalid_count:
                logger.warning(
                    "invalid events routed to dead-letter topic",
                    extra={
                        "event": "events_dead_lettered",
                        "count": outcome.invalid_count,
                        "topic": settings.kafka_dlq_topic,
                    },
                )
    finally:
        await consumer.stop()
        await dlq_producer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
