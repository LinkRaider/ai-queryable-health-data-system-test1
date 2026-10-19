import asyncio
import logging
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from health_data.config import get_settings
from health_data.generator import HealthDataGenerator
from health_data.logging_config import configure_logging
from health_data.models import HealthMetricEvent

logger = logging.getLogger(__name__)


async def ensure_topics(bootstrap_servers: str, topics: list[str]) -> None:
    admin = AIOKafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="health-data-topic-admin",
    )
    await admin.start()
    try:
        for topic in topics:
            try:
                await admin.create_topics(
                    [NewTopic(name=topic, num_partitions=3, replication_factor=1)]
                )
            except TopicAlreadyExistsError:
                pass
    finally:
        await admin.close()


async def topic_has_events(bootstrap_servers: str, topic: str) -> bool:
    probe = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        client_id="health-data-bootstrap-probe",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await probe.start()
    try:
        await asyncio.wait_for(probe.getone(), timeout=2.0)
        return True
    except TimeoutError:
        return False
    finally:
        await probe.stop()


async def publish_events(
    producer: AIOKafkaProducer,
    topic: str,
    events: Iterable[HealthMetricEvent],
    flush_every: int = 1000,
) -> int:
    acknowledgements: list[asyncio.Future] = []
    count = 0
    for event in events:
        acknowledgement = await producer.send(
            topic,
            key=event.user_id.encode(),
            value=event.model_dump_json().encode(),
        )
        acknowledgements.append(acknowledgement)
        count += 1
        if len(acknowledgements) >= flush_every:
            await asyncio.gather(*acknowledgements)
            acknowledgements.clear()
    if acknowledgements:
        await asyncio.gather(*acknowledgements)
    return count


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    generator = HealthDataGenerator(
        seed=settings.generator_seed,
        user_count=settings.generator_user_count,
    )
    await ensure_topics(
        settings.kafka_bootstrap_servers,
        [settings.kafka_topic, settings.kafka_dlq_topic],
    )
    stream_has_events = await topic_has_events(
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="health-data-producer",
        enable_idempotence=True,
        compression_type="gzip",
    )
    await producer.start()
    try:
        if stream_has_events:
            logger.info(
                "historical seed skipped because stream contains events",
                extra={
                    "event": "historical_seed_skipped",
                    "topic": settings.kafka_topic,
                },
            )
        else:
            history_end = datetime.now(UTC).replace(second=0, microsecond=0)
            history_start = history_end - timedelta(days=settings.generator_history_days)
            started_at = asyncio.get_running_loop().time()
            count = await publish_events(
                producer,
                settings.kafka_topic,
                generator.generate_events(history_start, history_end),
            )
            duration_ms = int((asyncio.get_running_loop().time() - started_at) * 1000)
            logger.info(
                "historical seed published",
                extra={
                    "event": "historical_seed_published",
                    "count": count,
                    "duration_ms": duration_ms,
                    "topic": settings.kafka_topic,
                },
            )

        while True:
            await asyncio.sleep(settings.generator_live_interval_seconds)
            observed_at = datetime.now(UTC)
            count = await publish_events(
                producer,
                settings.kafka_topic,
                generator.generate_live_snapshot(
                    observed_at,
                    settings.generator_live_interval_seconds,
                ),
            )
            logger.info(
                "live tick published",
                extra={
                    "event": "live_tick_published",
                    "count": count,
                    "topic": settings.kafka_topic,
                },
            )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())
