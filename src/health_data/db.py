import asyncio
import logging
from collections.abc import Sequence

import asyncpg

from health_data.models import HealthMetricEvent

logger = logging.getLogger(__name__)


async def create_pool_with_retry(
    dsn: str,
    min_size: int = 2,
    max_size: int = 10,
    attempts: int = 20,
) -> asyncpg.Pool:
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size)
        except (OSError, asyncpg.PostgresError):
            if attempt == attempts:
                raise
            logger.warning(
                "database connection failed; retrying",
                extra={"event": "database_retry", "count": attempt},
            )
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 5.0)
    raise RuntimeError("database connection attempts exhausted")


async def store_events(pool: asyncpg.Pool, events: Sequence[HealthMetricEvent]) -> None:
    if not events:
        return

    profiles = {
        event.user_profile.user_id: (
            event.user_profile.user_id,
            event.user_profile.device_id,
            event.user_profile.age_band.value,
            event.user_profile.fitness_level.value,
            event.user_profile.baseline_resting_hr,
        )
        for event in events
    }
    measurements = [
        (
            event.event_id,
            event.user_id,
            event.observed_at,
            event.metric_name,
            event.metric_value,
            event.unit,
            event.activity_state.value,
            event.device_id,
            event.schema_version,
        )
        for event in events
    ]

    async with pool.acquire() as connection, connection.transaction():
        await connection.executemany(
            """
            INSERT INTO synthetic_users (
                user_id, device_id, age_band, fitness_level, baseline_resting_hr
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (user_id) DO UPDATE SET
                device_id = EXCLUDED.device_id,
                age_band = EXCLUDED.age_band,
                fitness_level = EXCLUDED.fitness_level,
                baseline_resting_hr = EXCLUDED.baseline_resting_hr
            """,
            list(profiles.values()),
        )
        await connection.executemany(
            """
            INSERT INTO measurements (
                event_id, user_id, observed_at, metric_name, metric_value,
                unit, activity_state, device_id, schema_version
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (event_id, observed_at) DO NOTHING
            """,
            measurements,
        )
