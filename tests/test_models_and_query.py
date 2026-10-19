from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from health_data.models import HealthMetricEvent, QueryRequest, UserProfile
from health_data.query import build_query


def test_query_builder_uses_bound_filter_parameters() -> None:
    start = datetime(2026, 10, 1, tzinfo=UTC)
    request = QueryRequest(
        start_time=start,
        end_time=start + timedelta(days=2),
        user_ids=["user-0001", "user-0002"],
        activity_states=["sleep", "rest"],
        aggregation="p95",
        granularity="day",
        limit=25,
    )

    built = build_query(request)

    assert "user_id = ANY($4::text[])" in built.sql
    assert "activity_state = ANY($5::text[])" in built.sql
    assert "percentile_cont(0.95)" in built.sql
    assert "LIMIT $6" in built.sql
    assert "user-0001" not in built.sql
    assert built.parameters[3] == ["user-0001", "user-0002"]
    assert built.parameters[-1] == 25


def test_query_defaults_to_last_24_hours() -> None:
    now = datetime(2026, 10, 19, 12, tzinfo=UTC)
    built = build_query(QueryRequest(), now=now)

    assert built.end_time == now
    assert built.start_time == now - timedelta(hours=24)


@pytest.mark.parametrize(
    "payload",
    [
        {"user_ids": ["user-0001' OR 1=1"]},
        {
            "start_time": "2026-01-01T00:00:00Z",
            "end_time": "2026-03-01T00:00:00Z",
        },
        {
            "start_time": "2026-10-02T00:00:00Z",
            "end_time": "2026-10-01T00:00:00Z",
        },
        {
            "start_time": datetime.now(UTC) + timedelta(minutes=5),
        },
    ],
)
def test_query_rejects_unsafe_or_unbounded_input(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        QueryRequest(**payload)


def test_event_rejects_mismatched_profile() -> None:
    profile = UserProfile(
        user_id="user-0001",
        device_id="device-0001",
        age_band="30-44",
        fitness_level="moderate",
        baseline_resting_hr=65,
        workout_hour=18,
        chronotype_offset_hours=0,
    )

    with pytest.raises(ValidationError, match="does not match user_profile"):
        HealthMetricEvent(
            event_id=uuid4(),
            user_id="user-0002",
            device_id="device-0001",
            observed_at=datetime.now(UTC),
            metric_value=70,
            activity_state="rest",
            user_profile=profile,
        )
