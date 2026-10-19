from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from statistics import mean

from health_data.generator import HealthDataGenerator
from health_data.models import ActivityState


def test_generation_is_deterministic() -> None:
    start = datetime(2026, 10, 1, tzinfo=UTC)
    end = start + timedelta(hours=3)
    first = list(HealthDataGenerator(seed=42, user_count=3).generate_events(start, end))
    second = list(HealthDataGenerator(seed=42, user_count=3).generate_events(start, end))

    assert [(event.event_id, event.metric_value) for event in first] == [
        (event.event_id, event.metric_value) for event in second
    ]
    assert len({event.event_id for event in first}) == len(first)


def test_generated_data_has_realistic_state_relationships_and_irregular_sampling() -> None:
    generator = HealthDataGenerator(seed=20261019, user_count=20)
    start = datetime(2026, 10, 1, tzinfo=UTC)
    events = list(generator.generate_events(start, start + timedelta(days=7)))
    values_by_state: dict[ActivityState, list[float]] = defaultdict(list)
    for event in events:
        values_by_state[event.activity_state].append(event.metric_value)

    assert set(values_by_state) == set(ActivityState)
    assert mean(values_by_state[ActivityState.WORKOUT]) > mean(
        values_by_state[ActivityState.ACTIVE]
    )
    assert mean(values_by_state[ActivityState.ACTIVE]) > mean(
        values_by_state[ActivityState.REST]
    )
    assert mean(values_by_state[ActivityState.REST]) > mean(
        values_by_state[ActivityState.SLEEP]
    )
    assert all(35 <= event.metric_value <= 205 for event in events)

    rest_times = [
        event.observed_at
        for event in events
        if event.user_id == "user-0001" and event.activity_state is ActivityState.REST
    ]
    intervals = {
        int((later - earlier).total_seconds())
        for earlier, later in zip(rest_times, rest_times[1:], strict=False)
        if later - earlier < timedelta(minutes=15)
    }
    assert len(intervals) > 3


def test_device_off_gap_contains_no_events() -> None:
    generator = HealthDataGenerator(seed=20261019, user_count=1)
    profile = generator.profiles[0]
    search_day = date(2026, 10, 1)
    gaps = []
    for offset in range(30):
        gaps = generator.gap_windows(profile, search_day + timedelta(days=offset))
        if gaps:
            break

    assert gaps, "fixed seed must produce at least one gap in the 30-day search window"
    gap_start, gap_end = gaps[0]
    assert list(generator.generate_events(gap_start, gap_end)) == []
