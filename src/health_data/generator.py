import hashlib
import math
import random
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from uuid import UUID, uuid5

from health_data.models import (
    ActivityState,
    AgeBand,
    FitnessLevel,
    HealthMetricEvent,
    UserProfile,
)

EVENT_NAMESPACE = UUID("4a490534-44d8-41a5-84dc-f20cfc1b7543")
BASE_INTERVAL_SECONDS = {
    ActivityState.SLEEP: 600,
    ActivityState.REST: 300,
    ActivityState.ACTIVE: 120,
    ActivityState.WORKOUT: 30,
}


class HealthDataGenerator:
    """Deterministic, state-aware synthetic heart-rate generator."""

    def __init__(self, seed: int, user_count: int) -> None:
        self.seed = seed
        self.user_count = user_count
        self.profiles = self._build_profiles()

    def _stable_random(self, *parts: object) -> random.Random:
        material = ":".join(str(part) for part in (self.seed, *parts))
        digest = hashlib.sha256(material.encode()).digest()
        return random.Random(int.from_bytes(digest[:8], "big"))

    def _build_profiles(self) -> list[UserProfile]:
        profiles: list[UserProfile] = []
        for index in range(1, self.user_count + 1):
            rng = self._stable_random("profile", index)
            age_band = rng.choices(
                list(AgeBand),
                weights=[0.25, 0.35, 0.30, 0.10],
                k=1,
            )[0]
            fitness = rng.choices(
                list(FitnessLevel),
                weights=[0.25, 0.50, 0.25],
                k=1,
            )[0]
            fitness_baseline = {
                FitnessLevel.LOW: 73,
                FitnessLevel.MODERATE: 63,
                FitnessLevel.HIGH: 53,
            }[fitness]
            age_adjustment = {
                AgeBand.YOUNG_ADULT: 0,
                AgeBand.ADULT: 1,
                AgeBand.MIDDLE_AGED: 3,
                AgeBand.SENIOR: 5,
            }[age_band]
            baseline = min(92, max(45, rng.gauss(fitness_baseline + age_adjustment, 3.5)))
            profiles.append(
                UserProfile(
                    user_id=f"user-{index:04d}",
                    device_id=f"device-{index:04d}",
                    age_band=age_band,
                    fitness_level=fitness,
                    baseline_resting_hr=round(baseline, 1),
                    workout_hour=rng.choices(
                        [6, 7, 12, 17, 18, 19],
                        weights=[2, 3, 1, 2, 4, 2],
                        k=1,
                    )[0],
                    chronotype_offset_hours=rng.choices(
                        [-2, -1, 0, 1, 2],
                        weights=[1, 2, 5, 2, 1],
                        k=1,
                    )[0],
                )
            )
        return profiles

    def gap_windows(self, profile: UserProfile, day: date) -> list[tuple[datetime, datetime]]:
        """Return deterministic device-off periods for a user and UTC day."""
        rng = self._stable_random(profile.user_id, day.isoformat(), "gaps")
        if rng.random() >= 0.18:
            return []
        start_hour = rng.randint(9, 20)
        start_minute = rng.randrange(0, 60, 10)
        duration = timedelta(minutes=rng.randint(6, 30) * 10)
        start = datetime(
            day.year,
            day.month,
            day.day,
            start_hour,
            start_minute,
            tzinfo=UTC,
        )
        return [(start, min(start + duration, start + timedelta(hours=6)))]

    def activity_state(self, profile: UserProfile, observed_at: datetime) -> ActivityState:
        local_time = observed_at + timedelta(hours=profile.chronotype_offset_hours)
        hour = local_time.hour + local_time.minute / 60
        if hour >= 23 or hour < 7:
            return ActivityState.SLEEP

        day_rng = self._stable_random(profile.user_id, local_time.date(), "workout")
        workout_probability = {
            FitnessLevel.LOW: 0.28,
            FitnessLevel.MODERATE: 0.48,
            FitnessLevel.HIGH: 0.68,
        }[profile.fitness_level]
        workout_duration_hours = day_rng.uniform(0.5, 1.25)
        if (
            day_rng.random() < workout_probability
            and profile.workout_hour <= hour < profile.workout_hour + workout_duration_hours
        ):
            return ActivityState.WORKOUT

        if 7 <= hour < 9 or 17 <= hour < 21:
            return ActivityState.ACTIVE

        slot = int(local_time.timestamp() // (30 * 60))
        slot_rng = self._stable_random(profile.user_id, slot, "activity")
        if 9 <= hour < 17 and slot_rng.random() < 0.20:
            return ActivityState.ACTIVE
        return ActivityState.REST

    def _heart_rate(
        self,
        profile: UserProfile,
        observed_at: datetime,
        state: ActivityState,
    ) -> float:
        rng = self._stable_random(profile.user_id, observed_at.isoformat(), "heart-rate")
        local_hour = (observed_at + timedelta(hours=profile.chronotype_offset_hours)).hour
        circadian = 3.0 * math.sin(2 * math.pi * (local_hour - 10) / 24)
        state_offset = {
            ActivityState.SLEEP: -9.0,
            ActivityState.REST: 0.0,
            ActivityState.ACTIVE: 24.0,
            ActivityState.WORKOUT: 66.0,
        }[state]
        noise = {
            ActivityState.SLEEP: 2.0,
            ActivityState.REST: 3.5,
            ActivityState.ACTIVE: 7.0,
            ActivityState.WORKOUT: 10.0,
        }[state]
        workout_wave = (
            10 * math.sin(2 * math.pi * observed_at.minute / 60)
            if state is ActivityState.WORKOUT
            else 0
        )
        value = profile.baseline_resting_hr + circadian + state_offset + workout_wave
        return round(min(205, max(35, rng.gauss(value, noise))), 1)

    def _event(
        self,
        profile: UserProfile,
        observed_at: datetime,
        state: ActivityState,
    ) -> HealthMetricEvent:
        observed_at = observed_at.astimezone(UTC)
        event_id = uuid5(
            EVENT_NAMESPACE,
            f"{self.seed}:{profile.user_id}:{observed_at.isoformat()}",
        )
        return HealthMetricEvent(
            event_id=event_id,
            user_id=profile.user_id,
            device_id=profile.device_id,
            observed_at=observed_at,
            metric_value=self._heart_rate(profile, observed_at, state),
            activity_state=state,
            user_profile=profile,
        )

    @staticmethod
    def _in_gap(
        observed_at: datetime,
        gaps: list[tuple[datetime, datetime]],
    ) -> tuple[bool, datetime | None]:
        for gap_start, gap_end in gaps:
            if gap_start <= observed_at < gap_end:
                return True, gap_end
        return False, None

    def generate_events(
        self,
        start: datetime,
        end: datetime,
    ) -> Iterator[HealthMetricEvent]:
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("start and end must include a timezone")
        if start >= end:
            raise ValueError("start must be before end")
        start = start.astimezone(UTC)
        end = end.astimezone(UTC)

        for profile in self.profiles:
            cursor_rng = self._stable_random(profile.user_id, start.isoformat(), "first-sample")
            cursor = start + timedelta(seconds=cursor_rng.randint(0, 300))
            gaps_by_day: dict[date, list[tuple[datetime, datetime]]] = {}
            while cursor < end:
                gaps = gaps_by_day.setdefault(
                    cursor.date(),
                    self.gap_windows(profile, cursor.date()),
                )
                in_gap, gap_end = self._in_gap(cursor, gaps)
                if in_gap and gap_end:
                    cursor = gap_end
                    continue

                state = self.activity_state(profile, cursor)
                yield self._event(profile, cursor, state)
                base_interval = BASE_INTERVAL_SECONDS[state]
                interval_rng = self._stable_random(
                    profile.user_id,
                    cursor.isoformat(),
                    "interval",
                )
                jittered_interval = base_interval * interval_rng.uniform(0.80, 1.20)
                cursor += timedelta(seconds=max(20, round(jittered_interval)))

    def generate_live_snapshot(
        self,
        observed_at: datetime,
        elapsed_seconds: int,
    ) -> Iterator[HealthMetricEvent]:
        """Sample the population at state-dependent rates for a live producer tick."""
        observed_at = observed_at.astimezone(UTC).replace(microsecond=0)
        for profile in self.profiles:
            gaps = self.gap_windows(profile, observed_at.date())
            if self._in_gap(observed_at, gaps)[0]:
                continue
            state = self.activity_state(profile, observed_at)
            inclusion_rng = self._stable_random(
                profile.user_id,
                observed_at.isoformat(),
                "live-inclusion",
            )
            inclusion_probability = min(
                1.0,
                elapsed_seconds / BASE_INTERVAL_SECONDS[state],
            )
            if inclusion_rng.random() <= inclusion_probability:
                yield self._event(profile, observed_at, state)
