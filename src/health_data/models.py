import re
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ActivityState(StrEnum):
    SLEEP = "sleep"
    REST = "rest"
    ACTIVE = "active"
    WORKOUT = "workout"


class AgeBand(StrEnum):
    YOUNG_ADULT = "18-29"
    ADULT = "30-44"
    MIDDLE_AGED = "45-64"
    SENIOR = "65+"


class FitnessLevel(StrEnum):
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"


class UserProfile(BaseModel):
    model_config = ConfigDict(frozen=True)

    user_id: str = Field(pattern=r"^user-\d{4}$")
    device_id: str = Field(pattern=r"^device-\d{4}$")
    age_band: AgeBand
    fitness_level: FitnessLevel
    baseline_resting_hr: float = Field(ge=40, le=100)
    workout_hour: int = Field(ge=5, le=21)
    chronotype_offset_hours: int = Field(ge=-2, le=2)


class HealthMetricEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: Literal[1] = 1
    event_id: UUID
    user_id: str = Field(pattern=r"^user-\d{4}$")
    device_id: str = Field(pattern=r"^device-\d{4}$")
    observed_at: datetime
    metric_name: Literal["heart_rate_bpm"] = "heart_rate_bpm"
    metric_value: float = Field(ge=25, le=240)
    unit: Literal["bpm"] = "bpm"
    activity_state: ActivityState
    user_profile: UserProfile

    @field_validator("observed_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("observed_at must include a timezone")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def identifiers_match_profile(self) -> "HealthMetricEvent":
        if self.user_id != self.user_profile.user_id:
            raise ValueError("user_id does not match user_profile")
        if self.device_id != self.user_profile.device_id:
            raise ValueError("device_id does not match user_profile")
        return self


class Aggregation(StrEnum):
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    P50 = "p50"
    P95 = "p95"


class Granularity(StrEnum):
    HOUR = "hour"
    DAY = "day"


class QueryRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "metric": "heart_rate_bpm",
                "user_ids": ["user-0001"],
                "activity_states": ["rest"],
                "aggregation": "avg",
                "granularity": "day",
                "group_by_user": False,
                "limit": 100,
            }
        },
    )

    metric: Literal["heart_rate_bpm"] = "heart_rate_bpm"
    start_time: datetime | None = None
    end_time: datetime | None = None
    user_ids: list[str] = Field(default_factory=list, max_length=100)
    activity_states: list[ActivityState] = Field(default_factory=list, max_length=4)
    aggregation: Aggregation = Aggregation.AVG
    granularity: Granularity = Granularity.HOUR
    group_by_user: bool = True
    limit: int = Field(default=1000, ge=1, le=5000)

    @field_validator("start_time", "end_time")
    @classmethod
    def normalize_timezone(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include a timezone")
        return value.astimezone(UTC)

    @field_validator("user_ids")
    @classmethod
    def validate_user_ids(cls, values: list[str]) -> list[str]:
        invalid = [value for value in values if not re.fullmatch(r"user-\d{4}", value)]
        if invalid:
            raise ValueError("user_ids must use the user-NNNN format")
        return list(dict.fromkeys(values))

    @model_validator(mode="after")
    def validate_range(self) -> "QueryRequest":
        if self.start_time and self.end_time:
            if self.start_time >= self.end_time:
                raise ValueError("start_time must be before end_time")
            if self.end_time - self.start_time > timedelta(days=31):
                raise ValueError("query range cannot exceed 31 days")
        return self

    def resolved_range(self, now: datetime | None = None) -> tuple[datetime, datetime]:
        end = self.end_time or now or datetime.now(UTC)
        start = self.start_time or end - timedelta(hours=24)
        if start >= end:
            raise ValueError("start_time must be before end_time")
        if end - start > timedelta(days=31):
            raise ValueError("query range cannot exceed 31 days")
        return start, end


class QueryResultRow(BaseModel):
    bucket: datetime
    user_id: str | None = None
    value: float
    sample_count: int


class QueryResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "query": {
                    "metric": "heart_rate_bpm",
                    "start_time": "2026-10-18T12:00:00Z",
                    "end_time": "2026-10-19T12:00:00Z",
                    "user_ids": ["user-0001"],
                    "activity_states": ["rest"],
                    "aggregation": "avg",
                    "granularity": "day",
                    "group_by_user": False,
                    "limit": 100,
                },
                "row_count": 1,
                "rows": [
                    {
                        "bucket": "2026-10-19T00:00:00Z",
                        "value": 81.96,
                        "sample_count": 42,
                    }
                ],
            }
        }
    )

    query: QueryRequest
    row_count: int
    rows: list[QueryResultRow]


class NaturalLanguageQueryRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "question": "What was my average resting heart rate by day over the last week?",
                "user_id": "user-0001",
            }
        },
    )

    question: str = Field(min_length=3, max_length=1000)
    user_id: str | None = Field(default=None, pattern=r"^user-\d{4}$")


class NaturalLanguagePlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    supported: bool
    unsupported_reason: str | None = None
    query: QueryRequest | None = None

    @model_validator(mode="after")
    def validate_plan(self) -> "NaturalLanguagePlan":
        if self.supported and self.query is None:
            raise ValueError("supported plans require a query")
        if not self.supported and not self.unsupported_reason:
            raise ValueError("unsupported plans require a reason")
        return self


class NaturalLanguageQueryResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "question": "What was my average resting heart rate last week?",
                "model": "qwen2.5:0.5b",
                "interpretation": {
                    "metric": "heart_rate_bpm",
                    "start_time": "2026-10-12T12:00:00Z",
                    "end_time": "2026-10-19T12:00:00Z",
                    "user_ids": ["user-0001"],
                    "activity_states": ["rest"],
                    "aggregation": "avg",
                    "granularity": "day",
                    "group_by_user": False,
                    "limit": 1000,
                },
                "sql": "SELECT ... FROM measurements WHERE ...",
                "row_count": 1,
                "rows": [
                    {
                        "bucket": "2026-10-19T00:00:00Z",
                        "value": 81.96,
                        "sample_count": 42,
                    }
                ],
            }
        }
    )

    question: str
    model: str
    interpretation: QueryRequest
    sql: str
    row_count: int
    rows: list[QueryResultRow]
