import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from pydantic import ValidationError

from health_data.models import NaturalLanguagePlan, QueryRequest


class NaturalLanguageError(Exception):
    """Base error for natural-language planning failures."""


class NaturalLanguageServiceUnavailable(NaturalLanguageError):
    """Raised when the local model cannot be reached."""


class InvalidModelResponse(NaturalLanguageError):
    """Raised when model output does not match the constrained plan schema."""


def preflight_unsupported_reason(question: str, user_id: str | None) -> str | None:
    normalized = question.casefold()
    if "my" in normalized.split() and user_id is None:
        return "first-person questions require a user_id context"

    prohibited_medical_terms = (
        "diagnos",
        "prescrib",
        "medical advice",
        "treatment",
        "chest pain",
    )
    if any(term in normalized for term in prohibited_medical_terms):
        return "medical diagnosis and treatment questions are not supported"

    unavailable_data_terms = (
        "blood oxygen",
        "spo2",
        "step count",
        "steps",
        "weight",
        "sleep stage",
        "disrupted sleep",
    )
    if any(term in normalized for term in unavailable_data_terms):
        return "the question requires data that is not available"

    if "raw event" in normalized:
        return "raw event retrieval is not supported"
    return None


def build_fallback_plan(
    question: str,
    user_id: str | None,
    now: datetime | None = None,
) -> NaturalLanguagePlan | None:
    normalized = question.casefold()
    if "heart rate" not in normalized and " bpm" not in normalized:
        return None

    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    period_match = re.search(r"last\s+(\d+)\s+days?", normalized)
    if period_match:
        days = int(period_match.group(1))
        if not 1 <= days <= 31:
            return None
        start_time = current_time - timedelta(days=days)
    elif "last week" in normalized:
        start_time = current_time - timedelta(days=7)
    elif "yesterday" in normalized:
        end_of_yesterday = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_of_yesterday - timedelta(days=1)
        current_time = end_of_yesterday
    else:
        start_time = current_time - timedelta(hours=24)

    aggregation = "avg"
    if any(term in normalized for term in ("minimum", "lowest", " min ")):
        aggregation = "min"
    elif any(term in normalized for term in ("maximum", "highest", " max ")):
        aggregation = "max"
    elif any(term in normalized for term in ("median", "p50", "50th percentile")):
        aggregation = "p50"
    elif any(term in normalized for term in ("p95", "95th percentile")):
        aggregation = "p95"
    elif any(term in normalized for term in ("count", "how many")):
        aggregation = "count"

    activity_states: list[str] = []
    state_terms = {
        "rest": ("resting", "at rest"),
        "sleep": ("sleeping", "asleep", "during sleep"),
        "workout": ("workout", "exercise", "exercising"),
        "active": ("active", "activity"),
    }
    for state, terms in state_terms.items():
        if any(term in normalized for term in terms):
            activity_states.append(state)

    explicit_user_ids = list(dict.fromkeys(re.findall(r"user-\d{4}", normalized)))
    if "my" in normalized.split() and user_id:
        explicit_user_ids = [user_id]
    group_by_user = any(
        phrase in normalized
        for phrase in ("which users", "by user", "per user", "each user")
    )
    granularity = "hour" if "by hour" in normalized or "hourly" in normalized else "day"

    query = QueryRequest(
        start_time=start_time,
        end_time=current_time,
        user_ids=explicit_user_ids,
        activity_states=activity_states,
        aggregation=aggregation,
        granularity=granularity,
        group_by_user=group_by_user,
        limit=1000,
    )
    return NaturalLanguagePlan(
        supported=True,
        unsupported_reason=None,
        query=query,
    )


def build_planner_prompt(
    question: str,
    user_id: str | None,
    now: datetime | None = None,
) -> str:
    current_time = (now or datetime.now(UTC)).astimezone(UTC).isoformat()
    user_context = user_id or "none"
    return f"""
Translate the question into the supplied JSON schema. Current UTC time: {current_time}.
Authenticated synthetic-user context: {user_context}.

The only available metric is heart_rate_bpm. Available activity states are sleep, rest,
active, and workout. Available aggregations are avg, min, max, count, p50, and p95.
Available granularities are hour and day. Query ranges cannot exceed 31 days.

Rules:
- Never invent a metric, activity state, user ID, aggregation, or granularity.
- Resolve relative dates to explicit UTC timestamps.
- "resting" means activity state "rest".
- "my" means the authenticated synthetic-user context. If context is none, mark the
  question unsupported instead of guessing a user.
- Use group_by_user=true for comparisons such as "which users"; otherwise use false unless
  separate per-user series were explicitly requested.
- Sleep disruption, diagnoses, advice, raw event retrieval, and non-heart-rate questions are
  unsupported.
- For an unsupported or ambiguous question, set supported=false, explain why in
  unsupported_reason, and set query=null.
- For a supported question, set supported=true, unsupported_reason=null, and populate query.

Question: {question}
""".strip()


def parse_plan(content: str) -> NaturalLanguagePlan:
    normalized = content.strip()
    if normalized.startswith("```"):
        normalized = normalized.removeprefix("```json").removeprefix("```")
        normalized = normalized.removesuffix("```").strip()
    try:
        payload: Any = json.loads(normalized)
        return NaturalLanguagePlan.model_validate(payload)
    except (json.JSONDecodeError, ValidationError, TypeError) as error:
        raise InvalidModelResponse("local model returned an invalid query plan") from error


class OllamaPlanner:
    def __init__(
        self,
        base_url: str,
        model: str,
        timeout_seconds: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.client = client

    async def translate(
        self,
        question: str,
        user_id: str | None,
        now: datetime | None = None,
    ) -> NaturalLanguagePlan:
        body = {
            "model": self.model,
            "stream": False,
            "format": NaturalLanguagePlan.model_json_schema(),
            "options": {"temperature": 0, "seed": 42},
            "messages": [
                {
                    "role": "system",
                    "content": "You are a constrained health-data query planner. Output JSON only.",
                },
                {
                    "role": "user",
                    "content": build_planner_prompt(question, user_id, now),
                },
            ],
        }
        try:
            if self.client is not None:
                response = await self.client.post("/api/chat", json=body)
            else:
                async with httpx.AsyncClient(
                    base_url=self.base_url,
                    timeout=self.timeout_seconds,
                ) as client:
                    response = await client.post("/api/chat", json=body)
            response.raise_for_status()
            content = response.json()["message"]["content"]
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as error:
            raise NaturalLanguageServiceUnavailable(
                "local natural-language model is unavailable"
            ) from error
        try:
            return parse_plan(content)
        except InvalidModelResponse:
            fallback = build_fallback_plan(question, user_id, now)
            if fallback is not None:
                return fallback
            raise
