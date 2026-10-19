import json
from datetime import UTC, datetime

import httpx
import pytest

from health_data.natural_language import (
    InvalidModelResponse,
    OllamaPlanner,
    build_fallback_plan,
    build_planner_prompt,
    parse_plan,
    preflight_unsupported_reason,
)


def test_prompt_includes_time_and_user_context() -> None:
    now = datetime(2026, 10, 19, 12, tzinfo=UTC)
    prompt = build_planner_prompt(
        "What was my average resting heart rate last week?",
        "user-0001",
        now,
    )

    assert now.isoformat() in prompt
    assert "Authenticated synthetic-user context: user-0001" in prompt
    assert '"my" means the authenticated synthetic-user context' in prompt


def test_parse_plan_rejects_non_json_output() -> None:
    with pytest.raises(InvalidModelResponse):
        parse_plan("I cannot answer that.")


@pytest.mark.parametrize(
    ("question", "user_id"),
    [
        ("What was my average resting heart rate?", None),
        ("Diagnose my chest pain.", "user-0001"),
        ("Which users had the most disrupted sleep?", None),
        ("Return every raw event.", None),
    ],
)
def test_preflight_rejects_unsupported_questions(
    question: str,
    user_id: str | None,
) -> None:
    assert preflight_unsupported_reason(question, user_id) is not None


def test_preflight_allows_supported_question() -> None:
    assert (
        preflight_unsupported_reason(
            "Average resting heart rate for user-0001 last week",
            "user-0001",
        )
        is None
    )


def test_fallback_builds_bounded_heart_rate_query() -> None:
    now = datetime(2026, 10, 19, 12, tzinfo=UTC)
    plan = build_fallback_plan(
        "Show my average resting heart rate by day over the last 7 days.",
        "user-0001",
        now,
    )

    assert plan is not None
    assert plan.query is not None
    assert plan.query.start_time == datetime(2026, 10, 12, 12, tzinfo=UTC)
    assert plan.query.end_time == now
    assert plan.query.user_ids == ["user-0001"]
    assert plan.query.activity_states == ["rest"]
    assert plan.query.aggregation == "avg"


@pytest.mark.asyncio
async def test_ollama_planner_requests_structured_output() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert request.url.path == "/api/chat"
        assert body["stream"] is False
        assert body["options"]["temperature"] == 0
        assert body["options"]["seed"] == 42
        assert "properties" in body["format"]
        plan = {
            "supported": True,
            "unsupported_reason": None,
            "query": {
                "metric": "heart_rate_bpm",
                "start_time": "2026-10-12T00:00:00Z",
                "end_time": "2026-10-19T00:00:00Z",
                "user_ids": ["user-0001"],
                "activity_states": ["rest"],
                "aggregation": "avg",
                "granularity": "day",
                "group_by_user": False,
                "limit": 100,
            },
        }
        return httpx.Response(
            200,
            json={"message": {"content": json.dumps(plan)}},
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(
        base_url="http://ollama.test",
        transport=transport,
    ) as client:
        planner = OllamaPlanner(
            base_url="http://ollama.test",
            model="test-model",
            timeout_seconds=5,
            client=client,
        )
        plan = await planner.translate(
            "What was my average resting heart rate last week?",
            "user-0001",
            datetime(2026, 10, 19, tzinfo=UTC),
        )

    assert plan.supported
    assert plan.query is not None
    assert plan.query.user_ids == ["user-0001"]
    assert plan.query.activity_states == ["rest"]


@pytest.mark.asyncio
async def test_ollama_planner_falls_back_for_invalid_supported_plan() -> None:
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            200,
            json={
                "message": {
                    "content": json.dumps(
                        {
                            "supported": True,
                            "unsupported_reason": None,
                            "query": None,
                        }
                    )
                }
            },
        )
    )
    async with httpx.AsyncClient(
        base_url="http://ollama.test",
        transport=transport,
    ) as client:
        planner = OllamaPlanner(
            base_url="http://ollama.test",
            model="test-model",
            timeout_seconds=5,
            client=client,
        )
        plan = await planner.translate(
            "Average resting heart rate for user-0001 over the last 7 days",
            "user-0001",
            datetime(2026, 10, 19, tzinfo=UTC),
        )

    assert plan.query is not None
    assert plan.query.user_ids == ["user-0001"]
