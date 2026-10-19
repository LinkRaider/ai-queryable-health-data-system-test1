import logging
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import ORJSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from health_data.config import get_settings
from health_data.db import create_pool_with_retry
from health_data.logging_config import configure_logging
from health_data.models import (
    NaturalLanguageQueryRequest,
    NaturalLanguageQueryResponse,
    QueryRequest,
    QueryResponse,
)
from health_data.natural_language import (
    InvalidModelResponse,
    NaturalLanguageServiceUnavailable,
    OllamaPlanner,
    preflight_unsupported_reason,
)
from health_data.query import execute_query

logger = logging.getLogger(__name__)
QUERY_COUNT = Counter(
    "health_query_requests_total",
    "Structured query requests",
    ["status"],
)
QUERY_DURATION = Histogram(
    "health_query_duration_seconds",
    "Structured query execution latency",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    app.state.pool = await create_pool_with_retry(
        settings.postgres_dsn,
        min_size=settings.database_pool_min_size,
        max_size=settings.database_pool_max_size,
    )
    app.state.ollama_planner = OllamaPlanner(
        base_url=settings.ollama_base_url,
        model=settings.ollama_model,
        timeout_seconds=settings.ollama_timeout_seconds,
    )
    app.state.ollama_model = settings.ollama_model
    logger.info("api started", extra={"event": "api_started"})
    yield
    await app.state.pool.close()


app = FastAPI(
    title="AI-Queryable Health Data System",
    summary="Query realistic synthetic heart-rate measurements.",
    version="0.1.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)


def pool_from(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


@app.get("/health/live", tags=["health"])
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready", tags=["health"])
async def readiness(request: Request) -> dict[str, str]:
    try:
        await pool_from(request).fetchval("SELECT 1")
    except (OSError, asyncpg.PostgresError) as error:
        raise HTTPException(status_code=503, detail="database unavailable") from error
    return {"status": "ready"}


@app.get("/metrics", include_in_schema=False)
async def prometheus_metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/users", tags=["discovery"])
async def list_users(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    rows = await pool_from(request).fetch(
        """
        SELECT
            u.user_id,
            u.age_band,
            u.fitness_level,
            u.baseline_resting_hr,
            min(m.observed_at) AS first_observation,
            max(m.observed_at) AS last_observation,
            count(m.event_id) AS measurement_count
        FROM synthetic_users u
        LEFT JOIN measurements m ON m.user_id = u.user_id
        GROUP BY u.user_id
        ORDER BY u.user_id
        LIMIT $1 OFFSET $2
        """,
        limit,
        offset,
    )
    total = await pool_from(request).fetchval("SELECT count(*) FROM synthetic_users")
    return {"total": total, "items": [dict(row) for row in rows]}


@app.get("/v1/metrics", tags=["discovery"])
async def list_metrics(request: Request) -> dict[str, Any]:
    rows = await pool_from(request).fetch(
        """
        SELECT
            metric_name,
            unit,
            min(observed_at) AS first_observation,
            max(observed_at) AS last_observation,
            count(*) AS measurement_count
        FROM measurements
        GROUP BY metric_name, unit
        ORDER BY metric_name
        """
    )
    return {"items": [dict(row) for row in rows]}


@app.post("/v1/query", response_model=QueryResponse, tags=["query"])
async def query_measurements(
    query_request: QueryRequest,
    request: Request,
) -> QueryResponse:
    try:
        with QUERY_DURATION.time():
            built_query, rows = await execute_query(pool_from(request), query_request)
    except (OSError, asyncpg.PostgresError) as error:
        QUERY_COUNT.labels(status="error").inc()
        logger.exception("query failed", extra={"event": "query_failed"})
        raise HTTPException(status_code=503, detail="query engine unavailable") from error

    QUERY_COUNT.labels(status="success").inc()
    return QueryResponse(
        query={
            "metric": query_request.metric,
            "start_time": built_query.start_time,
            "end_time": built_query.end_time,
            "aggregation": query_request.aggregation,
            "granularity": query_request.granularity,
            "group_by_user": query_request.group_by_user,
            "user_ids": query_request.user_ids,
            "activity_states": query_request.activity_states,
            "limit": query_request.limit,
        },
        row_count=len(rows),
        rows=rows,
    )


@app.post(
    "/v1/query/natural-language",
    response_model=NaturalLanguageQueryResponse,
    tags=["query"],
)
async def query_natural_language(
    nl_request: NaturalLanguageQueryRequest,
    request: Request,
) -> NaturalLanguageQueryResponse:
    unsupported_reason = preflight_unsupported_reason(
        nl_request.question,
        nl_request.user_id,
    )
    if unsupported_reason:
        raise HTTPException(status_code=422, detail=unsupported_reason)

    planner: OllamaPlanner = request.app.state.ollama_planner
    try:
        plan = await planner.translate(nl_request.question, nl_request.user_id)
    except NaturalLanguageServiceUnavailable as error:
        raise HTTPException(
            status_code=503,
            detail="natural-language model unavailable",
        ) from error
    except InvalidModelResponse as error:
        raise HTTPException(
            status_code=502,
            detail="natural-language model returned an invalid plan",
        ) from error

    if not plan.supported or plan.query is None:
        raise HTTPException(
            status_code=422,
            detail=plan.unsupported_reason or "question is not supported",
        )

    try:
        built_query, rows = await execute_query(pool_from(request), plan.query)
    except (OSError, asyncpg.PostgresError) as error:
        raise HTTPException(status_code=503, detail="query engine unavailable") from error

    return NaturalLanguageQueryResponse(
        question=nl_request.question,
        model=request.app.state.ollama_model,
        interpretation=plan.query,
        sql=built_query.sql.strip(),
        row_count=len(rows),
        rows=rows,
    )
