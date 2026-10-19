from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from health_data.models import Aggregation, QueryRequest

AGGREGATION_SQL = {
    Aggregation.AVG: "avg(metric_value)",
    Aggregation.MIN: "min(metric_value)",
    Aggregation.MAX: "max(metric_value)",
    Aggregation.COUNT: "count(*)::double precision",
    Aggregation.P50: "percentile_cont(0.50) WITHIN GROUP (ORDER BY metric_value)",
    Aggregation.P95: "percentile_cont(0.95) WITHIN GROUP (ORDER BY metric_value)",
}


@dataclass(frozen=True)
class BuiltQuery:
    sql: str
    parameters: list[Any]
    start_time: datetime
    end_time: datetime


def build_query(request: QueryRequest, now: datetime | None = None) -> BuiltQuery:
    start_time, end_time = request.resolved_range(now)
    parameters: list[Any] = [request.metric, start_time, end_time]
    filters = [
        "metric_name = $1",
        "observed_at >= $2",
        "observed_at < $3",
    ]

    if request.user_ids:
        parameters.append(request.user_ids)
        filters.append(f"user_id = ANY(${len(parameters)}::text[])")
    if request.activity_states:
        parameters.append([state.value for state in request.activity_states])
        filters.append(f"activity_state = ANY(${len(parameters)}::text[])")

    parameters.append(request.limit)
    limit_placeholder = f"${len(parameters)}"
    bucket_expression = f"date_trunc('{request.granularity.value}', observed_at)"
    selected_user = ", user_id" if request.group_by_user else ""
    grouped_user = ", user_id" if request.group_by_user else ""
    ordered_user = ", user_id ASC" if request.group_by_user else ""
    aggregation_expression = AGGREGATION_SQL[request.aggregation]

    sql = f"""
        SELECT
            {bucket_expression} AS bucket
            {selected_user},
            round(({aggregation_expression})::numeric, 2) AS value,
            count(*) AS sample_count
        FROM measurements
        WHERE {" AND ".join(filters)}
        GROUP BY bucket{grouped_user}
        ORDER BY bucket DESC{ordered_user}
        LIMIT {limit_placeholder}
    """
    return BuiltQuery(
        sql=sql,
        parameters=parameters,
        start_time=start_time,
        end_time=end_time,
    )


async def execute_query(
    pool: asyncpg.Pool,
    request: QueryRequest,
    now: datetime | None = None,
) -> tuple[BuiltQuery, list[dict[str, Any]]]:
    query = build_query(request, now)
    async with pool.acquire() as connection:
        records = await connection.fetch(query.sql, *query.parameters)
    return query, [dict(record) for record in records]
