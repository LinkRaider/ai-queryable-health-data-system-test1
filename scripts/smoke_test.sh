#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"

echo "Waiting for API readiness..."
for _ in $(seq 1 60); do
  if curl --fail --silent "${BASE_URL}/health/ready" >/dev/null; then
    break
  fi
  sleep 2
done
curl --fail --silent "${BASE_URL}/health/ready" >/dev/null

echo "Waiting for ingested measurements..."
for _ in $(seq 1 90); do
  metrics="$(curl --fail --silent "${BASE_URL}/v1/metrics")"
  if python -c \
    'import json,sys; data=json.loads(sys.argv[1]); raise SystemExit(not data["items"] or data["items"][0]["measurement_count"] < 1000)' \
    "${metrics}"; then
    break
  fi
  sleep 2
done

metrics="$(curl --fail --silent "${BASE_URL}/v1/metrics")"
python -c \
  'import json,sys; data=json.loads(sys.argv[1]); assert data["items"][0]["measurement_count"] >= 1000' \
  "${metrics}"

echo "Waiting for all synthetic users..."
for _ in $(seq 1 90); do
  users="$(curl --fail --silent "${BASE_URL}/v1/users?limit=100")"
  if python -c \
    'import json,sys; data=json.loads(sys.argv[1]); raise SystemExit(data["total"] != 50)' \
    "${users}"; then
    break
  fi
  sleep 2
done

users="$(curl --fail --silent "${BASE_URL}/v1/users?limit=100")"
python -c \
  'import json,sys; data=json.loads(sys.argv[1]); assert data["total"] == 50; assert len(data["items"]) == 50' \
  "${users}"

result="$(
  curl --fail --silent \
    -H 'Content-Type: application/json' \
    -d '{
      "metric": "heart_rate_bpm",
      "aggregation": "avg",
      "granularity": "day",
      "group_by_user": false,
      "activity_states": ["rest"],
      "limit": 10
    }' \
    "${BASE_URL}/v1/query"
)"
python -c \
  'import json,sys; data=json.loads(sys.argv[1]); assert data["row_count"] > 0; assert data["rows"][0]["sample_count"] > 0' \
  "${result}"

echo "Checking local natural-language planning..."
nl_result=""
for _ in $(seq 1 6); do
  if nl_result="$(
    curl --fail --silent \
      -H 'Content-Type: application/json' \
      -d '{
        "question": "Show the average resting heart rate for user-0001 by day over the last 7 days.",
        "user_id": "user-0001"
      }' \
      "${BASE_URL}/v1/query/natural-language"
  )"; then
    break
  fi
  sleep 5
done
test -n "${nl_result}"
python -c \
  'import json,sys; data=json.loads(sys.argv[1]); assert data["row_count"] > 0; assert "SELECT" in data["sql"]; assert data["interpretation"]["metric"] == "heart_rate_bpm"' \
  "${nl_result}"

echo "Smoke test passed: ingestion, structured queries, and natural-language queries work."
