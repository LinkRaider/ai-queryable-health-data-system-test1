FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY pyproject.toml README.md ./
COPY src ./src
COPY tests ./tests

RUN pip install --no-cache-dir ".[test]"

RUN chown -R app:app /app

USER app

CMD ["uvicorn", "health_data.api:app", "--host", "0.0.0.0", "--port", "8000"]
