CREATE TABLE IF NOT EXISTS synthetic_users (
    user_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL UNIQUE,
    age_band TEXT NOT NULL CHECK (age_band IN ('18-29', '30-44', '45-64', '65+')),
    fitness_level TEXT NOT NULL CHECK (fitness_level IN ('low', 'moderate', 'high')),
    baseline_resting_hr DOUBLE PRECISION NOT NULL
        CHECK (baseline_resting_hr BETWEEN 40 AND 100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS measurements (
    event_id UUID NOT NULL,
    user_id TEXT NOT NULL REFERENCES synthetic_users(user_id),
    observed_at TIMESTAMPTZ NOT NULL,
    metric_name TEXT NOT NULL CHECK (metric_name = 'heart_rate_bpm'),
    metric_value DOUBLE PRECISION NOT NULL CHECK (metric_value BETWEEN 25 AND 240),
    unit TEXT NOT NULL CHECK (unit = 'bpm'),
    activity_state TEXT NOT NULL
        CHECK (activity_state IN ('sleep', 'rest', 'active', 'workout')),
    device_id TEXT NOT NULL,
    schema_version SMALLINT NOT NULL CHECK (schema_version = 1),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, observed_at)
) PARTITION BY RANGE (observed_at);

DO $$
DECLARE
    month_offset INTEGER;
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
BEGIN
    FOR month_offset IN -2..1 LOOP
        partition_start := (date_trunc('month', current_date)
            + make_interval(months => month_offset))::date;
        partition_end := (partition_start + INTERVAL '1 month')::date;
        partition_name := 'measurements_' || to_char(partition_start, 'YYYY_MM');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF measurements
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            partition_start,
            partition_end
        );
    END LOOP;
END
$$;

CREATE INDEX IF NOT EXISTS idx_measurements_user_time
    ON measurements (user_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_measurements_metric_time
    ON measurements (metric_name, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_measurements_state_time
    ON measurements (activity_state, observed_at DESC);
