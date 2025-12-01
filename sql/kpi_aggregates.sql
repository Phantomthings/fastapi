-- Materialized views for daily and weekly KPI rollups built from sessions data.
-- These views assume a PostgreSQL backend and can be refreshed concurrently.

CREATE MATERIALIZED VIEW IF NOT EXISTS kpi_daily AS
SELECT
    site_id,
    DATE_TRUNC('day', started_at)::date AS period_start,
    (DATE_TRUNC('day', started_at) + INTERVAL '1 day')::date AS period_end,
    COUNT(*) AS session_count,
    COALESCE(SUM(energy_kwh), 0) AS total_energy_kwh,
    AVG(energy_kwh) AS average_session_kwh,
    SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at, started_at) - started_at)) / 3600) AS total_session_hours
FROM sessions
GROUP BY site_id, DATE_TRUNC('day', started_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_kpi_daily_site_period
    ON kpi_daily (site_id, period_start);


CREATE MATERIALIZED VIEW IF NOT EXISTS kpi_weekly AS
SELECT
    site_id,
    DATE_TRUNC('week', started_at)::date AS period_start,
    (DATE_TRUNC('week', started_at) + INTERVAL '7 day')::date AS period_end,
    COUNT(*) AS session_count,
    COALESCE(SUM(energy_kwh), 0) AS total_energy_kwh,
    AVG(energy_kwh) AS average_session_kwh,
    SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at, started_at) - started_at)) / 3600) AS total_session_hours
FROM sessions
GROUP BY site_id, DATE_TRUNC('week', started_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_kpi_weekly_site_period
    ON kpi_weekly (site_id, period_start);


-- Example refresh commands (can be orchestrated by APScheduler/cron)
REFRESH MATERIALIZED VIEW CONCURRENTLY kpi_daily;
REFRESH MATERIALIZED VIEW CONCURRENTLY kpi_weekly;
