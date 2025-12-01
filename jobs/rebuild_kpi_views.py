"""Periodic job to refresh KPI materialized views.

This script can be launched as a standalone worker (e.g., `python jobs/rebuild_kpi_views.py`)
or imported inside a process manager. The AsyncIOScheduler keeps a single job
instance to prevent concurrent refreshes.
"""

import asyncio
import logging
from time import perf_counter

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import text

from app.database import engine
from app.logging_config import configure_logging
from app.settings import get_settings

logger = logging.getLogger("app.jobs")


async def refresh_views() -> None:
    start = perf_counter()
    views = ["kpi_daily", "kpi_weekly"]

    async with engine.begin() as connection:
        for view in views:
            view_start = perf_counter()
            await connection.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"))
            duration_ms = round((perf_counter() - view_start) * 1000, 2)
            logger.info(
                "kpi_view_refreshed",
                extra={"event": "kpi_view_refreshed", "view": view, "duration_ms": duration_ms},
            )

    logger.info(
        "kpi_views_refresh_cycle_complete",
        extra={"event": "kpi_views_refresh_cycle_complete", "duration_ms": round((perf_counter() - start) * 1000, 2)},
    )


async def main() -> None:
    settings = get_settings()
    configure_logging()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        refresh_views,
        "interval",
        minutes=settings.kpi_view_refresh_minutes,
        id="refresh_kpi_views",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()

    logger.info(
        "kpi_view_scheduler_started",
        extra={
            "event": "kpi_view_scheduler_started",
            "refresh_interval_minutes": settings.kpi_view_refresh_minutes,
        },
    )

    await refresh_views()  # Run once at startup for freshness
    await asyncio.Event().wait()  # Keep the loop alive


if __name__ == "__main__":
    asyncio.run(main())
