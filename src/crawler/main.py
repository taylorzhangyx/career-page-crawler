from __future__ import annotations

import asyncio
import logging
import sys

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from crawler.engine import CrawlEngine
from crawler.settings import load_search_config, settings

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


async def run_crawl() -> None:
    """Execute a single crawl run."""
    logger.info("Starting crawl run...")
    engine = CrawlEngine()
    try:
        stats = await engine.run_full_crawl()
        logger.info("Crawl run finished: %s", stats)
    except Exception:
        logger.exception("Crawl run failed")


def parse_cron_expression(cron_str: str) -> dict:
    """Parse a standard cron expression into APScheduler CronTrigger kwargs."""
    parts = cron_str.strip().split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: {cron_str!r} (expected 5 fields)")

    return {
        "minute": parts[0],
        "hour": parts[1],
        "day": parts[2],
        "month": parts[3],
        "day_of_week": parts[4],
    }


async def async_main() -> None:
    """Async entry point: start the scheduler and run crawls on schedule."""
    config = load_search_config()
    schedule_config = config.get("schedule", {})
    cron_expr = schedule_config.get("cron", "0 8 * * *")
    timezone = schedule_config.get("timezone", "UTC")

    logger.info("Career Page Crawler starting")
    logger.info("Schedule: %s (%s)", cron_expr, timezone)

    # Parse cron and set up scheduler
    cron_kwargs = parse_cron_expression(cron_expr)

    scheduler = AsyncIOScheduler(timezone=timezone)
    scheduler.add_job(
        run_crawl,
        trigger=CronTrigger(**cron_kwargs, timezone=timezone),
        id="crawl_job",
        name="Scheduled crawl",
        replace_existing=True,
    )

    scheduler.start()

    logger.info("Scheduler started. Next scheduled run: %s", scheduler.get_job("crawl_job").next_run_time)

    # Run immediately on startup
    await run_crawl()

    # Keep the event loop running for scheduled jobs
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()
        logger.info("Goodbye.")


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("Goodbye.")


if __name__ == "__main__":
    main()
