from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlparse

from sqlalchemy.ext.asyncio import AsyncSession

from crawler.db.queries import (
    create_crawl_run,
    finish_crawl_run,
    upsert_job_postings_batch,
)
from crawler.db.session import async_session_factory
from crawler.generic.cache import CachedLLMExtractor
from crawler.generic.extractor import LLMExtractor
from crawler.generic.fetcher import StealthFetcher
from crawler.jobspy_adapter import search_job_boards
from crawler.settings import load_search_config, settings

logger = logging.getLogger(__name__)


class CrawlEngine:
    """Orchestrates the full crawl pipeline: config → search → dedupe → store."""

    def __init__(self):
        self.fetcher = StealthFetcher()
        self.llm_extractor = LLMExtractor(model_key=settings.llm_model_key)
        self.cached_extractor = CachedLLMExtractor(self.llm_extractor)
        self.config = load_search_config()

    async def run_full_crawl(self) -> dict[str, Any]:
        """Execute a full crawl based on the search config."""
        searches = self.config.get("searches", [])
        total_stats = {"new": 0, "updated": 0, "unchanged": 0, "error": 0}

        for search_block in searches:
            keywords = search_block.get("keywords", [])
            locations = search_block.get("locations", [""])
            job_boards = search_block.get("job_boards", [])
            company_pages = search_block.get("company_pages", [])

            for keyword in keywords:
                for location in locations:
                    # Crawl job boards via JobSpy
                    if job_boards:
                        stats = await self._crawl_job_boards(keyword, location, job_boards)
                        for k, v in stats.items():
                            total_stats[k] = total_stats.get(k, 0) + v

                    # Crawl company career pages
                    for page_config in company_pages:
                        stats = await self._crawl_company_page(keyword, location, page_config)
                        for k, v in stats.items():
                            total_stats[k] = total_stats.get(k, 0) + v

        logger.info("Full crawl complete: %s", total_stats)
        return total_stats

    async def _crawl_job_boards(
        self, keyword: str, location: str, sites: list[str]
    ) -> dict[str, int]:
        """Crawl job boards using JobSpy adapter."""
        async with async_session_factory() as session:
            run = await create_crawl_run(session, keyword, source="jobspy")

            try:
                # JobSpy is synchronous, run it directly
                jobs = search_job_boards(
                    keyword=keyword,
                    location=location,
                    sites=sites,
                )

                if not jobs:
                    await finish_crawl_run(session, run.id, status="completed", new_count=0)
                    return {"new": 0, "updated": 0, "unchanged": 0}

                counts = await upsert_job_postings_batch(session, jobs)

                await finish_crawl_run(
                    session,
                    run.id,
                    status="completed",
                    new_count=counts.get("new", 0),
                    updated_count=counts.get("updated", 0),
                    error_count=counts.get("error", 0),
                )

                logger.info(
                    "JobSpy crawl done: keyword=%r, location=%r, results=%s",
                    keyword, location, counts,
                )
                return counts

            except Exception as e:
                logger.exception("JobSpy crawl failed: keyword=%r", keyword)
                await finish_crawl_run(
                    session, run.id, status="failed", error_message=str(e)
                )
                return {"error": 1}

    async def _crawl_company_page(
        self, keyword: str, location: str, page_config: dict
    ) -> dict[str, int]:
        """Crawl a single company career page using stealth fetcher + LLM extraction."""
        url_template = page_config.get("url", "")
        js_render = page_config.get("js_render", False)

        # Substitute keyword and location into URL template
        url = url_template.format(
            keyword=keyword.replace(" ", "+"),
            location=location.replace(" ", "+").replace(",", "%2C"),
        )

        domain = urlparse(url).netloc
        source_name = domain

        async with async_session_factory() as session:
            run = await create_crawl_run(session, keyword, source=source_name)

            try:
                # Fetch the page
                html = await self.fetcher.fetch(url, js_render=js_render)
                if not html:
                    await finish_crawl_run(
                        session, run.id, status="failed",
                        error_message=f"Failed to fetch {url}",
                    )
                    return {"error": 1}

                # Extract jobs using cached LLM extractor
                jobs = await self.cached_extractor.extract(
                    session, html, url, keyword
                )

                if not jobs:
                    await finish_crawl_run(session, run.id, status="completed", new_count=0)
                    return {"new": 0, "updated": 0, "unchanged": 0}

                counts = await upsert_job_postings_batch(session, jobs)

                await finish_crawl_run(
                    session,
                    run.id,
                    status="completed",
                    new_count=counts.get("new", 0),
                    updated_count=counts.get("updated", 0),
                    error_count=counts.get("error", 0),
                )

                logger.info(
                    "Company page crawl done: %s, keyword=%r, results=%s",
                    domain, keyword, counts,
                )
                return counts

            except Exception as e:
                logger.exception("Company page crawl failed: %s", url)
                await finish_crawl_run(
                    session, run.id, status="failed", error_message=str(e)
                )
                return {"error": 1}
