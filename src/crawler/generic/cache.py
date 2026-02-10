from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from crawler.db.queries import get_cached_selectors, save_cached_selectors
from crawler.generic.extractor import (
    LLMExtractor,
    compute_page_signature,
    extract_with_selectors,
)

logger = logging.getLogger(__name__)


class CachedLLMExtractor:
    """Wraps LLMExtractor with a DB-backed selector cache.

    On first visit to a domain/layout, calls the LLM and caches the selectors.
    On subsequent visits, tries cached selectors first and only falls back to
    the LLM if the cached selectors return no results.
    """

    def __init__(self, extractor: LLMExtractor):
        self.extractor = extractor

    async def extract(
        self,
        session: AsyncSession,
        html: str,
        page_url: str,
        keyword: str,
    ) -> list[dict]:
        """Extract job postings, using cached selectors when available."""
        from urllib.parse import urlparse

        domain = urlparse(page_url).netloc
        page_sig = compute_page_signature(html)

        # Try cached selectors first
        cached = await get_cached_selectors(session, domain, page_sig)
        if cached:
            logger.info("Using cached selectors for %s", domain)
            jobs = extract_with_selectors(html, cached, page_url)
            if jobs:
                # Enrich with keyword and source_site
                for job in jobs:
                    job["search_keyword"] = keyword
                    job["source_site"] = domain
                logger.info("Cached selectors extracted %d jobs from %s", len(jobs), domain)
                return jobs
            else:
                logger.info("Cached selectors returned no results for %s, falling back to LLM", domain)

        # Fall back to LLM extraction
        jobs, selectors = self.extractor.extract_jobs_from_html(html, page_url, keyword)

        # Cache the selectors for future use
        if selectors:
            await save_cached_selectors(session, domain, page_sig, selectors)
            logger.info("Cached new selectors for %s", domain)

        return jobs
