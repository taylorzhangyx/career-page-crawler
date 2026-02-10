from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from crawler.db.models import CrawlRun, JobPosting, LLMPatternCache

logger = logging.getLogger(__name__)


def compute_content_hash(description: str | None, title: str = "", company: str = "") -> str:
    """Compute SHA-256 hash of job content for deduplication."""
    content = f"{title}|{company}|{description or ''}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


async def upsert_job_posting(session: AsyncSession, job_data: dict[str, Any]) -> str:
    """Insert or update a job posting. Returns 'new', 'updated', or 'unchanged'."""
    content_hash = compute_content_hash(
        job_data.get("description"),
        job_data.get("title", ""),
        job_data.get("company", ""),
    )
    job_data["content_hash"] = content_hash

    # Try insert with ON CONFLICT
    stmt = pg_insert(JobPosting).values(**job_data)
    stmt = stmt.on_conflict_do_update(
        index_elements=["source_url"],
        set_={
            "title": stmt.excluded.title,
            "company": stmt.excluded.company,
            "location": stmt.excluded.location,
            "salary_range": stmt.excluded.salary_range,
            "description": stmt.excluded.description,
            "posted_date": stmt.excluded.posted_date,
            "content_hash": stmt.excluded.content_hash,
            "search_keyword": stmt.excluded.search_keyword,
            "updated_at": datetime.now(timezone.utc),
        },
        where=(JobPosting.content_hash != stmt.excluded.content_hash),
    )

    result = await session.execute(stmt)
    await session.commit()

    if result.rowcount == 0:
        return "unchanged"

    # Check if this was a new row or an update
    existing = await session.execute(
        select(JobPosting).where(JobPosting.source_url == job_data["source_url"])
    )
    row = existing.scalar_one_or_none()
    if row and row.crawled_at == row.updated_at:
        return "new"
    return "updated"


async def upsert_job_postings_batch(
    session: AsyncSession, jobs: list[dict[str, Any]]
) -> dict[str, int]:
    """Batch upsert job postings. Returns counts of new/updated/unchanged."""
    counts = {"new": 0, "updated": 0, "unchanged": 0}
    for job_data in jobs:
        try:
            result = await upsert_job_posting(session, job_data)
            counts[result] += 1
        except Exception:
            logger.exception("Failed to upsert job: %s", job_data.get("source_url", "unknown"))
            counts.setdefault("error", 0)
            counts["error"] = counts.get("error", 0) + 1
    return counts


async def create_crawl_run(
    session: AsyncSession, keyword: str, source: str
) -> CrawlRun:
    """Create a new crawl run record."""
    run = CrawlRun(keyword=keyword, source=source, status="running")
    session.add(run)
    await session.commit()
    await session.refresh(run)
    return run


async def finish_crawl_run(
    session: AsyncSession,
    run_id: UUID,
    status: str = "completed",
    new_count: int = 0,
    updated_count: int = 0,
    error_count: int = 0,
    error_message: str | None = None,
) -> None:
    """Mark a crawl run as finished."""
    stmt = (
        update(CrawlRun)
        .where(CrawlRun.id == run_id)
        .values(
            status=status,
            finished_at=datetime.now(timezone.utc),
            new_count=new_count,
            updated_count=updated_count,
            error_count=error_count,
            error_message=error_message,
        )
    )
    await session.execute(stmt)
    await session.commit()


async def get_cached_selectors(
    session: AsyncSession, domain: str, page_signature: str
) -> dict | None:
    """Retrieve cached CSS selectors for a domain + page signature."""
    stmt = select(LLMPatternCache).where(
        LLMPatternCache.domain == domain,
        LLMPatternCache.page_signature == page_signature,
    )
    result = await session.execute(stmt)
    cache_entry = result.scalar_one_or_none()
    if cache_entry:
        return cache_entry.selectors
    return None


async def save_cached_selectors(
    session: AsyncSession, domain: str, page_signature: str, selectors: dict
) -> None:
    """Save or update cached CSS selectors for a domain."""
    stmt = pg_insert(LLMPatternCache).values(
        domain=domain,
        page_signature=page_signature,
        selectors=selectors,
        verified_at=datetime.now(timezone.utc),
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_domain_signature",
        set_={
            "selectors": stmt.excluded.selectors,
            "verified_at": stmt.excluded.verified_at,
        },
    )
    await session.execute(stmt)
    await session.commit()
