from __future__ import annotations

import logging
from typing import Any

from jobspy import scrape_jobs

from crawler.db.queries import compute_content_hash

logger = logging.getLogger(__name__)

# Map our config names to JobSpy site_name values
SITE_NAME_MAP = {
    "indeed": "indeed",
    "linkedin": "linkedin",
    "glassdoor": "glassdoor",
    "zip_recruiter": "zip_recruiter",
}


def search_job_boards(
    keyword: str,
    location: str,
    sites: list[str],
    results_wanted: int = 50,
    hours_old: int | None = 24,
    country: str = "USA",
) -> list[dict[str, Any]]:
    """
    Search job boards using JobSpy and return normalized job posting dicts.

    Args:
        keyword: Search keyword (e.g. "AI engineer")
        location: Location string (e.g. "San Francisco, CA")
        sites: List of site names to search (e.g. ["indeed", "linkedin"])
        results_wanted: Max results per site
        hours_old: Only return jobs posted within this many hours
        country: Country for the search
    """
    # Map site names
    jobspy_sites = []
    for site in sites:
        mapped = SITE_NAME_MAP.get(site)
        if mapped:
            jobspy_sites.append(mapped)
        else:
            logger.warning("Unknown job board site: %s, skipping", site)

    if not jobspy_sites:
        logger.warning("No valid job board sites to search")
        return []

    logger.info(
        "JobSpy search: keyword=%r, location=%r, sites=%s, results_wanted=%d",
        keyword, location, jobspy_sites, results_wanted,
    )

    try:
        jobs_df = scrape_jobs(
            site_name=jobspy_sites,
            search_term=keyword,
            location=location,
            results_wanted=results_wanted,
            hours_old=hours_old,
            country_indeed=country,
        )
    except Exception:
        logger.exception("JobSpy scrape failed for keyword=%r, location=%r", keyword, location)
        return []

    if jobs_df is None or jobs_df.empty:
        logger.info("No results from JobSpy for keyword=%r, location=%r", keyword, location)
        return []

    # Normalize DataFrame rows to our JobPosting schema
    normalized = []
    for _, row in jobs_df.iterrows():
        job_url = str(row.get("job_url", ""))
        if not job_url:
            continue

        title = str(row.get("title", ""))
        company = str(row.get("company_name", row.get("company", "")))
        description = str(row.get("description", ""))

        # Build salary range string from components
        import pandas as pd

        salary_parts = []
        min_amount = row.get("min_amount")
        max_amount = row.get("max_amount")
        if pd.notna(min_amount) and pd.notna(max_amount):
            salary_parts.append(f"${min_amount:,.0f} - ${max_amount:,.0f}")
        elif pd.notna(min_amount):
            salary_parts.append(f"${min_amount:,.0f}+")
        interval = row.get("interval")
        if pd.notna(interval) and salary_parts:
            salary_parts.append(f"({interval})")
        salary_range = " ".join(salary_parts) if salary_parts else None

        # Parse posted date
        posted_date = None
        date_posted = row.get("date_posted")
        if pd.notna(date_posted):
            try:
                if isinstance(date_posted, pd.Timestamp):
                    posted_date = date_posted.date()
                elif hasattr(date_posted, "date"):
                    posted_date = date_posted.date()
            except Exception:
                pass

        job_data = {
            "source_site": str(row.get("site", "unknown")),
            "source_url": job_url,
            "search_keyword": keyword,
            "title": title,
            "company": company,
            "location": str(row.get("location", "")),
            "salary_range": salary_range,
            "description": description,
            "posted_date": posted_date,
        }
        normalized.append(job_data)

    logger.info("JobSpy returned %d normalized results for keyword=%r", len(normalized), keyword)
    return normalized
