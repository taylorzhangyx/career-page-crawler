from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _make_jobs_df():
    """Create a mock DataFrame mimicking JobSpy output."""
    return pd.DataFrame([
        {
            "site": "indeed",
            "job_url": "https://indeed.com/job/12345",
            "title": "AI Engineer",
            "company_name": "TechCorp",
            "location": "San Francisco, CA",
            "description": "Build AI systems",
            "min_amount": 150000,
            "max_amount": 200000,
            "interval": "yearly",
            "date_posted": pd.Timestamp("2025-01-15"),
        },
        {
            "site": "linkedin",
            "job_url": "https://linkedin.com/jobs/67890",
            "title": "ML Engineer",
            "company_name": "DataCo",
            "location": "Remote",
            "description": "ML pipelines",
            "min_amount": None,
            "max_amount": None,
            "interval": None,
            "date_posted": None,
        },
    ])


@patch("crawler.jobspy_adapter.scrape_jobs")
def test_search_job_boards_returns_normalized_results(mock_scrape):
    from crawler.jobspy_adapter import search_job_boards

    mock_scrape.return_value = _make_jobs_df()

    results = search_job_boards(
        keyword="AI engineer",
        location="San Francisco, CA",
        sites=["indeed", "linkedin"],
    )

    assert len(results) == 2

    # Check first job
    job1 = results[0]
    assert job1["title"] == "AI Engineer"
    assert job1["company"] == "TechCorp"
    assert job1["source_site"] == "indeed"
    assert job1["search_keyword"] == "AI engineer"
    assert "$150,000 - $200,000" in job1["salary_range"]
    assert job1["posted_date"] is not None

    # Check second job (no salary)
    job2 = results[1]
    assert job2["title"] == "ML Engineer"
    assert job2["salary_range"] is None
    assert job2["posted_date"] is None


@patch("crawler.jobspy_adapter.scrape_jobs")
def test_search_job_boards_empty_result(mock_scrape):
    from crawler.jobspy_adapter import search_job_boards

    mock_scrape.return_value = pd.DataFrame()

    results = search_job_boards(
        keyword="nonexistent job",
        location="Nowhere",
        sites=["indeed"],
    )

    assert results == []


@patch("crawler.jobspy_adapter.scrape_jobs")
def test_search_job_boards_handles_exception(mock_scrape):
    from crawler.jobspy_adapter import search_job_boards

    mock_scrape.side_effect = Exception("Network error")

    results = search_job_boards(
        keyword="AI engineer",
        location="SF",
        sites=["indeed"],
    )

    assert results == []


def test_search_job_boards_unknown_site():
    from crawler.jobspy_adapter import search_job_boards

    results = search_job_boards(
        keyword="AI",
        location="SF",
        sites=["unknown_board"],
    )

    assert results == []
