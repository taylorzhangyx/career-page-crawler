from __future__ import annotations

import pytest

from crawler.generic.extractor import clean_html, compute_page_signature, extract_with_selectors


SAMPLE_HTML = """
<html>
<head><title>Jobs</title><script>console.log('x')</script><style>.hidden{display:none}</style></head>
<body>
<div class="job-list">
    <div class="job-card">
        <h3 class="job-title"><a href="/jobs/123">AI Engineer</a></h3>
        <span class="company">TechCorp</span>
        <span class="location">San Francisco, CA</span>
        <span class="salary">$150k - $200k</span>
    </div>
    <div class="job-card">
        <h3 class="job-title"><a href="/jobs/456">ML Engineer</a></h3>
        <span class="company">DataCo</span>
        <span class="location">Remote</span>
    </div>
</div>
</body>
</html>
"""


class TestCleanHtml:
    def test_removes_scripts_and_styles(self):
        cleaned = clean_html(SAMPLE_HTML)
        assert "console.log" not in cleaned
        assert "<style>" not in cleaned
        assert "<script>" not in cleaned

    def test_preserves_content(self):
        cleaned = clean_html(SAMPLE_HTML)
        assert "AI Engineer" in cleaned
        assert "TechCorp" in cleaned

    def test_truncation(self):
        long_html = "<body>" + "x" * 100000 + "</body>"
        cleaned = clean_html(long_html, max_length=1000)
        assert len(cleaned) <= 1020  # allow for truncation suffix
        assert "TRUNCATED" in cleaned


class TestPageSignature:
    def test_same_structure_same_signature(self):
        html1 = '<div class="a"><span class="b">text1</span></div>'
        html2 = '<div class="a"><span class="b">text2</span></div>'
        assert compute_page_signature(html1) == compute_page_signature(html2)

    def test_different_structure_different_signature(self):
        html1 = '<div class="a"><span class="b">text</span></div>'
        html2 = '<div class="x"><p class="y">text</p></div>'
        assert compute_page_signature(html1) != compute_page_signature(html2)


class TestExtractWithSelectors:
    def test_extracts_jobs_with_valid_selectors(self):
        selectors = {
            "job_list_selector": ".job-card",
            "title_selector": ".job-title",
            "company_selector": ".company",
            "location_selector": ".location",
            "url_selector": ".job-title a",
            "salary_selector": ".salary",
        }

        jobs = extract_with_selectors(SAMPLE_HTML, selectors, "https://example.com/careers")

        assert len(jobs) == 2
        assert jobs[0]["title"] == "AI Engineer"
        assert jobs[0]["company"] == "TechCorp"
        assert jobs[0]["location"] == "San Francisco, CA"
        assert jobs[0]["salary_range"] == "$150k - $200k"
        assert jobs[0]["source_url"] == "https://example.com/jobs/123"

        assert jobs[1]["title"] == "ML Engineer"
        assert jobs[1]["salary_range"] is None

    def test_returns_empty_with_wrong_selectors(self):
        selectors = {
            "job_list_selector": ".nonexistent",
            "title_selector": ".nope",
        }

        jobs = extract_with_selectors(SAMPLE_HTML, selectors, "https://example.com")
        assert jobs == []

    def test_skips_jobs_without_title(self):
        html = """
        <div class="job-card">
            <span class="company">Corp</span>
        </div>
        """
        selectors = {
            "job_list_selector": ".job-card",
            "title_selector": ".job-title",
            "company_selector": ".company",
        }

        jobs = extract_with_selectors(html, selectors, "https://example.com")
        assert jobs == []
