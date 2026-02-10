from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from langchain.messages import HumanMessage, SystemMessage

from crawler.db.queries import compute_content_hash

logger = logging.getLogger(__name__)

EXTRACTION_SYSTEM_PROMPT = """You are a web scraping assistant that extracts job posting data from HTML content.

Given the HTML of a career/jobs page, extract ALL job postings visible on the page.

For each job posting, extract:
- title: Job title
- company: Company name
- location: Job location (city, state, remote, etc.)
- salary_range: Salary information if available (null if not shown)
- description: Brief job description or snippet
- job_url: Direct link to the job posting (full URL)
- posted_date: Date posted if available (YYYY-MM-DD format, null if not shown)

Also identify the CSS selectors that can be used to extract these fields for future crawls:
- job_list_selector: CSS selector for the list of job cards/items
- title_selector: CSS selector for job title within a card
- company_selector: CSS selector for company name within a card
- location_selector: CSS selector for location within a card
- url_selector: CSS selector for the link element within a card

Return your response as a JSON object with two keys:
1. "jobs": array of job objects
2. "selectors": object with the CSS selector mappings listed above

If no jobs are found, return {"jobs": [], "selectors": null}.
Respond ONLY with valid JSON, no markdown formatting."""

SELECTOR_EXTRACTION_PROMPT = """Given this HTML content from a career page, identify CSS selectors to extract job listings.

Return a JSON object with these keys:
- job_list_selector: CSS selector for the container/list of job cards
- title_selector: CSS selector for the job title within a card
- company_selector: CSS selector for company name within a card
- location_selector: CSS selector for location within a card
- url_selector: CSS selector for the link/anchor within a card
- salary_selector: CSS selector for salary info within a card (null if not applicable)

Respond ONLY with valid JSON."""


def _create_llm_model(model_key: str = "gpt4omini"):
    """Create a ChatGenAIGatewayModel instance using the internal GenAI Gateway."""
    from bkng.ml.agentic.lc import ChatGenAIGatewayModel
    from bkng.ml.rs.client import GenAIClient, Service, ServiceInfo
    from bkng.mlregistry.client.types import Application, Asset, AssetType

    MODEL_MAPPINGS = {
        "gpt4o": Asset(asset_type=AssetType.STATIC_MODEL, name="gpt-4o"),
        "claude3.5": Asset(asset_type=AssetType.STATIC_MODEL, name="claude_3_5_sonnet"),
        "gpt4turbo": Asset(asset_type=AssetType.STATIC_MODEL, name="gpt-4-turbo"),
        "gpt4omini": Asset(asset_type=AssetType.STATIC_MODEL, name="gpt-4o-mini"),
        "gemini-2_0-flash": Asset(asset_type=AssetType.STATIC_MODEL, name="gemini-2_0-flash"),
        "claude3_7_sonnet": Asset(asset_type=AssetType.STATIC_MODEL, name="claude_3_7_sonnet"),
        "gpt4_1_mini": Asset(asset_type=AssetType.STATIC_MODEL, name="gpt-4_1-mini"),
        "gpt4_1": Asset(asset_type=AssetType.STATIC_MODEL, name="gpt-4_1"),
        "gemini_2_5_flash": Asset(asset_type=AssetType.STATIC_MODEL, name="gemini-2_5-flash"),
    }

    model_asset = MODEL_MAPPINGS.get(model_key)
    if not model_asset:
        logger.warning("Unknown model key %r, falling back to gpt4omini", model_key)
        model_asset = MODEL_MAPPINGS["gpt4omini"]

    genai_client = GenAIClient(
        service_info=ServiceInfo(Service.GEN_AI),
        timeout_s=70.5,
        use_json=True,
    )
    application = Application(name="career-page-crawler")

    return ChatGenAIGatewayModel(
        client=genai_client,
        application=application,
        model_asset=model_asset,
    )


def clean_html(raw_html: str, max_length: int = 50000) -> str:
    """Clean HTML to reduce token usage: remove scripts, styles, and compress whitespace."""
    soup = BeautifulSoup(raw_html, "lxml")

    # Remove non-content elements
    for tag in soup(["script", "style", "noscript", "svg", "path", "meta", "link", "head"]):
        tag.decompose()

    # Remove hidden elements
    for tag in soup.find_all(attrs={"style": re.compile(r"display\s*:\s*none")}):
        tag.decompose()

    # Get cleaned HTML
    cleaned = str(soup.body) if soup.body else str(soup)

    # Compress whitespace
    cleaned = re.sub(r"\s+", " ", cleaned)

    # Truncate if too long
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length] + "... [TRUNCATED]"

    return cleaned


def compute_page_signature(html: str) -> str:
    """Compute a structural signature of the page for cache lookup.

    Uses the DOM structure (tag hierarchy) rather than content to identify
    pages with the same layout.
    """
    soup = BeautifulSoup(html, "lxml")

    # Extract structural elements: tag names and class attributes
    structure_parts = []
    for tag in soup.find_all(True, limit=200):
        classes = ".".join(sorted(tag.get("class", [])))
        structure_parts.append(f"{tag.name}:{classes}")

    signature = "|".join(structure_parts)
    return hashlib.md5(signature.encode()).hexdigest()


def extract_with_selectors(html: str, selectors: dict, base_url: str) -> list[dict[str, Any]]:
    """Extract job postings using cached CSS selectors."""
    soup = BeautifulSoup(html, "lxml")
    domain = urlparse(base_url).netloc

    job_list_sel = selectors.get("job_list_selector", "")
    if not job_list_sel:
        return []

    try:
        job_cards = soup.select(job_list_sel)
    except Exception:
        return []
    if not job_cards:
        return []

    def _safe_select_one(el, selector: str | None):
        if not selector:
            return None
        try:
            return el.select_one(selector)
        except Exception:
            return None

    jobs = []
    for card in job_cards:
        title_el = _safe_select_one(card, selectors.get("title_selector"))
        company_el = _safe_select_one(card, selectors.get("company_selector"))
        location_el = _safe_select_one(card, selectors.get("location_selector"))
        url_el = _safe_select_one(card, selectors.get("url_selector"))
        salary_el = _safe_select_one(card, selectors.get("salary_selector"))

        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            continue

        company = company_el.get_text(strip=True) if company_el else domain
        location = location_el.get_text(strip=True) if location_el else None
        salary = salary_el.get_text(strip=True) if salary_el else None

        # Resolve job URL
        job_url = ""
        if url_el:
            href = url_el.get("href", "")
            if href.startswith("http"):
                job_url = href
            elif href.startswith("/"):
                parsed = urlparse(base_url)
                job_url = f"{parsed.scheme}://{parsed.netloc}{href}"

        jobs.append({
            "title": title,
            "company": company,
            "location": location,
            "salary_range": salary,
            "description": "",
            "source_url": job_url,
            "posted_date": None,
        })

    return jobs


class LLMExtractor:
    """Extracts job postings from HTML using the internal GenAI Gateway LLM."""

    def __init__(self, model_key: str = "gpt4omini"):
        self.model_key = model_key
        self._model = None

    @property
    def model(self):
        """Lazy-initialize the LLM model."""
        if self._model is None:
            self._model = _create_llm_model(self.model_key)
        return self._model

    def extract_jobs_from_html(
        self, html: str, page_url: str, keyword: str
    ) -> tuple[list[dict[str, Any]], dict | None]:
        """
        Use LLM to extract job postings and CSS selectors from HTML.

        Returns:
            Tuple of (list of job dicts, selectors dict or None)
        """
        cleaned = clean_html(html)

        messages = [
            SystemMessage(content=EXTRACTION_SYSTEM_PROMPT),
            HumanMessage(content=f"URL: {page_url}\nSearch keyword: {keyword}\n\nHTML:\n{cleaned}"),
        ]

        try:
            response = self.model.invoke(messages)
            content = response.content

            # Parse JSON response
            data = json.loads(content)
            jobs = data.get("jobs", [])
            selectors = data.get("selectors")

            # Normalize jobs
            normalized = []
            for job in jobs:
                source_url = job.get("job_url", job.get("source_url", ""))
                if not source_url:
                    continue

                normalized.append({
                    "source_site": urlparse(page_url).netloc,
                    "source_url": source_url,
                    "search_keyword": keyword,
                    "title": job.get("title", ""),
                    "company": job.get("company", ""),
                    "location": job.get("location"),
                    "salary_range": job.get("salary_range"),
                    "description": job.get("description", ""),
                    "posted_date": job.get("posted_date"),
                })

            logger.info("LLM extracted %d jobs from %s", len(normalized), page_url)
            return normalized, selectors

        except json.JSONDecodeError:
            logger.error("LLM returned invalid JSON for %s", page_url)
            return [], None
        except Exception:
            logger.exception("LLM extraction failed for %s", page_url)
            return [], None

    def extract_selectors_only(self, html: str) -> dict | None:
        """Use LLM to identify CSS selectors for a career page layout."""
        cleaned = clean_html(html, max_length=30000)

        messages = [
            SystemMessage(content=SELECTOR_EXTRACTION_PROMPT),
            HumanMessage(content=f"HTML:\n{cleaned}"),
        ]

        try:
            response = self.model.invoke(messages)
            return json.loads(response.content)
        except Exception:
            logger.exception("LLM selector extraction failed")
            return None
