from __future__ import annotations

import logging
from urllib.parse import urlparse

import httpx
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from crawler.anti_throttle.circuit import CircuitBreaker
from crawler.anti_throttle.delays import AdaptiveDelay
from crawler.anti_throttle.fingerprint import random_headers, random_user_agent, random_viewport
from crawler.anti_throttle.proxies import ProxyPool
from crawler.settings import settings

logger = logging.getLogger(__name__)


class StealthFetcher:
    """Fetches web pages with anti-throttling measures.

    Uses httpx for static pages and Playwright with stealth for JS-rendered pages.
    """

    def __init__(self, proxy_pool: ProxyPool | None = None):
        self.delay = AdaptiveDelay(
            min_delay=settings.min_delay,
            max_delay=settings.max_delay,
        )
        self.circuit = CircuitBreaker(
            threshold=settings.circuit_breaker_threshold,
            cooldown=settings.circuit_breaker_cooldown,
        )
        self.proxy_pool = proxy_pool or ProxyPool(
            [settings.proxy_url] if settings.proxy_url else []
        )

    @staticmethod
    def _get_domain(url: str) -> str:
        return urlparse(url).netloc

    async def fetch_static(self, url: str) -> str | None:
        """Fetch a page using httpx (no JS rendering)."""
        domain = self._get_domain(url)

        if self.circuit.is_open(domain):
            logger.warning("Circuit open for %s, skipping", domain)
            return None

        await self.delay.wait(domain)

        headers = random_headers()
        proxy = self.proxy_pool.get_random()

        try:
            async with httpx.AsyncClient(
                headers=headers,
                proxy=proxy,
                follow_redirects=True,
                timeout=30.0,
            ) as client:
                response = await client.get(url)

                if response.status_code in (429, 503):
                    self.delay.report_error(domain, response.status_code)
                    self.circuit.record_failure(domain)
                    logger.warning("HTTP %d from %s", response.status_code, domain)
                    return None

                if response.status_code >= 400:
                    self.circuit.record_failure(domain)
                    logger.warning("HTTP %d from %s for %s", response.status_code, domain, url)
                    return None

                self.delay.report_success(domain)
                self.circuit.record_success(domain)
                return response.text

        except Exception:
            self.circuit.record_failure(domain)
            self.delay.report_error(domain)
            logger.exception("Failed to fetch %s", url)
            return None

    async def fetch_js(self, url: str) -> str | None:
        """Fetch a page using Playwright with stealth (for JS-heavy sites)."""
        domain = self._get_domain(url)

        if self.circuit.is_open(domain):
            logger.warning("Circuit open for %s, skipping", domain)
            return None

        await self.delay.wait(domain)

        proxy_url = self.proxy_pool.get_random()
        proxy_config = None
        if proxy_url:
            proxy_config = {"server": proxy_url}

        viewport = random_viewport()
        ua = random_user_agent()

        try:
            stealth = Stealth()
            async with stealth.use_async(async_playwright()) as p:
                browser = await p.chromium.launch(
                    headless=True,
                    proxy=proxy_config,
                )
                context = await browser.new_context(
                    user_agent=ua,
                    viewport=viewport,
                    locale="en-US",
                )
                page = await context.new_page()

                response = await page.goto(url, wait_until="networkidle", timeout=30000)

                if response and response.status in (429, 503):
                    self.delay.report_error(domain, response.status)
                    self.circuit.record_failure(domain)
                    logger.warning("Playwright HTTP %d from %s", response.status, domain)
                    await browser.close()
                    return None

                # Wait a bit for dynamic content to load
                await page.wait_for_timeout(2000)

                html = await page.content()
                await browser.close()

                self.delay.report_success(domain)
                self.circuit.record_success(domain)
                return html

        except Exception:
            self.circuit.record_failure(domain)
            self.delay.report_error(domain)
            logger.exception("Playwright failed for %s", url)
            return None

    async def fetch(self, url: str, js_render: bool = False) -> str | None:
        """Fetch a URL, choosing static or JS rendering based on config."""
        if js_render:
            return await self.fetch_js(url)
        return await self.fetch_static(url)
