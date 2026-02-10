from __future__ import annotations

import asyncio
from time import monotonic

import pytest

from crawler.anti_throttle.circuit import CircuitBreaker
from crawler.anti_throttle.delays import AdaptiveDelay
from crawler.anti_throttle.fingerprint import random_headers, random_user_agent, random_viewport
from crawler.anti_throttle.proxies import ProxyPool


class TestAdaptiveDelay:
    @pytest.mark.asyncio
    async def test_wait_introduces_delay(self):
        delay = AdaptiveDelay(min_delay=0.1, max_delay=0.2)
        start = monotonic()
        await delay.wait("example.com")
        await delay.wait("example.com")
        elapsed = monotonic() - start
        assert elapsed >= 0.1

    def test_backoff_increases_on_rate_limit(self):
        delay = AdaptiveDelay()
        assert delay._backoff_factor["test.com"] == 1.0
        delay.report_error("test.com", status_code=429)
        assert delay._backoff_factor["test.com"] == 2.0
        delay.report_error("test.com", status_code=429)
        assert delay._backoff_factor["test.com"] == 4.0

    def test_backoff_resets_on_success(self):
        delay = AdaptiveDelay()
        delay.report_error("test.com", status_code=429)
        delay.report_error("test.com", status_code=429)
        delay.report_success("test.com")
        assert delay._backoff_factor["test.com"] < 4.0


class TestCircuitBreaker:
    def test_starts_closed(self):
        cb = CircuitBreaker(threshold=3, cooldown=1.0)
        assert not cb.is_open("test.com")
        assert cb.get_status("test.com") == "closed"

    def test_opens_after_threshold_failures(self):
        cb = CircuitBreaker(threshold=3, cooldown=60.0)
        cb.record_failure("test.com")
        cb.record_failure("test.com")
        assert not cb.is_open("test.com")
        cb.record_failure("test.com")
        assert cb.is_open("test.com")
        assert cb.get_status("test.com") == "open"

    def test_resets_on_success(self):
        cb = CircuitBreaker(threshold=3, cooldown=60.0)
        cb.record_failure("test.com")
        cb.record_failure("test.com")
        cb.record_success("test.com")
        cb.record_failure("test.com")
        assert not cb.is_open("test.com")


class TestProxyPool:
    def test_empty_pool(self):
        pool = ProxyPool()
        assert not pool.enabled
        assert pool.get_random() is None
        assert pool.get_next() is None

    def test_round_robin(self):
        pool = ProxyPool(["http://p1", "http://p2", "http://p3"])
        assert pool.enabled
        assert pool.get_next() == "http://p1"
        assert pool.get_next() == "http://p2"
        assert pool.get_next() == "http://p3"
        assert pool.get_next() == "http://p1"

    def test_remove_proxy(self):
        pool = ProxyPool(["http://p1", "http://p2"])
        pool.remove_proxy("http://p1")
        assert pool.get_random() == "http://p2"


class TestFingerprint:
    def test_random_user_agent_returns_string(self):
        ua = random_user_agent()
        assert isinstance(ua, str)
        assert "Mozilla" in ua

    def test_random_headers_has_required_keys(self):
        headers = random_headers()
        assert "User-Agent" in headers
        assert "Accept" in headers

    def test_random_viewport_has_dimensions(self):
        vp = random_viewport()
        assert "width" in vp
        assert "height" in vp
        assert vp["width"] > 0
        assert vp["height"] > 0
