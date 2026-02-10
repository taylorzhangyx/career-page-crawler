from __future__ import annotations

import asyncio
import logging
import random
from collections import defaultdict
from time import monotonic

logger = logging.getLogger(__name__)


class AdaptiveDelay:
    """Per-domain adaptive delay with exponential backoff on errors."""

    def __init__(self, min_delay: float = 2.0, max_delay: float = 7.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._last_request: dict[str, float] = defaultdict(float)
        self._backoff_factor: dict[str, float] = defaultdict(lambda: 1.0)

    def _jittered_delay(self, domain: str) -> float:
        """Calculate a random delay with jitter, scaled by backoff factor."""
        base = random.uniform(self.min_delay, self.max_delay)
        return base * self._backoff_factor[domain]

    async def wait(self, domain: str) -> None:
        """Wait an appropriate amount of time before making a request to the domain."""
        now = monotonic()
        elapsed = now - self._last_request[domain]
        delay = self._jittered_delay(domain)

        if elapsed < delay:
            wait_time = delay - elapsed
            logger.debug("Throttle: waiting %.1fs for %s", wait_time, domain)
            await asyncio.sleep(wait_time)

        self._last_request[domain] = monotonic()

    def report_success(self, domain: str) -> None:
        """Reset backoff on successful request."""
        self._backoff_factor[domain] = max(1.0, self._backoff_factor[domain] * 0.5)

    def report_error(self, domain: str, status_code: int | None = None) -> None:
        """Increase backoff on error (especially 429/503)."""
        if status_code in (429, 503):
            self._backoff_factor[domain] = min(10.0, self._backoff_factor[domain] * 2.0)
            logger.warning("Rate limited on %s (HTTP %s), backoff factor: %.1f", domain, status_code, self._backoff_factor[domain])
        else:
            self._backoff_factor[domain] = min(5.0, self._backoff_factor[domain] * 1.5)
