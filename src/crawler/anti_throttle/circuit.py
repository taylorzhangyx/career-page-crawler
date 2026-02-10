from __future__ import annotations

import logging
from collections import defaultdict
from time import monotonic

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """Per-domain circuit breaker that pauses requests after consecutive failures."""

    def __init__(self, threshold: int = 5, cooldown: float = 300.0):
        self.threshold = threshold
        self.cooldown = cooldown
        self._failure_counts: dict[str, int] = defaultdict(int)
        self._open_since: dict[str, float] = {}

    def is_open(self, domain: str) -> bool:
        """Check if the circuit is open (domain is paused)."""
        if domain not in self._open_since:
            return False

        elapsed = monotonic() - self._open_since[domain]
        if elapsed >= self.cooldown:
            # Cooldown expired, half-open: allow a retry
            logger.info("Circuit breaker half-open for %s after %.0fs cooldown", domain, elapsed)
            del self._open_since[domain]
            self._failure_counts[domain] = 0
            return False

        return True

    def record_success(self, domain: str) -> None:
        """Record a successful request, resetting failure count."""
        self._failure_counts[domain] = 0
        if domain in self._open_since:
            del self._open_since[domain]
            logger.info("Circuit breaker closed for %s (recovered)", domain)

    def record_failure(self, domain: str) -> None:
        """Record a failed request. Opens circuit if threshold is reached."""
        self._failure_counts[domain] += 1
        if self._failure_counts[domain] >= self.threshold:
            self._open_since[domain] = monotonic()
            logger.warning(
                "Circuit breaker OPEN for %s (%d consecutive failures, cooldown %.0fs)",
                domain,
                self._failure_counts[domain],
                self.cooldown,
            )

    def get_status(self, domain: str) -> str:
        """Get circuit status for a domain."""
        if domain in self._open_since:
            elapsed = monotonic() - self._open_since[domain]
            if elapsed >= self.cooldown:
                return "half-open"
            return "open"
        return "closed"
