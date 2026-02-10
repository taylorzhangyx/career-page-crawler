from __future__ import annotations

import logging
import random

logger = logging.getLogger(__name__)


class ProxyPool:
    """Manages a pool of proxy URLs for rotation."""

    def __init__(self, proxy_urls: list[str] | None = None):
        self._proxies = proxy_urls or []
        self._index = 0

    @property
    def enabled(self) -> bool:
        return len(self._proxies) > 0

    def get_random(self) -> str | None:
        """Get a random proxy from the pool."""
        if not self._proxies:
            return None
        return random.choice(self._proxies)

    def get_next(self) -> str | None:
        """Get the next proxy in round-robin order."""
        if not self._proxies:
            return None
        proxy = self._proxies[self._index % len(self._proxies)]
        self._index += 1
        return proxy

    def add_proxy(self, proxy_url: str) -> None:
        """Add a proxy to the pool."""
        if proxy_url not in self._proxies:
            self._proxies.append(proxy_url)

    def remove_proxy(self, proxy_url: str) -> None:
        """Remove a failed proxy from the pool."""
        if proxy_url in self._proxies:
            self._proxies.remove(proxy_url)
            logger.warning("Removed proxy %s from pool (%d remaining)", proxy_url, len(self._proxies))
