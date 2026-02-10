from __future__ import annotations

import random
from pathlib import Path

from crawler.settings import CONFIGS_DIR


_user_agents: list[str] | None = None


def _load_user_agents() -> list[str]:
    """Load user agents from file, caching the result."""
    global _user_agents
    if _user_agents is None:
        ua_path = CONFIGS_DIR / "user_agents.txt"
        with open(ua_path) as f:
            _user_agents = [line.strip() for line in f if line.strip()]
    return _user_agents


def random_user_agent() -> str:
    """Return a random user agent string."""
    agents = _load_user_agents()
    return random.choice(agents)


def random_headers() -> dict[str, str]:
    """Generate randomized but realistic HTTP headers."""
    ua = random_user_agent()
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice([
            "en-US,en;q=0.9",
            "en-GB,en;q=0.9",
            "en-US,en;q=0.5",
            "en;q=0.9",
        ]),
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }

    # Randomize header order
    items = list(headers.items())
    random.shuffle(items)
    return dict(items)


def random_viewport() -> dict[str, int]:
    """Return a random realistic browser viewport size."""
    viewports = [
        {"width": 1920, "height": 1080},
        {"width": 1366, "height": 768},
        {"width": 1440, "height": 900},
        {"width": 1536, "height": 864},
        {"width": 1280, "height": 720},
        {"width": 1600, "height": 900},
        {"width": 2560, "height": 1440},
    ]
    return random.choice(viewports)
