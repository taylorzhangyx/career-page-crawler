from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIGS_DIR = PROJECT_ROOT / "configs"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql+asyncpg://user:password@localhost:5432/career_crawler"
    llm_model_key: str = "gpt4omini"
    proxy_url: Optional[str] = None
    log_level: str = "INFO"

    # Anti-throttle defaults
    min_delay: float = Field(default=2.0, description="Minimum delay between requests (seconds)")
    max_delay: float = Field(default=7.0, description="Maximum delay between requests (seconds)")
    circuit_breaker_threshold: int = Field(default=5, description="Consecutive failures before pausing a domain")
    circuit_breaker_cooldown: float = Field(default=300.0, description="Cooldown in seconds after circuit break")
    max_concurrent_per_domain: int = Field(default=1, description="Max concurrent requests per domain")


def load_search_config(path: Path | None = None) -> dict:
    """Load the search configuration YAML file."""
    config_path = path or CONFIGS_DIR / "search.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def load_user_agents(path: Path | None = None) -> list[str]:
    """Load user agent strings from the UA pool file."""
    ua_path = path or CONFIGS_DIR / "user_agents.txt"
    with open(ua_path) as f:
        return [line.strip() for line in f if line.strip()]


settings = Settings()
