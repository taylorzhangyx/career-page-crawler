# Career Page Crawler

A keyword-driven Python job posting crawler that combines **JobSpy** for major job boards (Indeed, LinkedIn, Glassdoor, ZipRecruiter) with a **custom LLM extractor** for company career pages. Includes robust anti-throttling, PostgreSQL storage, and cron scheduling.

## Features

- **Keyword-driven search**: Configure search keywords (e.g., "AI engineer", "agent engineer") and locations
- **Job board scraping**: Indeed, LinkedIn, Glassdoor, ZipRecruiter via [JobSpy](https://github.com/Bunsly/JobSpy)
- **Company career pages**: LLM-powered extraction from any career page with CSS selector caching
- **Anti-throttling**: Adaptive delays, UA rotation, Playwright stealth, proxy rotation, circuit breaker
- **Deduplication**: By URL + content hash to detect new and updated postings
- **Scheduled runs**: APScheduler with configurable cron expressions
- **PostgreSQL storage**: Full crawl history and pattern caching

## Quick Start

### 1. Prerequisites

- Python 3.11+
- PostgreSQL
- (Optional) Internal `bkng-ml` packages for LLM extraction

### 2. Install

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e .

# For internal LLM support (Booking only)
pip install -e '.[bkng]' --extra-index-url https://jfrog.booking.com/artifactory/api/pypi/pypi/simple

# Install Playwright browsers
playwright install chromium
```

### 3. Configure

```bash
# Copy and edit environment variables
cp .env.example .env
# Edit .env with your DATABASE_URL, LLM_MODEL_KEY, etc.
```

Edit `configs/search.yaml` to configure:
- **keywords**: Job search terms
- **locations**: Target locations
- **job_boards**: Which boards to search via JobSpy
- **company_pages**: Direct career page URLs with `{keyword}` and `{location}` placeholders
- **schedule**: Cron expression for automated runs

### 4. Database Setup

```bash
# Run migrations
alembic upgrade head
```

### 5. Run

```bash
# Start the scheduled crawler
python -m crawler.main

# Or run directly
crawler
```

## Configuration Reference

### `configs/search.yaml`

```yaml
searches:
  - keywords: ["AI engineer", "agent engineer"]
    locations: ["San Francisco, CA", "Remote"]
    job_boards: [indeed, linkedin, glassdoor, zip_recruiter]
    company_pages:
      - url: "https://careers.google.com/jobs/results/?q={keyword}"
        js_render: true

schedule:
  cron: "0 8 * * *"
  timezone: "UTC"
```

### Adding a New Company Career Page

Add an entry under `company_pages` in `search.yaml`:

```yaml
- url: "https://company.com/careers?search={keyword}&loc={location}"
  js_render: true  # set to true if the page requires JavaScript rendering
```

The `{keyword}` and `{location}` placeholders are automatically substituted.

## Architecture

```
search.yaml → Scheduler → Engine → JobSpy (job boards)
                                  → Fetcher + LLM Extractor (company pages)
                                  → PostgreSQL (deduplicated storage)
```

## Testing

```bash
pip install -e '.[dev]'
pytest tests/ -v
```

## Project Structure

```
src/crawler/
├── main.py              # Entry point + scheduler
├── engine.py            # Crawl orchestration
├── settings.py          # Configuration loader
├── jobspy_adapter.py    # JobSpy wrapper
├── db/                  # Database models + queries
├── generic/             # Stealth fetcher + LLM extractor + cache
└── anti_throttle/       # Delays, proxies, fingerprinting, circuit breaker
```
