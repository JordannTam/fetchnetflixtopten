# CLAUDE.md

## Project Overview

Netflix Top 10 scraper - AWS Lambda function that collects weekly rankings for 18 countries and stores them in MongoDB. Python 3.12.

## Key Commands

```bash
# Run all tests
pytest tests/ -v

# Run with coverage (target: 80%+)
pytest tests/ --cov=src --cov-report=term-missing

# Run unit tests only
pytest tests/unit/ -v

# Run integration tests only
pytest tests/integration/ -v

# Build Lambda deployment zip
pip install -r requirements.txt -t package/ && cd package && zip -r ../lambda.zip . && cd .. && zip lambda.zip -r src/
```

## Architecture

- **Entry point**: `src/handler.py` -> `lambda_handler(event, context)`
- **Fetch pipeline**: TSV (primary) -> HTML scraping (fallback), orchestrated by `src/fetchers/orchestrator.py`
- **Data source**: Netflix's public TSV at `netflix.com/tudum/top10/data/all-weeks-countries.tsv`
- **Storage**: MongoDB with upsert by `(week, country, category)` compound index
- **Config**: All from environment variables, loaded in `src/config.py`. Only required env var is `MONGODB_URI`.

## Code Conventions

- All dataclasses are **frozen** (immutable) - never mutate, always create new instances
- Tuples over lists for immutable collections in models
- Specific exception types only (`PyMongoError`, `RequestException`) - no bare `except Exception`
- Structured JSON logging via `_JSONFormatter` in handler
- No `print()` statements - use `logging` module
- The 18 tracked countries are defined in `src/config.py:TRACKED_COUNTRIES`

## MongoDB Collections

- `weekly_rankings` - one document per (week, country, category) with rankings array
- `scrape_runs` - audit trail for each Lambda invocation

## Test Fixtures

- `tests/fixtures/sample_countries.tsv` - real TSV data for US + South Korea (latest week)
- `tests/fixtures/sample_top10.html` - real HTML snapshot of US films page
- `tests/fixtures/sample_top10.xlsx` - Netflix all-time most popular Excel (not used by fetcher, kept for reference)

## Gotchas

- HTML scraping uses `data-uia` attributes (stable) not CSS classes (auto-generated, break on redesigns)
- TSV fetcher filters to latest week only, then filters to the 18 tracked countries
- MongoDB client uses `maxPoolSize=1` for Lambda cold start optimization
- HTML fallback has 1.5s delay between requests to avoid rate limiting
- The `openpyxl` dependency is in requirements.txt but not actively used by fetchers (the Excel "most popular" file turned out to be all-time global data, not weekly per-country)
