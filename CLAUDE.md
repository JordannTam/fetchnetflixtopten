# CLAUDE.md

## Project Overview

Netflix Top 10 scraper + artist linking pipeline - AWS Lambda function that collects weekly rankings for 18 countries, resolves titles to canonical content IDs (TMDB), links to tracked artists in MongoDB, and materializes weekly `artist_drama_score`. Python 3.12.

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
- **Linking pipeline**: `src/linking/artist_linker.py` resolves titles via TMDB, matches credits to tracked artists (exact-alias + fuzzy review), materializes `artist_drama_score`.
- **Storage**: MongoDB with upsert by `(week, country, category)` compound index; multi-collection writes run in a single transaction (replica set required).
- **Config**: All from environment variables, loaded in `src/config.py`. Required: `MONGODB_URI`. Optional: `TMDB_API_KEY`, `LINKING_TEST_MODE`, `ARTISTS_SOURCE_DATABASE`, `ARTISTS_MONGODB_URI`.

## Event Modes

The handler accepts optional event payloads:

- `{}` - default weekly run (latest week)
- `{"target_week": "YYYY-MM-DD"}` - run for a specific week
- `{"backfill_weeks": N}` - run for the N most recent weeks

## Code Conventions

- All dataclasses are **frozen** (immutable) - never mutate, always create new instances
- Tuples over lists for immutable collections in models
- Specific exception types only (`PyMongoError`, `RequestException`) - no bare `except Exception`
- Structured JSON logging via `_JSONFormatter` in handler
- No `print()` statements - use `logging` module
- The 18 tracked countries are defined in `src/config.py:TRACKED_COUNTRIES`
- All multi-collection writes must be idempotent (`UpdateOne` + `upsert=True`)

## MongoDB Collections

### In `MONGODB_URI` target database (default: `netflix_top10`)

- `weekly_rankings` - one document per (week, country, category) with rankings array
- `scrape_runs` - audit trail with per-run linking metrics

### In `general` database (or main DB when `LINKING_TEST_MODE=true`)

- `content_catalog` - one doc per canonical (provider, provider_content_id); stored once globally
- `content_artist_links` - many-to-many, unique by (tenant_id, content_provider, content_id, artist_id, role)
- `artist_drama_score` - materialized weekly score per (tenant_id, artist_id, year, week, country)
- `match_review_queue` - ambiguous matches pending manual review

### Read-only from `general.artists`

- Tracked artists source. Expected fields: `artist_id`, `english_name`, `korean_name`, `aliases`, `normalized_aliases`, `tenant_id`, `type`, `image`/`image_url`, `is_active`.

## Scoring (Netflix-only for now)

Formula per matched title:

```
rank_score = 11 - rank           # rank 1 => 10, rank 10 => 1
DP_netflix = rank_score + (weeks_in_top_10 * 10) + (streamed_hours / 1_000_000)
```

Per-artist weekly score:

- Dedupe same `content_id` across countries; take **max** contribution per title.
- Sum across distinct matched titles for that (tenant, artist, year, week, country=GLOBAL).
- `drama_score = netflix_score` (until non-Netflix sources are added).
- `netflix_normalized`: min-max normalization scoped to `(tenant_id, year, week, country)`. If min == max, falls back to `100`.

## Matching Policy

- Normalize names via `NFKC` + casefold + strip non-word chars (regex) before comparison.
- **Auto-link** when external credit exactly matches `english_name`, `korean_name`, or any alias (after normalization) for exactly one artist per tenant.
- If multiple artists in the same tenant match exactly, send all candidates to `match_review_queue`.
- If no exact match but fuzzy similarity >= `fuzzy_review_threshold` (default 0.85), send to review queue.
- Otherwise ignore (no link, counted as unmatched).
- Exact-alias hits across different tenants each auto-link within their tenant.

## Test Fixtures

- `tests/fixtures/sample_countries.tsv` - real TSV data for US + South Korea (latest week)
- `tests/fixtures/sample_top10.html` - real HTML snapshot of US films page
- `tests/fixtures/sample_top10.xlsx` - Netflix all-time most popular Excel (not used by fetcher, kept for reference)

## Gotchas

- HTML scraping uses `data-uia` attributes (stable) not CSS classes (auto-generated, break on redesigns)
- TSV fetcher filters to latest week only (or selected weeks for backfill), then filters to the 18 tracked countries
- MongoDB client uses `maxPoolSize=1` for Lambda cold start optimization
- HTML fallback has 1.5s delay between requests to avoid rate limiting
- The `openpyxl` dependency is in requirements.txt but not actively used by fetchers (the Excel "most popular" file turned out to be all-time global data, not weekly per-country)
- Transactions require MongoDB replica set or sharded cluster; standalone `mongod` will fail with `IllegalOperation`
- Artist linking only runs when `TMDB_API_KEY` is set; otherwise `content_resolved` stays 0 and entries remain `unmatched`
- `GLOBAL` drama scores are de-duplicated by `content_id` to avoid cross-country inflation
- `netflix_normalized` is tenant-scoped, so tenants with very few artists tend toward 0/100
