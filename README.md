# Netflix Top 10 Scraper + Artist Linking

AWS Lambda function that collects weekly Netflix Top 10 rankings for 18 countries, links matched titles to tracked artists in MongoDB via the TMDB API, and materializes a weekly `artist_drama_score` for downstream popularity ranking.

## Why This Approach

### The Original Problem

The original scraper (`main.py`) used BeautifulSoup to parse Netflix's Top 10 HTML pages. It broke when Netflix redesigned their site because it targeted auto-generated CSS class names like `css-1rheyty ehtxqvx0` - these are produced by CSS-in-JS toolchains and change on every build.

### The Solution: Structured Data First, HTML Last

Netflix publishes a **public TSV file** (`all-weeks-countries.tsv`) containing every Top 10 list for every country, every week, going back 240+ weeks. This is a ~28MB structured data export that's immune to UI redesigns because it's data, not a rendered page.

```
Data Flow:

  Netflix TSV ─────> Parse TSV ──┐
  (primary, fast)    (1 request) │
                                 ├──> Validate ──> Link (TMDB + artists) ──> MongoDB (transactional upsert)
  Netflix HTML ────> Scrape HTML ┘
  (fallback, slow)   (36 requests)
```

**Why TSV over HTML scraping:**

| | TSV Export | HTML Scraping |
|---|---|---|
| Requests needed | 1 (single file) | 36 (18 countries x 2 categories) |
| Speed | ~2 seconds | ~54 seconds (with rate limiting) |
| Stability | Very high (structured data) | Fragile (DOM can change) |
| Data completeness | All countries, all weeks | One page at a time |
| Risk of breaking | Only if Netflix removes the file | Any site redesign |

**Why keep the HTML fallback:**
The TSV file is not officially documented. If Netflix removes it or changes the URL, the HTML scraper kicks in automatically. The HTML fallback uses `data-uia` attributes (Netflix's own test automation hooks) instead of CSS classes, making it more resilient than the original scraper.

### Why Upsert + Transactions

Every write is an idempotent `UpdateOne` + `upsert=True`:

- **First run**: inserts all documents
- **Re-run same week**: updates in place (no duplicates)
- **Safe to retry**: if Lambda times out or partially fails, just run again

Ranking writes and all linking writes (catalog, artist links, drama score, review queue) run inside a **single MongoDB transaction** so either everything commits or nothing does. Transactions require a replica set or sharded cluster - a bare standalone `mongod` will fail.

### Why AWS Lambda

- Runs once a week for ~5 seconds - no need for a 24/7 server
- EventBridge cron trigger handles scheduling
- CloudWatch captures structured JSON logs automatically
- MongoDB connection stays warm between invocations (Lambda reuses containers)

## Data Sources

1. **Primary: Netflix TSV export** - Downloads `netflix.com/tudum/top10/data/all-weeks-countries.tsv`. One HTTP request for all data.
2. **Fallback: HTML scraping** - BeautifulSoup with `data-uia` selectors. 36 individual page requests with 1.5s rate limiting.
3. **TMDB API** (optional) - Resolves titles to canonical `movie`/`tv` IDs and fetches cast/crew for artist matching. Requires `TMDB_API_KEY`.

## Tracked Countries

South Korea, Hong Kong, Taiwan, Japan, Thailand, Vietnam, Philippines, Indonesia, United States, Canada, Brazil, Mexico, United Kingdom, Germany, France, Spain, Italy, Australia

Configured in `src/config.py:TRACKED_COUNTRIES`. Add or remove countries there.

## How It Works

```
EventBridge (weekly cron) or manual invoke
    │
    ▼
Lambda handler (src/handler.py)
    │
    ├── 1. Load config from environment
    │
    ├── 2. Create HTTP session (retry + backoff)
    │
    ├── 3. Fetch rankings (src/fetchers/orchestrator.py)
    │       ├── Try TSV download (src/fetchers/tsv_fetcher.py)
    │       │     Download 28MB TSV → filter to weeks → 18 countries
    │       │
    │       └── On failure: HTML scrape (src/fetchers/html_fetcher.py)
    │             For each country+category: fetch page → parse with data-uia
    │
    ├── 4. Validate (src/validation/validators.py)
    │
    ├── 5. Link to tracked artists (src/linking/artist_linker.py)  [only if TMDB_API_KEY set]
    │       ├── Load tracked artists (from ARTISTS_MONGODB_URI or primary)
    │       ├── For each unique title: TMDB search → credits
    │       ├── Match credits to tracked artists (exact alias or fuzzy review)
    │       ├── Compute DP_netflix per title → aggregate per artist
    │       └── Min-max normalize per (tenant, year, week, country)
    │
    ├── 6. Persist in a single MongoDB transaction (src/storage/repository.py)
    │       weekly_rankings + content_catalog + content_artist_links
    │                      + artist_drama_score + match_review_queue
    │
    └── 7. Log audit record (scrape_runs collection) with linking metrics
```

## Project Structure

```
src/
  config.py                  # TRACKED_COUNTRIES, Netflix + Mongo + TMDB + linking config
  models.py                  # Frozen dataclasses (RankingEntry, ContentRef, ScrapeRun, ...)
  handler.py                 # Lambda entry point - orchestrates the pipeline
  fetchers/
    tsv_fetcher.py           # Primary: TSV download + parse (supports backfill)
    html_fetcher.py          # Fallback: BeautifulSoup with data-uia selectors
    orchestrator.py          # TSV -> HTML fallback chain
    http_client.py           # Requests session with retry/exponential backoff
  linking/
    artist_linker.py         # TMDB resolution, artist matching, drama score calc
  storage/
    mongo_client.py          # MongoDB singleton + external client manager
    repository.py            # Idempotent writes across all collections
  validation/
    validators.py            # Data integrity checks before storage
tests/
  unit/                      # Unit tests (fetchers, models, repo, linker)
  integration/               # Full handler tests with mocked HTTP + MongoDB
  fixtures/                  # Real TSV + HTML snapshots from Netflix
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `MONGODB_URI` | yes | Connection string for the main Mongo cluster (rankings + linking outputs). Must be a replica set/sharded cluster to support transactions. |
| `TMDB_API_KEY` | no | Enables the linking pipeline. Without it, only rankings are written. |
| `LINKING_TEST_MODE` | no | `true` to read artists from `ARTISTS_SOURCE_DATABASE` (or `ARTISTS_MONGODB_URI`) while all linking writes still go to the main DB from `MONGODB_URI`. Default `false`. |
| `ARTISTS_SOURCE_DATABASE` | no | Source DB name for tracked artists (default `general`). |
| `ARTISTS_MONGODB_URI` | no | Separate Mongo URI for reading artists if they live in another cluster. |

Copy `.env.example` to `.env` and fill in your values.

## MongoDB Schema

### `weekly_rankings` (main DB)

```json
{
  "week": "2026-02-01",
  "country": "united-states",
  "country_name": "United States",
  "category": "films",
  "fetched_at": "2026-02-09T12:00:00Z",
  "source": "tsv",
  "rankings": [
    {
      "rank": 1,
      "title": "The Rip",
      "weeks_in_top_10": 3,
      "streamed_hours": 12500000,
      "content_ref": { "provider": "tmdb", "provider_content_id": "12345" },
      "match_status": "matched",
      "linked_artist_ids": ["artist_abc"]
    }
  ]
}
```

Unique compound index on `(week, country, category)`.

### `content_catalog` (general DB)

One document per canonical title, written once globally:

```json
{
  "provider": "tmdb",
  "provider_content_id": "12345",
  "title": "The Rip",
  "media_type": "movie",
  "original_title": "The Rip",
  "release_date": "2026-01-15",
  "first_seen_week": "2026-02-01",
  "last_seen_week": "2026-02-01"
}
```

### `content_artist_links` (general DB)

Many-to-many linking table. Unique by `(tenant_id, content_provider, content_id, artist_id, role)`.

### `artist_drama_score` (general DB)

Mirrors `artist_music_score`. One document per `(tenant_id, artist_id, year, week, country)`:

```json
{
  "tenant_id": "mishkan",
  "artist_id": "artist_abc",
  "year": 2026,
  "week": 6,
  "country": "GLOBAL",
  "netflix_score": 416,
  "drama_score": 416,
  "netflix_normalized": 87.5,
  "updated_at": "2026-02-09T06:00:05Z"
}
```

### `match_review_queue` (general DB)

Ambiguous or fuzzy matches pending manual review. Surfaces cases where a credit name matches multiple artists within the same tenant or scores below the auto-link threshold.

### `scrape_runs` (main DB)

```json
{
  "run_id": "550e8400-...",
  "started_at": "2026-02-09T06:00:00Z",
  "completed_at": "2026-02-09T06:00:05Z",
  "status": "success",
  "source_used": "tsv",
  "total_documents_saved": 36,
  "linking_metrics": {
    "content_resolved": 42,
    "artists_linked": 18,
    "ambiguous_matches": 2,
    "unmatched_entries": 140,
    "drama_scores_upserted": 18
  },
  "errors": []
}
```

## Drama Score Algorithm

Currently Netflix-only (extensible to other sources later).

```
rank_score = 11 - rank                 # rank 1 => 10, rank 10 => 1
DP_netflix = rank_score + (weeks_in_top_10 * 10) + (streamed_hours / 1_000_000)
```

Per artist, per week:

1. Collect all matched titles for that artist.
2. For `GLOBAL` aggregation, take the **max** `DP_netflix` contribution per title (avoids double counting when a title appears in multiple countries).
3. Sum across titles -> `netflix_score`.
4. `drama_score = netflix_score` (for now).
5. Apply **min-max normalization** scoped to `(tenant_id, year, week, country)` to get `netflix_normalized` (0-100). If min == max, falls back to `100`.

## Artist Matching Policy

Names are normalized via Unicode NFKC + casefold + regex strip of non-word chars before comparison.

- **Exact match** against `english_name`, `korean_name`, or any entry in `aliases` / `normalized_aliases` -> auto-link, provided the credit resolves to exactly one artist for that tenant.
- **Multiple exact matches** within a single tenant -> all candidates sent to `match_review_queue`.
- **Fuzzy match** >= `fuzzy_review_threshold` (default 0.85) -> review queue.
- Everything else is counted under `unmatched_entries` (common for non-Korean content in a Korean-focused artist corpus).

## Local Development

```bash
# Install dependencies
pip install -r requirements-dev.txt

# Configure env
cp .env.example .env
# Edit .env and fill in MONGODB_URI (and optional TMDB_API_KEY)

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=term-missing
```

### Running locally against MongoDB

Transactions require a replica set, not a plain standalone. The easiest local setup is a single-node replica set in Docker:

```bash
docker run -d --name mongodb-rs -p 27017:27017 mongo:7 \
  mongod --replSet rs0 --bind_ip_all

# Initiate the replica set (first time only)
docker exec -it mongodb-rs mongosh --eval "rs.initiate()"
```

Then set in `.env`:

```
MONGODB_URI=mongodb://localhost:27017/netflix_top10?replicaSet=rs0&directConnection=true
```

Invoke the handler locally:

```bash
python3 -c "from src.handler import lambda_handler; print(lambda_handler({}, None))"
```

Or with event payloads:

```bash
# Specific week
python3 -c "from src.handler import lambda_handler; print(lambda_handler({'target_week': '2026-02-01'}, None))"

# Backfill last N weeks
python3 -c "from src.handler import lambda_handler; print(lambda_handler({'backfill_weeks': 8}, None))"
```

Inspect results:

```bash
mongosh "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
```

```js
use netflix_top10
db.weekly_rankings.countDocuments()      // 36 per week
db.scrape_runs.find().sort({started_at: -1}).limit(1)

use general
db.content_catalog.countDocuments()
db.content_artist_links.countDocuments()
db.artist_drama_score.find().limit(5)
db.match_review_queue.find().limit(5)
```

Or use [MongoDB Compass](https://www.mongodb.com/products/compass) to browse the databases visually.

## AWS Lambda Deployment

### Build the deployment zip

```bash
pip install -r requirements.txt -t package/
cd package && zip -r ../lambda.zip . && cd ..
zip lambda.zip -r src/
```

### AWS Console setup

1. **Create Lambda function**
   - Runtime: Python 3.12
   - Handler: `src.handler.lambda_handler`
   - Upload `lambda.zip`

2. **Environment variables**
   - `MONGODB_URI` - main Mongo connection string (replica set / Atlas)
   - `TMDB_API_KEY` - optional, enables linking + drama score materialization
   - `LINKING_TEST_MODE` - optional (`true/false`), read artists from another DB but write everything to the main DB
   - `ARTISTS_SOURCE_DATABASE` - optional, defaults to `general`
   - `ARTISTS_MONGODB_URI` - optional, separate cluster for artists

3. **Configuration**
   - Timeout: 120 seconds
   - Memory: 256 MB

4. **EventBridge schedule** (weekly trigger)
   - Rule type: Schedule
   - Cron expression: `cron(0 6 ? * SUN *)`
   - Target: your Lambda function

5. **Test** - Use the "Test" button in Lambda console
   - Default: empty event `{}`
   - Backfill: `{"backfill_weeks": 8}`
   - Specific week: `{"target_week": "2026-02-01"}`

## Lambda Response

```json
{
  "statusCode": 200,
  "body": {
    "run_id": "uuid",
    "status": "success",
    "source_used": "tsv",
    "saved": 36,
    "linking_metrics": {
      "content_resolved": 42,
      "artists_linked": 18,
      "ambiguous_matches": 2,
      "unmatched_entries": 140,
      "drama_scores_upserted": 18
    },
    "errors": []
  }
}
```

Status values: `success`, `partial_failure`, `failure`
