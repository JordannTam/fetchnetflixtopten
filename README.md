# Netflix Top 10 Scraper

AWS Lambda function that collects weekly Netflix Top 10 rankings for 18 countries and stores them in MongoDB.

## Why This Approach

### The Problem

The original scraper (`main.py`) used BeautifulSoup to parse Netflix's Top 10 HTML pages. It broke when Netflix redesigned their site because it targeted auto-generated CSS class names like `css-1rheyty ehtxqvx0` - these are produced by CSS-in-JS toolchains and change on every build.

### The Solution: Structured Data First, HTML Last

Netflix publishes a **public TSV file** (`all-weeks-countries.tsv`) containing every Top 10 list for every country, every week, going back 240+ weeks. This is a ~28MB structured data export that's immune to UI redesigns because it's data, not a rendered page.

```
Data Flow:

  Netflix TSV ──────> Parse TSV ──┐
  (primary, fast)     (1 request) │
                                  ├──> Validate ──> MongoDB (upsert)
  Netflix HTML ─────> Scrape HTML ┘
  (fallback, slow)    (36 requests)
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

### Why Upsert Instead of Insert

The scraper uses MongoDB's `UpdateOne` with `upsert=True` keyed on `(week, country, category)`. This means:
- **First run**: inserts all documents
- **Re-run same week**: updates existing documents (no duplicates)
- **Safe to retry**: if Lambda times out or partially fails, just run again

### Why AWS Lambda

- Runs once a week for ~5 seconds - no need for a 24/7 server
- EventBridge cron trigger handles scheduling
- CloudWatch captures structured JSON logs automatically
- MongoDB connection stays warm between invocations (Lambda reuses containers)

## Data Sources

1. **Primary: Netflix TSV export** - Downloads `netflix.com/tudum/top10/data/all-weeks-countries.tsv`. One HTTP request for all data.
2. **Fallback: HTML scraping** - BeautifulSoup with `data-uia` selectors. 36 individual page requests with 1.5s rate limiting.

## Tracked Countries

South Korea, Hong Kong, Taiwan, Japan, Thailand, Vietnam, Philippines, Indonesia, United States, Canada, Brazil, Mexico, United Kingdom, Germany, France, Spain, Italy, Australia

Configured in `src/config.py:TRACKED_COUNTRIES`. Add or remove countries there.

## How It Works

```
EventBridge (weekly cron)
    │
    ▼
Lambda handler (src/handler.py)
    │
    ├── 1. Load config from environment (MONGODB_URI)
    │
    ├── 2. Create HTTP session (retry + backoff)
    │
    ├── 3. Fetch rankings (src/fetchers/orchestrator.py)
    │       ├── Try TSV download (src/fetchers/tsv_fetcher.py)
    │       │     Download 28MB TSV → filter to 18 countries → latest week
    │       │
    │       └── On failure: HTML scrape (src/fetchers/html_fetcher.py)
    │             For each country+category: fetch page → parse with data-uia selectors
    │
    ├── 4. Validate (src/validation/validators.py)
    │       Check: ranks 1-10, non-empty titles, no duplicates
    │
    ├── 5. Store to MongoDB (src/storage/repository.py)
    │       bulk_write with upsert by (week, country, category)
    │
    └── 6. Log audit record (scrape_runs collection)
            run_id, status, source_used, saved count, errors
```

## Project Structure

```
src/
  config.py                  # TRACKED_COUNTRIES dict, Netflix + Mongo config
  models.py                  # Frozen dataclasses (RankingEntry, CountryRanking, etc.)
  handler.py                 # Lambda entry point - orchestrates the pipeline
  fetchers/
    tsv_fetcher.py           # Primary: download + parse Netflix TSV export
    html_fetcher.py          # Fallback: BeautifulSoup with data-uia selectors
    orchestrator.py          # TSV -> HTML fallback chain
    http_client.py           # Requests session with retry/exponential backoff
  storage/
    mongo_client.py          # MongoDB singleton connection (warm across invocations)
    repository.py            # Upsert rankings + insert audit records
  validation/
    validators.py            # Data integrity checks before storage
tests/
  unit/                      # 44 tests, 81% coverage
  integration/               # Full handler test with mocked HTTP + MongoDB
  fixtures/                  # Real TSV + HTML snapshots from Netflix
```

## MongoDB Schema

### `weekly_rankings` collection

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
      "weeks_in_top_10": 3
    },
    {
      "rank": 2,
      "title": "M3GAN 2.0",
      "weeks_in_top_10": 1
    }
  ]
}
```

Unique compound index on `(week, country, category)` - re-runs upsert instead of duplicating.

### `scrape_runs` collection

```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "started_at": "2026-02-09T06:00:00Z",
  "completed_at": "2026-02-09T06:00:05Z",
  "status": "success",
  "source_used": "tsv",
  "total_documents_saved": 36,
  "errors": []
}
```

Every Lambda invocation is logged here for monitoring and debugging.

## Local Development

```bash
# Install dependencies
pip install -r requirements-dev.txt

# Set environment variable
cp .env.example .env
# Edit .env with your MongoDB URI

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=term-missing
```

### Running locally with MongoDB

Start a local MongoDB instance (Docker or Homebrew):

```bash
# Docker
docker run -d --name mongodb -p 27017:27017 mongo:7

# OR Homebrew
brew install mongodb-community && brew services start mongodb-community
```

Run the scraper against local MongoDB:

```bash
MONGODB_URI="mongodb://localhost:27017/netflix_top10" python3 -c "
from src.handler import lambda_handler
print(lambda_handler({}, None))
"
```

Connect to inspect results:

```bash
mongosh "mongodb://localhost:27017/netflix_top10"
```

```js
db.weekly_rankings.countDocuments()   // 36 (18 countries x 2 categories)
db.weekly_rankings.findOne()
db.scrape_runs.find().sort({started_at: -1}).limit(1)
```

Or use [MongoDB Compass](https://www.mongodb.com/products/compass) to browse the `netflix_top10` database visually.

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
   - `MONGODB_URI` - your MongoDB connection string

3. **Configuration**
   - Timeout: 120 seconds
   - Memory: 256 MB

4. **EventBridge schedule** (weekly trigger)
   - Rule type: Schedule
   - Cron expression: `cron(0 6 ? * SUN *)`
   - Target: your Lambda function

5. **Test** - Use the "Test" button in Lambda console with an empty event `{}`

## Lambda Response

```json
{
  "statusCode": 200,
  "body": {
    "run_id": "uuid",
    "status": "success",
    "source_used": "tsv",
    "saved": 36,
    "errors": []
  }
}
```

Status values: `success`, `partial_failure`, `failure`
