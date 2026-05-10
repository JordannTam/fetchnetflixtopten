# Netflix Top 10 → DP_Netflix Pipeline

Weekly pipeline that scores tracked Korean actors based on Netflix Top 10 chart performance.

```
Netflix TSV ─→ TMDB resolve title ─→ TMDB cast ─→ match by tmdb_person_id ─→ DP_Netflix → MongoDB
```

Two parts:

1. **`handler.py`** — the pipeline (also runnable as an AWS Lambda).
2. **`scripts/`** — alias maintenance for `general.artists` (a prerequisite — actors need `tmdb_person_id` populated for the pipeline to match them).

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env  # fill in TMDB_API_KEY and ARTISTS_MONGODB_URI
```

Required env: `TMDB_API_KEY`, `ARTISTS_MONGODB_URI` (or `MONGODB_URI`). Optional: `ARTISTS_SOURCE_DATABASE` (default `general`), `TMDB_TIMEOUT` (default `10`).

## Running the pipeline

```bash
# Dry-run: print 3 sample documents, no DB writes
python3 handler.py

# Commit: upsert all scores into general.artist_dp_netflix
python3 handler.py --apply

# Quiet apply (skip sample preview)
python3 handler.py --apply --samples 0
```

CLI flags:
- `--apply` — commit upserts (default: dry-run preview)
- `--samples N` — how many sample docs to print (default 3)
- `--api-key`, `--uri`, `--db`, `--timeout` — override env vars

The Lambda entry (`handler.lambda_handler`) always commits — there's no dry-run for invocations.

## What gets written

Collection: `general.artist_dp_netflix`
Unique key: `(tenant_id, artist_id, year, week, country)`

Each weekly run writes **one doc per (artist, country) the artist charted in** PLUS **one GLOBAL doc per artist** (cross-country aggregate). For 46 scored artists this typically produces ~120 per-country docs + 46 GLOBAL = ~165 docs/week.

```js
{
  artist_id:    ObjectId("..."),
  tenant_id:    ObjectId("..."),
  year:         2026,
  week:         18,                   // ISO week number
  week_date:    "2026-05-03",         // chart-end Sunday
  country:      "South Korea",        // or "GLOBAL"
  english_name: "Go YounJung",
  korean_name:  "고윤정",
  dp_netflix:   229.0,                // per-country: only that country's chart rows
                                      // GLOBAL:      max-per-drama across countries
  dramas: [
    {
      tmdb_id:      229891,
      title:        "Can This Love Be Translated?",
      release_year: 2026,
      country:      "South Korea",
      rank:         2,
      contribution: 229.0
    },
    ...
  ],
  updated_at:   ISODate(...)
}
```

Indexes created automatically on first write:

| Index | For |
|---|---|
| `tenant_artist_year_week_country_unique` | Idempotent upsert + uniqueness |
| `leaderboard_by_country_week` | Top-N artists in country X for week Y |
| `artist_country_timeline` | Single artist's weekly timeline in country X |

## Frontend query patterns

```js
// Artist worldwide timeline (chart over weeks)
db.artist_dp_netflix.find({artist_id: X, country: "GLOBAL"}).sort({year:1, week:1})

// Artist's timeline in a specific country
db.artist_dp_netflix.find({artist_id: X, country: "South Korea"}).sort({year:1, week:1})

// Top 10 artists in Vietnam this week
db.artist_dp_netflix.find({country: "Vietnam", year: 2026, week: 18})
  .sort({dp_netflix: -1}).limit(10)

// Country comparison for one artist this week
db.artist_dp_netflix.find({artist_id: X, year: 2026, week: 18})
```

GLOBAL vs per-country totals can disagree — that's intentional. GLOBAL takes the max contribution per drama across countries (so a single drama in 5 countries isn't double-counted); per-country only considers that country's chart rows. A drama appearing only in Vietnam adds to Vietnam's total and to GLOBAL, but not to Korea's total.

## Scoring formula

```
Rank Score = 201 - rank             (rank 1 = 200, rank 10 = 191)
DP_Netflix = Rank Score
           + (Weeks on Chart × 10)
           + (Streamed Hours / 1,000,000)
```

Per artist: take the **max** contribution per `tmdb_id` across countries (a single drama appearing in 5 countries' top 10 isn't double-counted), then **sum** across distinct titles for that artist.

`DP_Spotify_OST` and `DP_IG_Hashtags` are sourced from other collections; `DP = DP_Netflix + DP_Spotify_OST + DP_IG_Hashtags` happens downstream.

## Pipeline strategy

```
1. Fetch Netflix TSV (single GET)
2. Filter: latest week only, 18 tracked countries
3. For each unique (title, category): TMDB /search/{movie|tv} → tmdb_id, release_year
4. For each tmdb_id: TMDB /credits → cast list
5. Match: dict.get(tmdb_person_id) against in-memory {tmdb_person_id: artist} index
6. Score: max-per-title, sum-per-artist
7. Upsert into general.artist_dp_netflix
```

Match step is **deterministic** — single hash lookup by `tmdb_person_id`. No name normalization, no fuzzy matching, no ambiguity handling. Coverage depends on how many actors have `tmdb_person_id` populated (see alias backfill below).

## Alias backfill (prerequisite)

The pipeline can only match artists that have `tmdb_person_id` set. The backfill script populates it (and rich aliases) from TMDB.

```bash
# Test on 10 actors first
python3 scripts/backfill_aliases_from_tmdb.py --limit 10
python3 scripts/backfill_aliases_from_tmdb.py --limit 10 --apply

# Run on all Actors
python3 scripts/backfill_aliases_from_tmdb.py --apply

# Re-process actors that already have tmdb_person_id
python3 scripts/backfill_aliases_from_tmdb.py --apply --force
```

Strategy (conservative — skips uncertain matches):
- Pre-filter to `type: "Actor"` (skips musicians and groups)
- Search `/search/person`, fetch `/person/{id}` for top-5 candidates
- Korean-name verification: only accept candidates whose `also_known_as` contains the artist's `korean_name` (NFKC-normalized)
- Skip if zero or multiple candidates verify
- Group-name filter: drop aliases that match another artist's primary name (catches "Red Velvet", "소녀시대", etc.)

### Rollback

Removes everything backfill wrote — refetches each artist's `also_known_as` from TMDB, `$pullAll` those exact strings, `$unset` `tmdb_person_id`.

```bash
python3 scripts/rollback_aliases_backfill.py             # dry-run
python3 scripts/rollback_aliases_backfill.py --apply
```

## Sample run stats

| Metric | Count |
|---|---|
| Tracked Actors | 1137 |
| Actors with `tmdb_person_id` | 700 (62%) |
| Netflix entries (latest week) | 360 across 18 countries |
| Unique titles | 167 |
| Titles resolved on TMDB | 158 (95%) |
| Cast → artist matches | 125 |
| Artists scored | 46 |
| Docs written (per-country + GLOBAL) | ~165 |
| Pipeline runtime | ~70 seconds |

## Files

```
handler.py                              # pipeline (Lambda + CLI)
scripts/
  backfill_aliases_from_tmdb.py
  rollback_aliases_backfill.py
requirements.txt                        # requests + pymongo
CLAUDE.md                               # context for Claude Code sessions
```
