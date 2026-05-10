# CLAUDE.md

## Project Overview

Two pieces:

1. **`handler.py`** — weekly pipeline (also a Lambda entry point). Fetches Netflix's TSV, resolves titles via TMDB, matches cast to tracked artists by `tmdb_person_id`, scores `DP_Netflix`, and upserts results to `general.artist_dp_netflix`.
2. **`scripts/`** — alias maintenance for `general.artists`. The pipeline's deterministic match step depends on these aliases being populated.

Python 3.12. No test suite (intentional — happy-path-only scripts).

## Layout

```
handler.py                          # Lambda + CLI: Netflix TSV -> TMDB -> scores
scripts/
  backfill_aliases_from_tmdb.py     # populate tmdb_person_id + aliases on artists
  rollback_aliases_backfill.py      # undo a backfill run
requirements.txt                    # requests + pymongo
.env.example
```

## Required env vars

- `TMDB_API_KEY` — v3 API key
- `ARTISTS_MONGODB_URI` (or `MONGODB_URI` as fallback)
- `ARTISTS_SOURCE_DATABASE` (default `general`)
- `TMDB_TIMEOUT` (default `10`, used by Lambda only)

## Key Commands

```bash
# Pipeline (Lambda + CLI)
python3 handler.py                  # dry-run: print 3 sample docs, no DB writes
python3 handler.py --apply          # commit upserts to general.artist_dp_netflix
python3 handler.py --apply --samples 0   # quiet apply

# Alias backfill
python3 scripts/backfill_aliases_from_tmdb.py --limit 10        # dry-run
python3 scripts/backfill_aliases_from_tmdb.py --apply           # all Actors
python3 scripts/backfill_aliases_from_tmdb.py --apply --force   # re-process existing

# Alias rollback (undo backfill)
python3 scripts/rollback_aliases_backfill.py --apply
```

All scripts dry-run by default. Lambda (`lambda_handler`) always commits.

## Pipeline Schema (`general.artist_dp_netflix`)

```js
{
  artist_id:    ObjectId("..."),    // ┐
  tenant_id:    ObjectId("..."),    // ├─ unique key (tenant_id, artist_id, year, week)
  year:         2026,               // │
  week:         18,                 // ┘  ISO week number
  week_date:    "2026-05-03",       // chart-end Sunday (kept for human readability)
  english_name: "Go YounJung",
  korean_name:  "고윤정",
  dp_netflix:   552.0,
  dramas: [
    { tmdb_id, title, release_year, contribution },
    ...
  ],
  updated_at:   ISODate(...)
}
```

Unique compound index `tenant_artist_year_week_unique` on `(tenant_id, artist_id, year, week)` is created automatically on first write.

## Pipeline Strategy

1. **Fetch TSV**: single GET to `netflix.com/tudum/top10/data/all-weeks-countries.tsv`.
2. **Filter**: latest `week` only, 18 tracked countries (hardcoded in `handler.TRACKED_COUNTRIES`), use `show_title` (NOT `season_title` which is `"N/A"` for films and breaks TMDB search).
3. **Resolve titles** via `/search/{movie|tv}` — first hit; capture `release_date` / `first_air_date` for `release_year`. Cached by `(title, category)`.
4. **Fetch cast** via `/{media_type}/{id}/credits`. Cached by `tmdb_id`.
5. **Match deterministically**: `dict.get(tmdb_person_id)` against the in-memory artist index. No name normalization, no fuzzy.
6. **Score**: per `(artist, tmdb_id)` take the max DP across countries; sum across distinct titles per artist.
7. **Upsert** into `general.artist_dp_netflix`.

## Scoring Formula (Netflix-only)

```
Rank Score = 201 - rank             # rank 1 = 200, rank 10 = 191
DP_Netflix = Rank Score
           + (Weeks on Chart * 10)
           + (Streamed Hours / 1,000,000)
```

Per artist: max contribution per `tmdb_id` across countries (avoids cross-country inflation), then sum across distinct titles. `DP_Spotify_OST` is sourced separately from another collection.

## Alias Backfill Strategy

Conservative — skips uncertain matches rather than guessing.

1. **Pre-filter**: only artists with `'Actor'` in `type` (skips musicians/groups; TMDB person search is meaningless for them).
2. **Korean-name verification**: search `/search/person`, then for each top-5 candidate fetch `/person/{id}` and only accept candidates whose `also_known_as` (NFKC-normalized) contains the artist's `korean_name`.
3. **Skip ambiguity**: if zero or multiple candidates verify, skip the artist (logged as `missing`).
4. **Group-name filter**: aliases whose normalized form matches another artist's primary name or alias get dropped (catches `Red Velvet`, `소녀시대`, etc.).
5. **Idempotent**: `$addToSet` dedupes on re-runs; `tmdb_person_id` cache lets re-runs skip the search step.

Schema written to `general.artists`:
```js
{
  english_name, korean_name, type, ...                    // existing
  tmdb_person_id: 1471055,                                // backfill adds
  aliases: ["Stephanie Young Hwang", "황미영", ...],       // backfill adds
  normalized_aliases: ["stephanieyounghwang", "황미영", ...], // backfill adds
}
```

## Rollback Strategy (aliases)

Finds all artists currently carrying `tmdb_person_id`, refetches the AKA from TMDB, then `$pullAll` those exact strings (and their normalized forms) and `$unset`s `tmdb_person_id`. Surgical — only removes what backfill added.

## Code Conventions

- Specific exception types only (`PyMongoError`, `RequestException`) — no bare `except Exception`.
- `logging` module (no `print` in scripts; CLI sample output uses `print()` deliberately).
- `frozenset` for immutable lookup sets; never mutate dicts in place.
- `--apply` flag pattern: dry-run is default, `--apply` commits writes.

## Gotchas

- **`season_title` is `"N/A"` for films** — must search by `show_title`. Filtering on `season_title or show_title` silently picks "N/A" because Python treats it as truthy.
- **TMDB v3 mononym search is unreliable** — "Joy", "Wendy", "Irene" return 20 candidates ranked by global popularity. Korean-name verification is the load-bearing filter; do not weaken it without a replacement.
- **Group artists** (Girls' Generation, Red Velvet, AOA) are stored as `type: ["Musician"]`. The Actor pre-filter excludes them automatically.
- **TMDB `also_known_as` often includes group names**. The blocked-set filter catches groups already in the artists collection. Foreign-script transliterations of group names (e.g. `少女時代`) leak unless they're added as aliases on the group's artist doc.
- **`week` is an ISO week number (int)**, not a date. The unique key is `(tenant_id, artist_id, year, week)` because `week` alone wraps yearly. The original chart-end Sunday is preserved as `week_date`.
- **Pipeline coverage** is bounded by alias backfill — only artists with `tmdb_person_id` can match. Re-run the backfill (with relaxed filters or manual curation) to extend coverage.
- **No retry / no error backoff** — TMDB or Mongo failures fail the run. Acceptable for a weekly batch; revisit if Lambda failures become routine.
