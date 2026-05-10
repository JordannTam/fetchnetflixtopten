# CLAUDE.md

## Project Overview

Artist alias maintenance scripts for the `general.artists` collection. Populates each `Actor` artist with TMDB-derived aliases (english, korean, transliterations) so downstream cast-credit matching can resolve names accurately.

Python 3.12. No application server, no Lambda, no test suite — just two scripts you run against MongoDB.

## Layout

```
scripts/
  backfill_aliases_from_tmdb.py  # populate aliases + tmdb_person_id from TMDB
  rollback_aliases_backfill.py   # undo a backfill run (re-fetches AKA, $pullAll)
requirements.txt
.env.example
```

## Required env vars

- `TMDB_API_KEY` — v3 API key
- `ARTISTS_MONGODB_URI` (or `MONGODB_URI` as fallback)
- `ARTISTS_SOURCE_DATABASE` (default `general`)

## Key Commands

```bash
# Dry-run on 10 actors first
python3 scripts/backfill_aliases_from_tmdb.py --limit 10

# Apply on 10 actors
python3 scripts/backfill_aliases_from_tmdb.py --limit 10 --apply

# Apply on all actors with type: "Actor"
python3 scripts/backfill_aliases_from_tmdb.py --apply

# Force re-process artists that already have tmdb_person_id
python3 scripts/backfill_aliases_from_tmdb.py --apply --force

# Roll back ALL artists currently carrying tmdb_person_id
python3 scripts/rollback_aliases_backfill.py --apply
```

Both scripts dry-run by default; pass `--apply` to commit.

## Schema written to `general.artists`

```js
{
  // existing fields preserved
  english_name: "Tiffany Young",
  korean_name: "티파니",
  type: ["Actor"],            // or ["Musician", "Actor"]
  // backfill adds:
  tmdb_person_id: 1471055,
  aliases: ["Stephanie Young Hwang", "황미영", "黃美永", ...],
  normalized_aliases: ["stephanieyounghwang", "황미영", ...],
}
```

## Backfill Strategy

The script is conservative — it would rather skip than write the wrong person.

1. **Pre-filter**: only artists with `'Actor'` in `type` (skips musicians/groups; TMDB person search is meaningless for them).
2. **Korean-name verification**: search `/search/person`, then for each top-5 candidate fetch `/person/{id}` and only accept candidates whose `also_known_as` (NFKC-normalized) contains the artist's `korean_name`.
3. **Skip ambiguity**: if zero or multiple candidates verify, skip the artist (logged as `missing`).
4. **Group-name filter**: aliases whose normalized form matches another artist's primary name or alias get dropped (catches `Red Velvet`, `소녀시대`, etc.).
5. **Idempotent**: `$addToSet` dedupes on re-runs; `tmdb_person_id` cache lets re-runs skip the search step.

## Rollback Strategy

Finds all artists currently carrying `tmdb_person_id`, refetches the AKA from TMDB, then `$pullAll` those exact strings (and their normalized forms) and `$unset`s `tmdb_person_id`. Surgical — only removes what backfill added.

## Code Conventions

- Specific exception types only (`PyMongoError`, `RequestException`) — no bare `except Exception`.
- `logging` module (no `print` in scripts).
- Frozen-style: `frozenset` for immutable lookup sets; never mutate dicts in place.

## Gotchas

- TMDB v3 mononym search is unreliable — "Joy", "Wendy", "Irene" return 20 candidates. Korean-name verification is the load-bearing filter; do not weaken it without a replacement.
- Group artists (Girls' Generation, Red Velvet, AOA) are stored in `general.artists` as `type: ["Musician"]`. The pre-filter excludes them automatically.
- `also_known_as` from TMDB often includes the person's *group name*. The blocked-set filter catches groups already in the artists collection. Foreign-script transliterations of group names (e.g. `少女時代`) leak unless they're explicitly added as aliases on the group's artist doc.
- `aliases` field is a flat `string[]` (not subdoc[]). Decision made for simplicity; no provenance metadata.
- The blocked-set is rebuilt at startup, so once a group name is anywhere in the collection's aliases, future runs filter it everywhere.
