# Artist Alias Backfill

Scripts for populating TMDB-derived aliases on the `general.artists` collection. Reads each `Actor` artist, searches TMDB for the corresponding person, verifies the match by Korean name, and writes verified aliases back to MongoDB.

## Why

Tracked artists (Korean actors) need rich alias coverage so that downstream cast-credit matching can resolve names across romanizations (`Lee Jung-jae` / `Yi Jung-jae` / `Jungjae Lee` / `이정재` / `李政宰`). Hand-curating ~1100 actors doesn't scale; TMDB's `also_known_as` field already contains these variants. This project automates the backfill with safeguards against the failure modes that come with naive name search (mononym ambiguity, group-name pollution, wrong-person matches).

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env  # fill in TMDB_API_KEY and ARTISTS_MONGODB_URI
```

## Usage

Both scripts dry-run by default. Pass `--apply` to commit.

### Backfill aliases

```bash
# Test on 10 actors (dry run)
python3 scripts/backfill_aliases_from_tmdb.py --limit 10

# Test on 10 actors and commit
python3 scripts/backfill_aliases_from_tmdb.py --limit 10 --apply

# Run on all Actors
python3 scripts/backfill_aliases_from_tmdb.py --apply

# Re-process actors that already have tmdb_person_id (e.g. after improving filters)
python3 scripts/backfill_aliases_from_tmdb.py --apply --force
```

Flags:
- `--limit N` — cap artists processed (testing)
- `--workers N` — concurrency (default 10)
- `--timeout S` — per-request timeout (default 10s)
- `--force` — re-process artists with existing `tmdb_person_id`

### Rollback

Removes everything the backfill wrote — refetches each artist's `also_known_as` from TMDB and `$pullAll` those exact strings, then `$unset` `tmdb_person_id`.

```bash
# Dry run
python3 scripts/rollback_aliases_backfill.py

# Commit
python3 scripts/rollback_aliases_backfill.py --apply
```

## How it works

The backfill is conservative — it skips uncertain matches rather than guessing.

```
For each artist with type: "Actor":
  1. Search TMDB /search/person?query=<english_name or korean_name>
  2. For top-5 candidates: fetch /person/{id}, check if also_known_as
     (NFKC-normalized) contains artist's korean_name
  3. If exactly one candidate verifies -> accept
     If zero or multiple verify -> skip (logged as 'missing')
  4. Filter aliases that match other artists' primary names or aliases
     (drops group names like 'Red Velvet', '소녀시대')
  5. $addToSet aliases + normalized_aliases, $set tmdb_person_id
```

**Pre-filter**: only `type: "Actor"` documents are searched. Musicians and groups are skipped — TMDB doesn't have entries for them and naive search produces wrong matches (e.g. K-pop "Wendy" → American actress "Wendy Crawson").

**Korean-name verification** is the load-bearing safeguard. TMDB's mononym search returns up to 20 candidates ranked by global popularity; without verification, the first hit is rarely the right person.

**Group-name filter** rebuilds a blocked-set from the entire `artists` collection (all `english_name`, `korean_name`, and existing `aliases`) on each run. Anything in TMDB's `also_known_as` that matches an entry in this set gets dropped.

## Data shape

After backfill:

```js
{
  _id: ObjectId("..."),
  english_name: "Tiffany Young",
  korean_name: "티파니",
  type: ["Musician", "Actor"],
  // added by backfill:
  tmdb_person_id: 1471055,
  aliases: ["Stephanie Young Hwang", "황미영", "黃美永", "ティファニー", "Fany"],
  normalized_aliases: ["stephanieyounghwang", "황미영", "黃美永", "ティファニー", "fany"],
}
```

`aliases` and `normalized_aliases` are kept as flat `string[]` (no per-alias provenance metadata — kept simple).

## Stats from a real run

| Metric | Count |
|---|---|
| Actors processed | 1137 |
| Successfully aliased | 703 (62%) |
| Skipped (couldn't verify) | 434 (38%) |
| Average aliases per matched artist | 4.7 |
| Total runtime | ~60 seconds |

The 434 skipped are typically lesser-known actors not in TMDB, or actors whose TMDB record uses a Korean-name variant that doesn't NFKC-match the stored `korean_name`. Manual curation or relaxed-fallback strategies can close this gap.

## Files

```
scripts/
  backfill_aliases_from_tmdb.py   # main backfill script
  rollback_aliases_backfill.py    # undo a backfill run
requirements.txt                   # requests + pymongo
CLAUDE.md                          # context for Claude Code sessions
```
