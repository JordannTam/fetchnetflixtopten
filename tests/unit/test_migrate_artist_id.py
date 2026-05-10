"""Unit tests for scripts/migrate_artist_id_to_objectid.py.

Uses lightweight fake collection/database stand-ins instead of mongomock,
keeping the test runtime dependency-free. The fakes implement only the
subset of the PyMongo API the migration script touches:
  - db[name] -> Collection
  - collection.find(filter, projection) -> iterable of docs
  - collection.find_one(filter, projection) -> doc or None
  - collection.bulk_write(operations, ordered=False) -> result with
    `modified_count`
"""

from __future__ import annotations

from bson import ObjectId

from scripts.migrate_artist_id_to_objectid import (
    ARTISTS_COLLECTION,
    TARGET_COLLECTIONS,
    build_id_map,
    migrate_collection,
    preflight_check_no_mixed_types,
)


class _FakeBulkResult:
    def __init__(self, modified_count: int) -> None:
        self.modified_count = modified_count


class _FakeCollection:
    def __init__(self, docs: list[dict] | None = None) -> None:
        self._docs: list[dict] = list(docs or [])

    def find(self, filter_: dict, projection: dict | None = None):
        for doc in self._docs:
            if _matches(doc, filter_):
                yield dict(doc)  # copy so caller mutations don't leak

    def find_one(self, filter_: dict, projection: dict | None = None):
        for doc in self._docs:
            if _matches(doc, filter_):
                return dict(doc)
        return None

    def bulk_write(self, operations, ordered: bool = True):
        modified = 0
        for op in operations:
            doc_id = op._filter["_id"]
            update_set = op._update["$set"]
            for stored in self._docs:
                if stored["_id"] == doc_id:
                    stored.update(update_set)
                    modified += 1
                    break
        return _FakeBulkResult(modified)


class _FakeUpdateOne:
    """Minimal stand-in matching the UpdateOne attribute access in _FakeCollection."""

    def __init__(self, filter_: dict, update: dict) -> None:
        self._filter = filter_
        self._update = update


class _FakeDB:
    def __init__(self, collections: dict[str, _FakeCollection]) -> None:
        self._collections = collections

    def __getitem__(self, name: str) -> _FakeCollection:
        return self._collections.setdefault(name, _FakeCollection())


def _matches(doc: dict, filter_: dict) -> bool:
    for key, expected in filter_.items():
        if isinstance(expected, dict) and "$type" in expected:
            value = doc.get(key)
            if expected["$type"] == "string" and not isinstance(value, str):
                return False
            if expected["$type"] == "objectId" and not isinstance(value, ObjectId):
                return False
        else:
            if doc.get(key) != expected:
                return False
    return True


CONTENT_LINKS = TARGET_COLLECTIONS[0][0]
DRAMA_SCORE = TARGET_COLLECTIONS[1][0]
REVIEW_QUEUE = TARGET_COLLECTIONS[2][0]


class TestBuildIdMap:
    def test_returns_str_artist_id_to_objectid_mapping(self):
        oid_a = ObjectId()
        oid_b = ObjectId()
        db = _FakeDB({
            ARTISTS_COLLECTION: _FakeCollection([
                {"_id": oid_a, "artist_id": "1297"},
                {"_id": oid_b, "artist_id": 452},  # numeric legacy id coerced to str
            ]),
        })

        mapping = build_id_map(db)

        assert mapping == {"1297": oid_a, "452": oid_b}

    def test_skips_artists_without_legacy_id(self):
        oid = ObjectId()
        db = _FakeDB({
            ARTISTS_COLLECTION: _FakeCollection([
                {"_id": ObjectId()},  # no artist_id at all
                {"_id": oid, "artist_id": "abc"},
            ]),
        })

        mapping = build_id_map(db)

        assert mapping == {"abc": oid}

    def test_duplicate_legacy_ids_keep_last_seen(self):
        first = ObjectId()
        second = ObjectId()
        db = _FakeDB({
            ARTISTS_COLLECTION: _FakeCollection([
                {"_id": first, "artist_id": "dup"},
                {"_id": second, "artist_id": "dup"},
            ]),
        })

        mapping = build_id_map(db)

        # Last write wins (warning is logged at runtime; not asserted here)
        assert mapping == {"dup": second}


class TestPreflightCheck:
    def test_passes_when_only_strings(self):
        db = _FakeDB({
            CONTENT_LINKS: _FakeCollection([{"_id": ObjectId(), "artist_id": "a"}]),
            DRAMA_SCORE: _FakeCollection([{"_id": ObjectId(), "artist_id": "b"}]),
            REVIEW_QUEUE: _FakeCollection([{"_id": ObjectId(), "candidate_artist_id": "c"}]),
        })
        assert preflight_check_no_mixed_types(db) is True

    def test_passes_when_only_objectids(self):
        db = _FakeDB({
            CONTENT_LINKS: _FakeCollection([{"_id": ObjectId(), "artist_id": ObjectId()}]),
            DRAMA_SCORE: _FakeCollection([{"_id": ObjectId(), "artist_id": ObjectId()}]),
            REVIEW_QUEUE: _FakeCollection(
                [{"_id": ObjectId(), "candidate_artist_id": ObjectId()}]
            ),
        })
        assert preflight_check_no_mixed_types(db) is True

    def test_passes_when_collections_empty(self):
        db = _FakeDB({
            CONTENT_LINKS: _FakeCollection(),
            DRAMA_SCORE: _FakeCollection(),
            REVIEW_QUEUE: _FakeCollection(),
        })
        assert preflight_check_no_mixed_types(db) is True

    def test_fails_when_any_collection_has_mixed_types(self):
        db = _FakeDB({
            CONTENT_LINKS: _FakeCollection([
                {"_id": ObjectId(), "artist_id": "legacy"},
                {"_id": ObjectId(), "artist_id": ObjectId()},  # mixed!
            ]),
            DRAMA_SCORE: _FakeCollection(),
            REVIEW_QUEUE: _FakeCollection(),
        })
        assert preflight_check_no_mixed_types(db) is False


class TestMigrateCollection:
    def test_dry_run_does_not_mutate(self):
        oid = ObjectId()
        doc_id = ObjectId()
        coll = _FakeCollection([{"_id": doc_id, "artist_id": "1297"}])
        db = _FakeDB({CONTENT_LINKS: coll})

        # Patch UpdateOne in the migration module to use our fake.
        import scripts.migrate_artist_id_to_objectid as mig
        original = mig.UpdateOne
        mig.UpdateOne = _FakeUpdateOne
        try:
            stats = migrate_collection(
                db, CONTENT_LINKS, "artist_id", {"1297": oid}, apply=False,
            )
        finally:
            mig.UpdateOne = original

        assert stats["candidates"] == 1
        assert stats["to_update"] == 1
        assert stats["modified"] == 0
        # Source doc unchanged
        assert coll._docs[0]["artist_id"] == "1297"

    def test_apply_rewrites_string_to_objectid(self):
        oid = ObjectId()
        doc_id = ObjectId()
        coll = _FakeCollection([{"_id": doc_id, "artist_id": "1297"}])
        db = _FakeDB({CONTENT_LINKS: coll})

        import scripts.migrate_artist_id_to_objectid as mig
        original = mig.UpdateOne
        mig.UpdateOne = _FakeUpdateOne
        try:
            stats = migrate_collection(
                db, CONTENT_LINKS, "artist_id", {"1297": oid}, apply=True,
            )
        finally:
            mig.UpdateOne = original

        assert stats["modified"] == 1
        assert coll._docs[0]["artist_id"] == oid

    def test_unknown_legacy_id_is_skipped(self):
        coll = _FakeCollection([
            {"_id": ObjectId(), "artist_id": "unknown-legacy-id"},
        ])
        db = _FakeDB({CONTENT_LINKS: coll})

        import scripts.migrate_artist_id_to_objectid as mig
        original = mig.UpdateOne
        mig.UpdateOne = _FakeUpdateOne
        try:
            stats = migrate_collection(
                db, CONTENT_LINKS, "artist_id", {"different": ObjectId()}, apply=True,
            )
        finally:
            mig.UpdateOne = original

        assert stats["candidates"] == 1
        assert stats["to_update"] == 0
        assert stats["skipped_unknown"] == 1
        assert stats["modified"] == 0
        # Unchanged
        assert coll._docs[0]["artist_id"] == "unknown-legacy-id"

    def test_empty_collection_returns_zero_stats(self):
        db = _FakeDB({CONTENT_LINKS: _FakeCollection()})
        stats = migrate_collection(
            db, CONTENT_LINKS, "artist_id", {"any": ObjectId()}, apply=True,
        )
        assert stats["candidates"] == 0
        assert stats["to_update"] == 0
        assert stats["modified"] == 0
