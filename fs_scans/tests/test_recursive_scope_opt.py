"""Parity tests for the ancestor-at-depth scoped-query optimization.

Every scoped consumer (owner-summary, group-summary, directory listing, the
access-history and file-size histograms) can answer a subtree query either via
the historical recursive `parent_id` CTE or via the denormalized `anc_d{k}`
equality predicate populated by pass2c. These tests assert the two paths return
byte-identical results — same rows, same aggregates, same ordering — and that a
scope deeper than SCOPE_MAX_DEPTH transparently falls back to the CTE.

The fast path engages only when the anc_d* columns are *populated* (pass2c run);
an otherwise-identical database without pass2c drives the recursive CTE. We build
the same tree both ways and compare.
"""

import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from fs_scans.core.models import Base, Directory, DirectoryStats, SCOPE_MAX_DEPTH
from fs_scans.importers.pass2c import pass2c_populate_ancestor_columns
from fs_scans.queries.query_engine import (
    query_owner_summary,
    query_group_summary,
    query_directories,
    resolve_scope,
    _ANC_COLUMNS_CACHE,
)
from fs_scans.queries.access_history import compute_access_history
from fs_scans.queries.file_size import compute_size_histogram_from_directory_stats


# ---------------------------------------------------------------------------
# Synthetic tree
# ---------------------------------------------------------------------------
#
# Rows: (dir_id, parent_id, name, depth, owner_uid, owner_gid, size_nr, files_nr)
# Layout (-1 owner = no direct files):
#
#   /fs(1) -> coll(2)
#       p1(3) -> u1(4,uid100) u2(5,uid200) sub(6) -> u1b(7,uid100)
#       p2(8) -> v1(9,uid300) v2(10,uid100)
#       + a deep chain under p1 reaching depth 14 (for the >MAX_DEPTH fallback)
_BASE_ROWS = [
    (1, None, "fs", 1, -1, -1, 0, 0),
    (2, 1, "coll", 2, -1, -1, 0, 0),
    (3, 2, "p1", 3, -1, -1, 0, 0),
    (4, 3, "u1", 4, 100, 10, 500, 5),
    (5, 3, "u2", 4, 200, 20, 300, 3),
    (6, 3, "sub", 4, -1, -1, 0, 0),
    (7, 6, "u1b", 5, 100, 10, 100, 1),
    (8, 2, "p2", 3, -1, -1, 0, 0),
    (9, 8, "v1", 4, 300, 30, 700, 7),
    (10, 8, "v2", 4, 100, 10, 50, 2),
]

# Deep chain dc4..dc14 hanging off p1 (dir_id 3): dir_ids 11..21, depths 4..14.
_DEEP_CHAIN = [
    (10 + i, (3 if i == 1 else 10 + i - 1), f"dc{3 + i}", 3 + i, 400, 40, 10, 1)
    for i in range(1, 12)
]
_ALL_ROWS = _BASE_ROWS + _DEEP_CHAIN

# Path of the depth-13 deep-chain node (dc13, dir_id 20): exceeds SCOPE_MAX_DEPTH.
_DEEP_SCOPE_DEPTH13 = "/fs/coll/p1/" + "/".join(f"dc{d}" for d in range(4, 14))


def _make_session(run_pass2c: bool):
    """Build an in-memory DB with the synthetic tree; optionally run pass2c."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    session.add_all(
        Directory(dir_id=r[0], parent_id=r[1], name=r[2], depth=r[3]) for r in _ALL_ROWS
    )
    session.add_all(
        DirectoryStats(
            dir_id=r[0],
            owner_uid=r[4],
            owner_gid=r[5],
            total_size_nr=r[6],
            file_count_nr=r[7],
            total_size_r=r[6],
            file_count_r=r[7],
            # Concrete atime so the access-history histogram lands in a real
            # bucket (the None branch hits an unrelated pre-existing bug).
            max_atime_nr=datetime(2025, 6, 1),
        )
        for r in _ALL_ROWS
    )
    session.commit()
    if run_pass2c:
        pass2c_populate_ancestor_columns(session)
    # The fast/slow decision is cached per engine; clear so each fixture decides
    # afresh against its own (populated or not) database.
    _ANC_COLUMNS_CACHE.clear()
    return session


@pytest.fixture
def fast_session():
    """Populated DB → anc_d{k} fast path engages for in-range scopes."""
    s = _make_session(run_pass2c=True)
    yield s
    s.close()
    _ANC_COLUMNS_CACHE.clear()


@pytest.fixture
def slow_session():
    """Unpopulated DB → recursive-CTE path always used."""
    s = _make_session(run_pass2c=False)
    yield s
    s.close()
    _ANC_COLUMNS_CACHE.clear()


_SCAN_DATE = datetime(2026, 1, 1)


def _access_repr(hist):
    return (
        hist.total_data,
        hist.total_files,
        {label: (b["data"], b["files"]) for label, b in hist.buckets.items()},
    )


# ---------------------------------------------------------------------------
# The fast path must actually engage (otherwise parity is vacuous)
# ---------------------------------------------------------------------------


def test_fast_path_engages_for_in_range_scope(fast_session, slow_session):
    resolved, use_fast = resolve_scope(fast_session, ["/fs/coll/p1"])
    assert resolved == [(3, 3)]
    assert use_fast is True

    # Same scope on an unpopulated DB falls back to the recursive CTE.
    resolved, use_fast = resolve_scope(slow_session, ["/fs/coll/p1"])
    assert resolved == [(3, 3)]
    assert use_fast is False


def test_deep_scope_forces_fallback_even_when_populated(fast_session):
    # dc13 sits at depth 13 > SCOPE_MAX_DEPTH (12): fast path declines.
    resolved, use_fast = resolve_scope(fast_session, [_DEEP_SCOPE_DEPTH13])
    assert resolved == [(20, 13)]
    assert use_fast is False
    assert SCOPE_MAX_DEPTH == 12  # guard: update _DEEP_SCOPE_DEPTH13 if this changes


# ---------------------------------------------------------------------------
# Parity: fast path vs recursive CTE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "prefixes",
    [
        ["/fs/coll/p1"],          # single prefix
        ["/fs/coll/p2"],
        ["/fs/coll/p1", "/fs/coll/p2"],  # multi-prefix OR
        ["/fs/coll"],             # whole-collection sub-root
        [_DEEP_SCOPE_DEPTH13],    # > SCOPE_MAX_DEPTH → fallback on both
    ],
)
def test_owner_summary_parity(fast_session, slow_session, prefixes):
    for sort_by in ("size", "files", "dirs"):
        fast = query_owner_summary(fast_session, path_prefixes=prefixes, sort_by=sort_by)
        slow = query_owner_summary(slow_session, path_prefixes=prefixes, sort_by=sort_by)
        assert fast == slow


def test_owner_summary_with_depth_filter_parity(fast_session, slow_session):
    # Depth filter keeps the directories join on the fast path; must still match.
    fast = query_owner_summary(
        fast_session, path_prefixes=["/fs/coll/p1"], min_depth=4, max_depth=4
    )
    slow = query_owner_summary(
        slow_session, path_prefixes=["/fs/coll/p1"], min_depth=4, max_depth=4
    )
    assert fast == slow
    # Sanity: p1's depth-4 single-owner dirs are u1(100), u2(200) and the deep
    # chain's first link dc4(400). sub(6) has no direct owner (-1, excluded).
    assert {r["owner_uid"] for r in fast} == {100, 200, 400}


@pytest.mark.parametrize(
    "prefixes",
    [["/fs/coll/p1"], ["/fs/coll/p1", "/fs/coll/p2"], [_DEEP_SCOPE_DEPTH13]],
)
def test_group_summary_parity(fast_session, slow_session, prefixes):
    for sort_by in ("size", "files", "dirs"):
        fast = query_group_summary(fast_session, path_prefixes=prefixes, sort_by=sort_by)
        slow = query_group_summary(slow_session, path_prefixes=prefixes, sort_by=sort_by)
        assert fast == slow


@pytest.mark.parametrize(
    "prefixes",
    [["/fs/coll/p1"], ["/fs/coll/p1", "/fs/coll/p2"], [_DEEP_SCOPE_DEPTH13]],
)
def test_list_directories_parity(fast_session, slow_session, prefixes):
    for sort_by in ("size_r", "files_r", "path"):
        fast = query_directories(fast_session, path_prefixes=prefixes, sort_by=sort_by)
        slow = query_directories(slow_session, path_prefixes=prefixes, sort_by=sort_by)
        # Identical rows AND ordering (incl. the dir_id tiebreaker).
        assert [d["path"] for d in fast] == [d["path"] for d in slow]
        assert fast == slow


def test_list_directories_tiebreaker_parity(fast_session, slow_session):
    # u1 and v2 both belong to uid 100; equal-size rows must order identically.
    fast = query_directories(fast_session, path_prefixes=["/fs/coll"], sort_by="size_r")
    slow = query_directories(slow_session, path_prefixes=["/fs/coll"], sort_by="size_r")
    assert [d["dir_id"] for d in fast] == [d["dir_id"] for d in slow]


@pytest.mark.parametrize(
    "prefixes",
    [["/fs/coll/p1"], ["/fs/coll/p1", "/fs/coll/p2"], [_DEEP_SCOPE_DEPTH13]],
)
def test_access_history_parity(fast_session, slow_session, prefixes):
    fast = compute_access_history(fast_session, _SCAN_DATE, path_prefixes=prefixes)
    slow = compute_access_history(slow_session, _SCAN_DATE, path_prefixes=prefixes)
    assert _access_repr(fast) == _access_repr(slow)


@pytest.mark.parametrize(
    "prefixes",
    [["/fs/coll/p1"], ["/fs/coll/p1", "/fs/coll/p2"], [_DEEP_SCOPE_DEPTH13]],
)
def test_size_histogram_parity(fast_session, slow_session, prefixes):
    fast = compute_size_histogram_from_directory_stats(
        fast_session, _SCAN_DATE, path_prefixes=prefixes
    )
    slow = compute_size_histogram_from_directory_stats(
        slow_session, _SCAN_DATE, path_prefixes=prefixes
    )
    assert fast == slow


def test_nonexistent_scope_returns_empty(fast_session):
    assert query_owner_summary(fast_session, path_prefixes=["/fs/coll/nope"]) == []
    assert query_directories(fast_session, path_prefixes=["/fs/coll/nope"]) == []
