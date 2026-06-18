"""Tests for the FsScanQueries facade and the CLI exporter/envelope layer.

The facade is the single source of truth shared by the ``fs-scans`` CLI and
external Python importers, so these tests exercise it directly against a small
on-disk SQLite collection and verify the envelope/exporter round-trips.
"""

import json
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from fs_scans.core.database import clear_engine_cache, set_data_dir
from fs_scans.core.models import (
    AccessHistogram as AccessHistogramRow,
    Base,
    Directory,
    DirectoryStats,
    GroupInfo,
    GroupSummary,
    OwnerSummary,
    ScanMetadata,
    SizeHistogram as SizeHistogramRow,
    UserInfo,
)
from fs_scans.queries.query_engine import (
    resolve_groupnames_across_databases,
    resolve_usernames_across_databases,
)
from fs_scans.queries.facade import (
    FsScanQueries,
    _collapse_prefixes,
    _is_collection_root,
)
from fs_scans.cli.core import (
    build_access_history,
    build_directories,
    build_group_summary,
    build_owner_summary,
)
from fs_scans.cli.core.output import ExporterRegistry, RichExporter

SCAN_DATE = datetime(2026, 1, 15)


@pytest.fixture
def collection(tmp_path):
    """Create an on-disk SQLite collection named ``testfs`` and point the data
    directory at it, so ``FsScanQueries(filesystems=["testfs"])`` resolves it.
    """
    db_path = tmp_path / "testfs.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()

    # Directory hierarchy: /tank (root) -> alice, bob, proj
    session.add_all([
        Directory(dir_id=1, parent_id=None, name="tank", depth=1),
        Directory(dir_id=2, parent_id=1, name="alice", depth=2),
        Directory(dir_id=3, parent_id=1, name="bob", depth=2),
        Directory(dir_id=4, parent_id=1, name="proj", depth=2),
    ])
    session.add_all([
        DirectoryStats(dir_id=1, file_count_r=600, total_size_r=6_000, owner_uid=-1,
                       owner_gid=-1, file_count_nr=0, total_size_nr=0),
        DirectoryStats(dir_id=2, file_count_r=300, total_size_r=3_000, owner_uid=1001,
                       owner_gid=2001, file_count_nr=300, total_size_nr=3_000,
                       max_atime_r=datetime(2025, 12, 1), max_atime_nr=datetime(2025, 12, 1)),
        DirectoryStats(dir_id=3, file_count_r=200, total_size_r=2_000, owner_uid=1002,
                       owner_gid=2001, file_count_nr=200, total_size_nr=2_000,
                       max_atime_r=datetime(2025, 6, 1), max_atime_nr=datetime(2025, 6, 1)),
        DirectoryStats(dir_id=4, file_count_r=100, total_size_r=1_000, owner_uid=None,
                       owner_gid=None, file_count_nr=100, total_size_nr=1_000,
                       max_atime_r=datetime(2024, 1, 1), max_atime_nr=datetime(2024, 1, 1)),
    ])
    # Pre-computed summaries (fast paths)
    session.add_all([
        OwnerSummary(owner_uid=1001, total_size=3_000, total_files=300, directory_count=1),
        OwnerSummary(owner_uid=1002, total_size=2_000, total_files=200, directory_count=1),
        GroupSummary(owner_gid=2001, total_size=5_000, total_files=500, directory_count=2),
    ])
    # Pre-computed histograms (bucket_index 0 = most recent)
    session.add_all([
        AccessHistogramRow(owner_uid=1001, bucket_index=0, file_count=300, total_size=3_000),
        AccessHistogramRow(owner_uid=1002, bucket_index=3, file_count=200, total_size=2_000),
        SizeHistogramRow(owner_uid=1001, bucket_index=1, file_count=300, total_size=3_000),
        SizeHistogramRow(owner_uid=1002, bucket_index=2, file_count=200, total_size=2_000),
    ])
    # Name maps + scan metadata
    session.add_all([
        UserInfo(uid=1001, username="alice"),
        UserInfo(uid=1002, username="bob"),
        GroupInfo(gid=2001, groupname="staff"),
        ScanMetadata(source_file="20260115_testfs.log", filesystem="testfs",
                     scan_timestamp=SCAN_DATE),
    ])
    session.commit()
    session.close()
    engine.dispose()

    set_data_dir(tmp_path)
    clear_engine_cache()
    yield "testfs"
    clear_engine_cache()
    set_data_dir(None)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------
def test_single_filesystem_resolution(collection):
    q = FsScanQueries(filesystems="testfs")
    assert q.filesystems == ["testfs"]
    assert q.multi_db is False


def test_all_resolution_discovers_collection(collection):
    q = FsScanQueries(filesystems="all")
    assert "testfs" in q.filesystems


# ---------------------------------------------------------------------------
# Owner / group summaries
# ---------------------------------------------------------------------------
def test_owner_summary_fast_path(collection):
    rows = FsScanQueries(filesystems="testfs").owner_summary(limit=10)
    assert [r["owner_uid"] for r in rows] == [1001, 1002]  # sorted by size desc
    assert rows[0]["total_size"] == 3_000
    assert all(r["filesystem"] == "testfs" for r in rows)


def test_owner_summary_sort_by_files(collection):
    rows = FsScanQueries(filesystems="testfs").owner_summary(sort_by="files")
    assert rows[0]["total_files"] >= rows[-1]["total_files"]


def test_group_summary_fast_path(collection):
    rows = FsScanQueries(filesystems="testfs").group_summary()
    assert rows[0]["owner_gid"] == 2001
    assert rows[0]["total_size"] == 5_000


# ---------------------------------------------------------------------------
# Directory listing
# ---------------------------------------------------------------------------
def test_list_directories_depth_filter(collection):
    rows = FsScanQueries(filesystems="testfs").list_directories(min_depth=2, limit=0)
    assert {r["path"] for r in rows} == {"/tank/alice", "/tank/bob", "/tank/proj"}


def test_list_directories_single_owner(collection):
    rows = FsScanQueries(filesystems="testfs").list_directories(
        min_depth=2, single_owner=True, limit=0
    )
    # proj has multiple owners (owner_uid NULL) -> excluded
    assert {r["path"] for r in rows} == {"/tank/alice", "/tank/bob"}


def test_list_directories_owner_filter(collection):
    rows = FsScanQueries(filesystems="testfs").list_directories(owner_id=1001, limit=0)
    assert [r["path"] for r in rows] == ["/tank/alice"]


# ---------------------------------------------------------------------------
# Histograms
# ---------------------------------------------------------------------------
def test_access_history_fast_path(collection):
    hist = FsScanQueries(filesystems="testfs").access_history()
    assert hist is not None
    assert hist["histogram_type"] == "access"
    assert hist["renderer"] == "histogram_data"
    assert hist["fast_path"] is True
    assert hist["total_files"] == 500
    assert hist["total_data"] == 5_000
    # bucket 0 ("< 1 Month") holds alice's 300 files
    assert hist["buckets"]["< 1 Month"]["files"] == 300


def test_file_size_histogram_fast_path(collection):
    hist = FsScanQueries(filesystems="testfs").file_size_histogram()
    assert hist is not None
    assert hist["histogram_type"] == "size"
    assert hist["total_files"] == 500


def test_access_history_path_filter_slow_path(collection):
    # A genuine SUB-path (not the collection root) still takes the slow,
    # on-the-fly path. '/tank/alice' is below the root, so it can't use the
    # pre-computed AccessHistogram table.
    hist = FsScanQueries(filesystems="testfs").access_history(path_prefixes=["/tank/alice"])
    assert hist is not None
    assert hist["fast_path"] is False
    assert hist["renderer"] == "access_histogram"


# ---------------------------------------------------------------------------
# Path-prefix scope resolution: collapse + whole-collection-root fast path
# ---------------------------------------------------------------------------
def test_collapse_prefixes_drops_descendants():
    assert _collapse_prefixes(["/mmm", "/mmm/parc", "/mmm/c3we"]) == ["/mmm"]
    assert _collapse_prefixes(["/mmm/parc", "/cisl"]) == ["/cisl", "/mmm/parc"]
    assert _collapse_prefixes(["/mmm", "/mmm"]) == ["/mmm"]  # dedupe


def test_is_collection_root():
    assert _is_collection_root("/mmm") is True
    assert _is_collection_root("/mmm/parc") is False
    assert _is_collection_root("") is False


def test_resolve_scope_root_prefix_uses_fast_path():
    q = FsScanQueries(filesystems=["mmm", "cisl"])
    # A whole-collection root -> unfiltered (None) over just the named collection.
    assert q._resolve_scope(["/gpfs/csfs1/mmm"]) == (["mmm"], None)
    # Nested children collapse to the root, then take the fast path.
    assert q._resolve_scope(["/glade/campaign/mmm", "/gpfs/csfs1/mmm/parc"]) == (["mmm"], None)


def test_resolve_scope_subpath_stays_filtered():
    q = FsScanQueries(filesystems=["mmm", "cisl"])
    assert q._resolve_scope(["/gpfs/csfs1/cisl/csg"]) == (["mmm", "cisl"], ["/cisl/csg"])


def test_resolve_scope_unconfigured_root_falls_back():
    # A root prefix naming a collection that isn't configured does NOT widen
    # scope — it falls back to the filtered path (which matches nothing).
    q = FsScanQueries(filesystems=["mmm"])
    assert q._resolve_scope(["/gpfs/csfs1/zzz"]) == (["mmm"], ["/zzz"])


def test_resolve_scope_no_prefixes():
    q = FsScanQueries(filesystems=["mmm", "cisl"])
    assert q._resolve_scope(None) == (["mmm", "cisl"], None)


def test_owner_summary_collection_root_matches_unfiltered(collection):
    # '/testfs' names the whole collection -> identical to the unfiltered
    # pre-computed fast path (the 20-150x speedup for lab-parent projects).
    q = FsScanQueries(filesystems="testfs")
    assert q.owner_summary(path_prefixes=["/testfs"]) == q.owner_summary()


def test_access_history_collection_root_uses_fast_path(collection):
    q = FsScanQueries(filesystems="testfs")
    hist = q.access_history(path_prefixes=["/testfs"])
    assert hist["fast_path"] is True
    assert hist["total_files"] == 500  # whole collection, from precomputed table


def test_list_directories_overlapping_prefixes_no_duplicates(collection):
    # Overlapping prefixes used to yield duplicate rows (one per matching
    # ancestor); _collapse_prefixes removes the redundancy.
    rows = FsScanQueries(filesystems="testfs").list_directories(
        path_prefixes=["/tank", "/tank/alice"], limit=0,
    )
    paths = [r["path"] for r in rows]
    assert len(paths) == len(set(paths))


# ---------------------------------------------------------------------------
# Name resolution
# ---------------------------------------------------------------------------
def test_resolve_usernames(collection):
    names = FsScanQueries(filesystems="testfs").resolve_usernames({1001, 1002})
    assert names == {1001: "alice", 1002: "bob"}


def test_resolve_groupnames(collection):
    names = FsScanQueries(filesystems="testfs").resolve_groupnames({2001})
    assert names == {2001: "staff"}


# ---------------------------------------------------------------------------
# Envelope builders + exporters
# ---------------------------------------------------------------------------
def test_owner_envelope_json_roundtrip(collection, capsys):
    q = FsScanQueries(filesystems="testfs")
    rows = q.owner_summary(limit=10)
    env = build_owner_summary(rows, filesystems=["testfs"],
                              name_map=q.resolve_usernames({1001, 1002}))
    assert env["kind"] == "fs_owner_summary"
    assert {c["key"] for c in env["columns"]} >= {"owner_uid", "total_size"}

    ExporterRegistry.resolve("json").emit(env)
    payload = json.loads(capsys.readouterr().out)
    assert payload["kind"] == "fs_owner_summary"
    assert payload["rows"][0]["owner_uid"] == 1001


def test_directories_envelope_json_roundtrip(collection, capsys):
    q = FsScanQueries(filesystems="testfs")
    rows = q.list_directories(min_depth=2, limit=0)
    env = build_directories(rows, filesystems=["testfs"])
    ExporterRegistry.resolve("json").emit(env)
    payload = json.loads(capsys.readouterr().out)
    assert payload["kind"] == "fs_directories"
    assert len(payload["rows"]) == 3


def test_access_history_envelope_json_serializable(collection, capsys):
    q = FsScanQueries(filesystems="testfs")
    hist = q.access_history()
    env = build_access_history(hist, filesystems=["testfs"], top_n=5)
    ExporterRegistry.resolve("json").emit(env)
    payload = json.loads(capsys.readouterr().out)
    assert payload["kind"] == "fs_access_history"
    assert payload["histogram"]["total_files"] == 500


# ---------------------------------------------------------------------------
# Cross-database name resolution (regression for the multi-db UID bug)
# ---------------------------------------------------------------------------
# UIDs/GIDs chosen far outside any local passwd/group range so pwd/grp lookups
# fail and the database-backed resolution is what's under test.
_REMOTE_UID = 9_400_001
_REMOTE_GID = 9_400_002


@pytest.fixture
def two_collections(tmp_path):
    """Two collections where the second owns the user/group, the first does not.

    Reproduces the bug: a path-filtered multi-db query fans out across every
    database, but only the owning collection's user_info/group_info knows its
    users. The first database queried must not short-circuit the search with a
    str(uid) placeholder.
    """
    for name, populate in (("a_first", False), ("b_owner", True)):
        engine = create_engine(f"sqlite:///{tmp_path / (name + '.db')}")
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        if populate:
            session.add_all([
                UserInfo(uid=_REMOTE_UID, username="claire"),
                GroupInfo(gid=_REMOTE_GID, groupname="climate"),
            ])
        else:
            # An unrelated user, so a_first.db's user_info is non-empty but
            # lacks _REMOTE_UID (the historical trigger for the early-stop bug).
            session.add(UserInfo(uid=1, username="root"))
        session.commit()
        session.close()
        engine.dispose()

    set_data_dir(tmp_path)
    clear_engine_cache()
    yield ["a_first", "b_owner"]  # order matters: owning db is second
    clear_engine_cache()
    set_data_dir(None)


def test_username_resolved_from_non_first_database(two_collections):
    names = resolve_usernames_across_databases([_REMOTE_UID], two_collections)
    assert names == {_REMOTE_UID: "claire"}


def test_groupname_resolved_from_non_first_database(two_collections):
    names = resolve_groupnames_across_databases([_REMOTE_GID], two_collections)
    assert names == {_REMOTE_GID: "climate"}


def test_unknown_uid_falls_back_to_str(two_collections):
    names = resolve_usernames_across_databases([_REMOTE_UID + 999], two_collections)
    assert names == {_REMOTE_UID + 999: str(_REMOTE_UID + 999)}


def test_facade_resolve_usernames_across_collections(two_collections):
    names = FsScanQueries(filesystems=two_collections).resolve_usernames({_REMOTE_UID})
    assert names == {_REMOTE_UID: "claire"}


def test_rich_exporter_renders_all_kinds(collection):
    """The rich exporter must handle every envelope kind without error."""
    q = FsScanQueries(filesystems="testfs")
    exporter = RichExporter()

    exporter.emit(build_owner_summary(
        q.owner_summary(), filesystems=["testfs"],
        name_map=q.resolve_usernames({1001, 1002})))
    exporter.emit(build_group_summary(
        q.group_summary(), filesystems=["testfs"],
        name_map=q.resolve_groupnames({2001})))
    exporter.emit(build_directories(
        q.list_directories(min_depth=2, limit=0), filesystems=["testfs"]))
    exporter.emit(build_access_history(q.access_history(), filesystems=["testfs"]))
    exporter.emit(build_access_history(
        q.access_history(path_prefixes=["/tank"]), filesystems=["testfs"]))
