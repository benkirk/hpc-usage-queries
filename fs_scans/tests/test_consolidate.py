"""Tests for the SQLite → PostgreSQL consolidation step.

The encoder/stream tests are pure-Python (no database) and always run — they
guard the COPY text-format encoding, including the integer-epoch DateTime
sentinel that SQLite's dynamic typing allows.

The round-trip + swap integration test requires a reachable PostgreSQL and is
opt-in via FS_SCAN_RUN_PG_TESTS=1 (it creates and drops a throwaway schema).
"""

import os

import pytest

from fs_scans.consolidate.consolidator import (
    _CopyStream,
    _encode_datetime,
    _encode_field,
    _make_encoders,
)
from fs_scans.core.models import DirectoryStats, Directory


# --------------------------------------------------------------------------- #
# Pure-Python encoder tests (no database)
# --------------------------------------------------------------------------- #

class TestCopyEncoding:
    def test_encode_field_null_and_scalars(self):
        assert _encode_field(None) == r"\N"
        assert _encode_field(5) == "5"
        assert _encode_field(-1) == "-1"

    def test_encode_field_escapes_text(self):
        # backslash, tab, newline, carriage return must be escaped for COPY text
        assert _encode_field("a\tb\nc\rd\\e") == "a\\tb\\nc\\rd\\\\e"

    def test_encode_datetime_text_passthrough(self):
        assert _encode_datetime("2026-06-13 22:34:45") == "2026-06-13 22:34:45"

    def test_encode_datetime_null(self):
        assert _encode_datetime(None) == r"\N"

    def test_encode_datetime_integer_sentinel(self):
        # SQLite stores 0 in a DateTime column (MAX over no rows); map to epoch
        # so it keeps sorting before every real timestamp.
        assert _encode_datetime(0) == "1970-01-01 00:00:00"

    def test_encode_datetime_integer_epoch(self):
        # 1700000000 = 2023-11-14 22:13:20 UTC
        assert _encode_datetime(1700000000) == "2023-11-14 22:13:20"

    def test_make_encoders_picks_datetime_for_atime_columns(self):
        cols = [c.name for c in DirectoryStats.__table__.columns]
        encs = _make_encoders(DirectoryStats)
        by_name = dict(zip(cols, encs))
        assert by_name["max_atime_nr"] is _encode_datetime
        assert by_name["max_atime_r"] is _encode_datetime
        assert by_name["file_count_r"] is _encode_field
        assert by_name["owner_uid"] is _encode_field

    def test_copystream_full_read_and_chunked_read_agree(self):
        rows = [
            (1, None, "root", 1),
            (2, 1, "a\tb", 2),       # embedded tab → escaped
            (3, 2, "c", 3),
        ]
        encoders = _make_encoders(Directory)
        expected = (
            "1\t\\N\troot\t1\n"
            "2\t1\ta\\tb\t2\n"
            "3\t2\tc\t3\n"
        )
        # full read
        assert _CopyStream(rows, encoders).read() == expected
        # chunked read (simulates copy_expert's read(size) loop)
        s = _CopyStream(rows, encoders)
        out = ""
        while chunk := s.read(7):
            out += chunk
        assert out == expected


# --------------------------------------------------------------------------- #
# Opt-in live round-trip + swap (requires reachable PostgreSQL)
# --------------------------------------------------------------------------- #

pg = pytest.mark.skipif(
    os.getenv("FS_SCAN_RUN_PG_TESTS") != "1",
    reason="set FS_SCAN_RUN_PG_TESTS=1 (and FS_SCAN_PG_* / postgres backend) to run",
)


@pg
def test_roundtrip_and_swap(tmp_path):
    """Build a tiny SQLite db, consolidate into a throwaway schema, verify, drop."""
    import os as _os

    from sqlalchemy import text

    import fs_scans.core.config as cfg
    from fs_scans.consolidate.consolidator import consolidate_sqlite_to_postgres
    from fs_scans.core.database import clear_engine_cache, get_engine, get_session
    from fs_scans.core.models import Base
    from fs_scans.queries.query_engine import query_directories

    # Unique throwaway collection name so we never touch real data.
    collection = f"pytest_tmp_{_os.getpid()}"

    # --- build a small SQLite source with a DateTime sentinel + a real one ---
    cfg.FsScanConfig.DB_BACKEND = "sqlite"
    clear_engine_cache()
    src = tmp_path / f"{collection}.db"
    eng = get_engine(collection, db_path=src)
    Base.metadata.create_all(eng)
    sess = get_session(collection, db_path=src)
    sess.execute(text("INSERT INTO directories (dir_id, parent_id, name, depth) VALUES "
                      "(1, NULL, 'root', 1), (2, 1, 'child', 2)"))
    sess.execute(text("INSERT INTO directory_stats (dir_id, file_count_nr, total_size_nr, "
                      "dir_count_nr, file_count_r, total_size_r, dir_count_r, owner_uid, "
                      "owner_gid, max_atime_r) VALUES "
                      "(1, 0, 0, 1, 5, 100, 1, 1000, 2000, '2026-01-01 00:00:00'), "
                      "(2, 5, 100, 0, 5, 100, 0, 1000, 2000, 0)"))  # 0 = integer sentinel
    sess.commit()
    sess.close()
    clear_engine_cache()

    # --- consolidate into postgres ---
    cfg.FsScanConfig.DB_BACKEND = "postgres"
    clear_engine_cache()
    try:
        result = consolidate_sqlite_to_postgres(src, collection, swap=True)
        assert result["rows"]["directories"] == 2
        assert result["rows"]["directory_stats"] == 2

        psess = get_session(collection)
        assert psess.execute(text("SELECT COUNT(*) FROM directories")).scalar() == 2
        # the integer-0 sentinel became the epoch timestamp
        epoch = psess.execute(
            text("SELECT max_atime_r FROM directory_stats WHERE dir_id = 2")
        ).scalar()
        assert str(epoch).startswith("1970-01-01")
        # query path works against the live schema
        rows = query_directories(psess, limit=10, sort_by="size_r")
        assert len(rows) == 2
        psess.close()
    finally:
        # cleanup: drop the throwaway schemas
        eng = get_engine(collection, schema="public")
        with eng.begin() as conn:
            for s in (collection, f"{collection}_staging", f"{collection}_old"):
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{s}" CASCADE'))
        clear_engine_cache()
