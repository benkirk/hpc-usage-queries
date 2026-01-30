"""Tests for histogram collection during import."""

import pytest
from pathlib import Path
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from fs_scans.core.models import (
    Base,
    AccessHistogram,
    Directory,
    DirectoryStats,
    SizeHistogram,
)
from fs_scans.core.database import clear_engine_cache
from fs_scans.importers.importer import (
    ATIME_BUCKETS,
    SIZE_BUCKETS,
    classify_atime_bucket,
    classify_size_bucket,
    run_import,
)
from fs_scans.parsers.gpfs import GPFSParser


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def test_data_file():
    """Path to minimal test data file."""
    return Path(__file__).parent / "data" / "20260115_test_minimal.list.list_all.log"


# ============================================================================
# Bucket Classification Tests
# ============================================================================


class TestBucketClassification:
    """Tests for histogram bucket classification functions."""

    def test_classify_atime_bucket_recent(self):
        """Test classification of recently accessed files."""
        scan_date = datetime(2026, 1, 15)

        # 15 days old -> bucket 0 (< 1 Month)
        atime = datetime(2025, 12, 31)
        assert classify_atime_bucket(atime, scan_date) == 0

        # 45 days old -> bucket 1 (1-3 Months)
        atime = datetime(2025, 12, 1)
        assert classify_atime_bucket(atime, scan_date) == 1

    def test_classify_atime_bucket_old(self):
        """Test classification of old files."""
        scan_date = datetime(2026, 1, 15)

        # ~365 days (1 year) -> bucket 4 (1-2 Years)
        atime = datetime(2025, 1, 15)
        assert classify_atime_bucket(atime, scan_date) == 4

        # ~730 days (2 years) -> bucket 5 (2-3 Years)
        atime = datetime(2024, 1, 15)
        assert classify_atime_bucket(atime, scan_date) == 5

        # ~1095 days (3 years) -> bucket 6 (3-4 Years)
        atime = datetime(2023, 1, 15)
        assert classify_atime_bucket(atime, scan_date) == 6

    def test_classify_atime_bucket_none(self):
        """Test classification when atime is None."""
        scan_date = datetime(2026, 1, 15)
        # None should map to oldest bucket
        assert classify_atime_bucket(None, scan_date) == 9

    def test_classify_atime_bucket_boundaries(self):
        """Test bucket boundaries are correct."""
        scan_date = datetime(2026, 1, 15)

        # Test boundary between bucket 0 and 1 (30 days)
        atime = scan_date - timedelta(days=29)  # 29 days old
        assert classify_atime_bucket(atime, scan_date) == 0

        atime = scan_date - timedelta(days=31)  # 31 days old
        assert classify_atime_bucket(atime, scan_date) == 1

    def test_classify_size_bucket_small(self):
        """Test classification of small files."""
        # 512 bytes -> bucket 0 (0 - 1 KiB)
        assert classify_size_bucket(512) == 0

        # 5 KiB -> bucket 1 (1 KiB - 10 KiB)
        assert classify_size_bucket(5 * 1024) == 1

        # 50 KiB -> bucket 2 (10 KiB - 100 KiB)
        assert classify_size_bucket(50 * 1024) == 2

    def test_classify_size_bucket_large(self):
        """Test classification of large files."""
        # 1 MiB -> bucket 4 (1 MiB - 10 MiB)
        assert classify_size_bucket(1024 * 1024) == 4

        # 1 GiB -> bucket 7 (1 GiB - 10 GiB)
        assert classify_size_bucket(1024 * 1024 * 1024) == 7

        # 200 GiB -> bucket 9 (100 GiB+)
        assert classify_size_bucket(200 * 1024 * 1024 * 1024) == 9

    def test_classify_size_bucket_boundaries(self):
        """Test size bucket boundaries."""
        # 0 bytes -> bucket 0
        assert classify_size_bucket(0) == 0

        # 1023 bytes -> bucket 0
        assert classify_size_bucket(1023) == 0

        # 1024 bytes (1 KiB) -> bucket 1
        assert classify_size_bucket(1024) == 1

    def test_all_buckets_reachable(self):
        """Verify all 10 buckets are reachable."""
        scan_date = datetime(2026, 1, 15)

        # Test access time buckets
        atime_test_days = [15, 45, 120, 270, 500, 900, 1200, 1800, 2400, 3000]
        buckets_seen = set()
        for days in atime_test_days:
            atime = scan_date - timedelta(days=days)
            bucket = classify_atime_bucket(atime, scan_date)
            buckets_seen.add(bucket)

        # Should see all 10 buckets
        assert len(buckets_seen) == 10

        # Test size buckets
        size_test_bytes = [
            512, 5*1024, 50*1024, 500*1024,
            5*1024*1024, 50*1024*1024, 500*1024*1024,
            5*1024*1024*1024, 50*1024*1024*1024, 200*1024*1024*1024
        ]
        buckets_seen = set()
        for size in size_test_bytes:
            bucket = classify_size_bucket(size)
            buckets_seen.add(bucket)

        # Should see all 10 buckets
        assert len(buckets_seen) == 10


# ============================================================================
# Histogram Import Tests
# ============================================================================


class TestHistogramImport:
    """Tests for histogram collection during import."""

    def test_import_creates_histogram_tables(self, test_data_file, tmp_path):
        """Test that import creates histogram tables."""
        db_path = tmp_path / "test.db"

        # Run import
        run_import(
            input_file=test_data_file,
            parser=GPFSParser(),
            filesystem="test",
            db_path=db_path,
            replace=True,
        )

        # Clear engine cache to release database lock
        clear_engine_cache()

        # Check tables exist
        engine = create_engine(f"sqlite:///{db_path}")
        Session = sessionmaker(bind=engine)
        session = Session()

        # Verify tables exist
        result = session.execute(text("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name IN ('access_histogram', 'size_histogram')
            ORDER BY name
        """)).fetchall()

        assert len(result) == 2
        assert result[0][0] == "access_histogram"
        assert result[1][0] == "size_histogram"

        session.close()
        engine.dispose()

    def test_histogram_data_collected(self, test_data_file, tmp_path):
        """Test that histogram data is correctly collected."""
        db_path = tmp_path / "test.db"

        # Run import
        run_import(
            input_file=test_data_file,
            parser=GPFSParser(),
            filesystem="test",
            db_path=db_path,
            replace=True,
        )

        # Clear engine cache to release database lock
        clear_engine_cache()

        # Query histogram data
        engine = create_engine(f"sqlite:///{db_path}")
        Session = sessionmaker(bind=engine)
        session = Session()

        # Check access histogram has data
        atime_count = session.query(AccessHistogram).count()
        assert atime_count > 0

        # Check size histogram has data
        size_count = session.query(SizeHistogram).count()
        assert size_count > 0

        session.close()
        engine.dispose()

    def test_histogram_per_user(self, test_data_file, tmp_path):
        """Test that histograms are tracked per user."""
        db_path = tmp_path / "test.db"

        # Run import
        run_import(
            input_file=test_data_file,
            parser=GPFSParser(),
            filesystem="test",
            db_path=db_path,
            replace=True,
        )

        # Clear engine cache to release database lock
        clear_engine_cache()

        engine = create_engine(f"sqlite:///{db_path}")
        Session = sessionmaker(bind=engine)
        session = Session()

        # Get distinct users from access histogram
        users = session.execute(text("""
            SELECT DISTINCT owner_uid FROM access_histogram ORDER BY owner_uid
        """)).fetchall()

        # Should have 2 users (1000 and 2000)
        assert len(users) == 2
        assert users[0][0] == 1000
        assert users[1][0] == 2000

        session.close()
        engine.dispose()

    def test_histogram_totals_match_directory_stats(self, test_data_file, tmp_path):
        """Test that histogram totals match directory_stats."""
        db_path = tmp_path / "test.db"

        # Run import
        run_import(
            input_file=test_data_file,
            parser=GPFSParser(),
            filesystem="test",
            db_path=db_path,
            replace=True,
        )

        # Clear engine cache to release database lock
        clear_engine_cache()

        engine = create_engine(f"sqlite:///{db_path}")
        Session = sessionmaker(bind=engine)
        session = Session()

        # Get totals from different sources
        atime_totals = session.execute(text("""
            SELECT SUM(file_count), SUM(total_size) FROM access_histogram
        """)).fetchone()

        size_totals = session.execute(text("""
            SELECT SUM(file_count), SUM(total_size) FROM size_histogram
        """)).fetchone()

        dir_totals = session.execute(text("""
            SELECT SUM(file_count_nr), SUM(total_size_nr) FROM directory_stats
        """)).fetchone()

        # All should match
        assert atime_totals[0] == size_totals[0] == dir_totals[0]
        assert atime_totals[1] == size_totals[1] == dir_totals[1]

        # Should have 5 files total (from test data)
        assert atime_totals[0] == 5

        session.close()
        engine.dispose()

    def test_histogram_correct_buckets(self, test_data_file, tmp_path):
        """Test that files are placed in correct buckets."""
        db_path = tmp_path / "test.db"

        # Run import
        run_import(
            input_file=test_data_file,
            parser=GPFSParser(),
            filesystem="test",
            db_path=db_path,
            replace=True,
        )

        # Clear engine cache to release database lock
        clear_engine_cache()

        engine = create_engine(f"sqlite:///{db_path}")
        Session = sessionmaker(bind=engine)
        session = Session()

        # Check specific buckets for user 1000
        user_1000_atime = session.execute(text("""
            SELECT bucket_index, file_count FROM access_histogram
            WHERE owner_uid = 1000 ORDER BY bucket_index
        """)).fetchall()

        # Should have entries in buckets
        assert len(user_1000_atime) > 0

        # Verify bucket indices are in valid range (0-9)
        for bucket_idx, count in user_1000_atime:
            assert 0 <= bucket_idx <= 9
            assert count > 0

        session.close()
        engine.dispose()

    def test_empty_buckets_not_stored(self, test_data_file, tmp_path):
        """Test that empty buckets are not stored (sparse representation)."""
        db_path = tmp_path / "test.db"

        # Run import
        run_import(
            input_file=test_data_file,
            parser=GPFSParser(),
            filesystem="test",
            db_path=db_path,
            replace=True,
        )

        # Clear engine cache to release database lock
        clear_engine_cache()

        engine = create_engine(f"sqlite:///{db_path}")
        Session = sessionmaker(bind=engine)
        session = Session()

        # Get histogram entries per user
        # Each user should have fewer than 10 entries (not all buckets filled)
        user_1000_count = session.execute(text("""
            SELECT COUNT(*) FROM access_histogram WHERE owner_uid = 1000
        """)).scalar()

        user_2000_count = session.execute(text("""
            SELECT COUNT(*) FROM access_histogram WHERE owner_uid = 2000
        """)).scalar()

        # With 5 files total, we shouldn't have all 10 buckets filled per user
        assert user_1000_count < 10
        assert user_2000_count < 10

        session.close()
        engine.dispose()

    def test_filesystem_wide_aggregation(self, test_data_file, tmp_path):
        """Test filesystem-wide histogram aggregation."""
        db_path = tmp_path / "test.db"

        # Run import
        run_import(
            input_file=test_data_file,
            parser=GPFSParser(),
            filesystem="test",
            db_path=db_path,
            replace=True,
        )

        # Clear engine cache to release database lock
        clear_engine_cache()

        engine = create_engine(f"sqlite:///{db_path}")
        Session = sessionmaker(bind=engine)
        session = Session()

        # Aggregate across all users
        fs_wide = session.execute(text("""
            SELECT
                bucket_index,
                SUM(file_count) as total_files,
                SUM(total_size) as total_size
            FROM access_histogram
            GROUP BY bucket_index
            ORDER BY bucket_index
        """)).fetchall()

        # Should have some buckets with data
        assert len(fs_wide) > 0

        # Total files across all buckets should be 5
        total_files = sum(row[1] for row in fs_wide)
        assert total_files == 5

        session.close()
        engine.dispose()
