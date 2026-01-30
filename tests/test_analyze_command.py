"""Tests for fs_scans analyze command with histogram queries."""

import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from fs_scans.core.models import (
    Base,
    Directory,
    DirectoryStats,
    AccessHistogram,
    SizeHistogram,
    ScanMetadata,
)
from fs_scans.queries.histogram_common import (
    HistogramData,
    query_histogram_orm,
    aggregate_histograms_across_databases,
)
from fs_scans.queries.access_history import query_access_histogram_fast
from fs_scans.queries.file_size import (
    query_size_histogram_fast,
    compute_size_histogram_from_directory_stats,
)
from fs_scans.queries.query_engine import resolve_owner_filter
from fs_scans.importers.importer import ATIME_BUCKETS, SIZE_BUCKETS


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def histogram_engine():
    """Create an in-memory SQLite database with histogram tables."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def histogram_session(histogram_engine):
    """Create a session bound to the in-memory database."""
    Session = sessionmaker(bind=histogram_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def populated_histogram_session(histogram_session):
    """Session with sample histogram data.

    Creates:
    - Scan metadata
    - Access histogram with 3 buckets, 2 users
    - Size histogram with 3 buckets, 2 users
    - Directory hierarchy for fallback tests
    """
    # Add scan metadata
    scan_meta = ScanMetadata(
        scan_id=1,
        source_file="test_scan.log",
        scan_timestamp=datetime(2026, 1, 15),
        import_timestamp=datetime.now(),
        filesystem="test",
        total_directories=5,
        total_files=1000,
        total_size=1000000,
    )
    histogram_session.add(scan_meta)

    # Add access histogram data
    # Bucket 0: < 1 Month
    # Bucket 2: 3-6 Months
    # Bucket 5: 2-3 Years
    access_histograms = [
        AccessHistogram(owner_uid=1001, bucket_index=0, file_count=100, total_size=10000),
        AccessHistogram(owner_uid=1002, bucket_index=0, file_count=50, total_size=5000),
        AccessHistogram(owner_uid=1001, bucket_index=2, file_count=200, total_size=20000),
        AccessHistogram(owner_uid=1002, bucket_index=2, file_count=150, total_size=15000),
        AccessHistogram(owner_uid=1001, bucket_index=5, file_count=300, total_size=30000),
        AccessHistogram(owner_uid=1002, bucket_index=5, file_count=200, total_size=20000),
    ]
    histogram_session.add_all(access_histograms)

    # Add size histogram data
    # Bucket 2: 10 KiB - 100 KiB
    # Bucket 5: 10 MiB - 100 MiB
    # Bucket 8: 10 GiB - 100 GiB
    size_histograms = [
        SizeHistogram(owner_uid=1001, bucket_index=2, file_count=100, total_size=1000000),
        SizeHistogram(owner_uid=1002, bucket_index=2, file_count=80, total_size=800000),
        SizeHistogram(owner_uid=1001, bucket_index=5, file_count=50, total_size=50000000),
        SizeHistogram(owner_uid=1002, bucket_index=5, file_count=40, total_size=40000000),
        SizeHistogram(owner_uid=1001, bucket_index=8, file_count=10, total_size=100000000000),
        SizeHistogram(owner_uid=1002, bucket_index=8, file_count=5, total_size=50000000000),
    ]
    histogram_session.add_all(size_histograms)

    # Add directory structure for fallback tests
    dirs = [
        Directory(dir_id=1, parent_id=None, name="test", depth=1),
        Directory(dir_id=2, parent_id=1, name="users", depth=2),
        Directory(dir_id=3, parent_id=2, name="user1", depth=3),
        Directory(dir_id=4, parent_id=2, name="user2", depth=3),
    ]
    stats = [
        DirectoryStats(
            dir_id=1,
            file_count_nr=0,
            total_size_nr=0,
            file_count_r=1000,
            total_size_r=1000000,
            owner_uid=-1,
        ),
        DirectoryStats(
            dir_id=2,
            file_count_nr=0,
            total_size_nr=0,
            file_count_r=1000,
            total_size_r=1000000,
            owner_uid=-1,
        ),
        DirectoryStats(
            dir_id=3,
            file_count_nr=600,
            total_size_nr=600000,
            file_count_r=600,
            total_size_r=600000,
            owner_uid=1001,
        ),
        DirectoryStats(
            dir_id=4,
            file_count_nr=400,
            total_size_nr=400000,
            file_count_r=400,
            total_size_r=400000,
            owner_uid=1002,
        ),
    ]
    histogram_session.add_all(dirs + stats)
    histogram_session.commit()

    yield histogram_session


# ============================================================================
# Owner Filter Resolution Tests
# ============================================================================


class TestOwnerFilterResolution:
    """Tests for owner filter resolution."""

    def test_resolve_owner_none(self):
        """Test that no filter returns None."""
        result = resolve_owner_filter(owner_arg=None, mine_flag=False)
        assert result is None

    def test_resolve_owner_mine(self):
        """Test --mine flag returns current UID."""
        import os
        result = resolve_owner_filter(owner_arg=None, mine_flag=True)
        assert result == os.getuid()

    def test_resolve_owner_uid_string(self):
        """Test numeric UID string is converted to int."""
        result = resolve_owner_filter(owner_arg="1001", mine_flag=False)
        assert result == 1001
        assert isinstance(result, int)

    def test_resolve_owner_invalid_username(self):
        """Test invalid username raises SystemExit."""
        with pytest.raises(SystemExit):
            resolve_owner_filter(owner_arg="nonexistentuser123", mine_flag=False)


# ============================================================================
# HistogramData Class Tests
# ============================================================================


class TestHistogramData:
    """Tests for HistogramData class."""

    def test_initialization(self):
        """Test HistogramData initialization."""
        bucket_labels = [label for label, _ in ATIME_BUCKETS]
        scan_date = datetime(2026, 1, 15)

        hist = HistogramData(bucket_labels, scan_date)

        assert hist.scan_date == scan_date
        assert hist.bucket_labels == bucket_labels
        assert hist.total_files == 0
        assert hist.total_data == 0
        assert len(hist.buckets) == 10

    def test_add_bucket_data(self):
        """Test adding data to buckets."""
        bucket_labels = ["< 1 Month", "1-3 Months"]
        hist = HistogramData(bucket_labels)

        hist.add_bucket_data("< 1 Month", owner_uid=1001, file_count=100, total_size=10000)
        hist.add_bucket_data("< 1 Month", owner_uid=1002, file_count=50, total_size=5000)
        hist.add_bucket_data("1-3 Months", owner_uid=1001, file_count=200, total_size=20000)

        assert hist.total_files == 350
        assert hist.total_data == 35000
        assert hist.buckets["< 1 Month"]["files"] == 150
        assert hist.buckets["< 1 Month"]["data"] == 15000
        assert len(hist.buckets["< 1 Month"]["owners"]) == 2

    def test_format_count(self):
        """Test file count formatting."""
        # Test small numbers
        assert HistogramData._format_count(100) == "100"
        assert HistogramData._format_count(999) == "999"

        # Test thousands
        assert HistogramData._format_count(1000) == "1.0 K"
        assert HistogramData._format_count(5432) == "5.4 K"

        # Test millions
        assert HistogramData._format_count(1000000) == "1.0 M"
        assert HistogramData._format_count(2500000) == "2.5 M"


# ============================================================================
# ORM Query Tests
# ============================================================================


class TestHistogramORMQueries:
    """Tests for ORM-based histogram queries."""

    def test_query_access_histogram_all_users(self, populated_histogram_session):
        """Test querying access histogram without user filter."""
        result = query_histogram_orm(
            populated_histogram_session,
            histogram_type="access",
            owner_uid=None,
        )

        # Should have data for buckets 0, 2, 5
        assert len(result) == 3
        assert "< 1 Month" in result
        assert "3-6 Months" in result
        assert "2-3 Years" in result

        # Check bucket 0 has both users
        bucket_0 = result["< 1 Month"]
        assert 1001 in bucket_0
        assert 1002 in bucket_0
        assert bucket_0[1001] == (100, 10000)
        assert bucket_0[1002] == (50, 5000)

    def test_query_access_histogram_single_user(self, populated_histogram_session):
        """Test querying access histogram with user filter."""
        result = query_histogram_orm(
            populated_histogram_session,
            histogram_type="access",
            owner_uid=1001,
        )

        # Should have data for buckets 0, 2, 5 but only user 1001
        assert len(result) == 3

        # Check each bucket only has user 1001
        for bucket_label, owners in result.items():
            assert 1001 in owners
            assert 1002 not in owners

    def test_query_size_histogram_all_users(self, populated_histogram_session):
        """Test querying size histogram without user filter."""
        result = query_histogram_orm(
            populated_histogram_session,
            histogram_type="size",
            owner_uid=None,
        )

        # Should have data for buckets 2, 5, 8
        assert len(result) == 3
        assert "10 KiB - 100 KiB" in result
        assert "10 MiB - 100 MiB" in result
        assert "10 GiB - 100 GiB" in result

    def test_query_histogram_missing_table(self, histogram_session):
        """Test graceful handling of missing histogram tables."""
        from sqlalchemy import text

        # Drop histogram tables
        histogram_session.execute(text("DROP TABLE IF EXISTS access_histogram"))
        histogram_session.commit()

        result = query_histogram_orm(
            histogram_session,
            histogram_type="access",
            owner_uid=None,
        )

        # Should return None when table doesn't exist
        assert result is None


# ============================================================================
# Fast Path Query Tests
# ============================================================================


class TestAccessHistogramFastPath:
    """Tests for fast ORM-based access histogram queries."""

    def test_query_access_histogram_fast(self, populated_histogram_session):
        """Test fast access histogram query."""
        result = query_access_histogram_fast(populated_histogram_session)

        # Check totals
        total_files = 100 + 50 + 200 + 150 + 300 + 200
        total_size = 10000 + 5000 + 20000 + 15000 + 30000 + 20000
        assert result.total_files == total_files
        assert result.total_data == total_size

        # Check buckets have correct data
        assert result.buckets["< 1 Month"]["files"] == 150
        assert result.buckets["< 1 Month"]["data"] == 15000

    def test_query_access_histogram_fast_with_owner(self, populated_histogram_session):
        """Test fast access histogram query with owner filter."""
        result = query_access_histogram_fast(
            populated_histogram_session,
            owner_uid=1001,
        )

        # Check totals (only user 1001)
        assert result.total_files == 600  # 100 + 200 + 300
        assert result.total_data == 60000  # 10000 + 20000 + 30000


class TestSizeHistogramQueries:
    """Tests for size histogram queries."""

    def test_query_size_histogram_fast(self, populated_histogram_session):
        """Test fast size histogram query using ORM tables."""
        # Query using the low-level function
        result = query_size_histogram_fast(populated_histogram_session)

        # Should return histogram data dict structure
        assert isinstance(result, dict)
        assert "10 KiB - 100 KiB" in result
        assert "10 MiB - 100 MiB" in result
        assert "10 GiB - 100 GiB" in result

    def test_compute_size_histogram_fallback(self, populated_histogram_session):
        """Test approximate size histogram from directory_stats."""
        scan_date = datetime(2026, 1, 15)

        result = compute_size_histogram_from_directory_stats(
            populated_histogram_session,
            scan_date=scan_date,
            path_prefixes=None,
            min_depth=None,
            max_depth=None,
            owner_uid=None,
        )

        # Should return histogram data
        assert isinstance(result, dict)
        # Should have some buckets with data
        assert len(result) > 0


# ============================================================================
# Integration Tests
# ============================================================================


class TestAnalyzeCommandIntegration:
    """Integration tests for the analyze command."""

    def test_histogram_data_format_output(self, populated_histogram_session):
        """Test histogram output formatting."""
        bucket_labels = [label for label, _ in ATIME_BUCKETS]
        hist = HistogramData(bucket_labels, datetime(2026, 1, 15))

        # Add sample data
        hist.add_bucket_data("< 1 Month", 1001, 100, 10000)
        hist.add_bucket_data("< 1 Month", 1002, 50, 5000)
        hist.add_bucket_data("3-6 Months", 1001, 200, 20000)

        # Format output
        username_map = {1001: "user1", 1002: "user2"}
        output = hist.format_output(
            title="Test Histogram",
            directory="/test",
            username_map=username_map,
            top_n=10,
        )

        # Check output contains expected sections
        assert "Test Histogram" in output
        assert "Total Files:" in output
        assert "Total Data:" in output
        assert "< 1 Month" in output
        assert "user1" in output
        assert "user2" in output

    def test_end_to_end_access_histogram(self, populated_histogram_session):
        """Test complete access histogram query flow."""
        # Query histogram
        hist = query_access_histogram_fast(populated_histogram_session)

        # Verify structure
        assert hist.total_files > 0
        assert hist.total_data > 0
        assert len(hist.buckets) == 10

        # Format output
        username_map = {1001: "user1", 1002: "user2"}
        output = hist.format_output(
            "/test",
            username_map=username_map,
            top_n=10,
        )

        # Verify formatted output
        assert "Access Time Distribution" in output or "Directory" in output
        assert len(output) > 100  # Should have substantial content

    def test_missing_histogram_graceful_degradation(self, histogram_session):
        """Test that missing histogram tables are handled gracefully."""
        from sqlalchemy import text

        # Drop histogram tables to simulate missing tables
        histogram_session.execute(text("DROP TABLE IF EXISTS access_histogram"))
        histogram_session.commit()

        # Try to query non-existent histograms
        result = query_histogram_orm(
            histogram_session,
            histogram_type="access",
            owner_uid=None,
        )

        # Should return None instead of raising exception
        assert result is None


# ============================================================================
# Bucket Compatibility Tests
# ============================================================================


class TestBucketCompatibility:
    """Tests to ensure bucket definitions are consistent."""

    def test_atime_buckets_match_histogram(self):
        """Test that ATIME_BUCKETS has 10 buckets."""
        assert len(ATIME_BUCKETS) == 10

        # Verify first and last buckets
        assert ATIME_BUCKETS[0][0] == "< 1 Month"
        assert ATIME_BUCKETS[-1][0] == "7+ Years"

    def test_size_buckets_match_histogram(self):
        """Test that SIZE_BUCKETS has 10 buckets."""
        assert len(SIZE_BUCKETS) == 10

        # Verify first and last buckets
        assert SIZE_BUCKETS[0][0] == "0 - 1 KiB"
        assert SIZE_BUCKETS[-1][0] == "100 GiB+"

    def test_histogram_data_supports_all_buckets(self):
        """Test that HistogramData can handle all bucket types."""
        # Test with access buckets
        atime_labels = [label for label, _ in ATIME_BUCKETS]
        hist_atime = HistogramData(atime_labels)
        assert len(hist_atime.buckets) == 10

        # Test with size buckets
        size_labels = [label for label, _, _ in SIZE_BUCKETS]
        hist_size = HistogramData(size_labels)
        assert len(hist_size.buckets) == 10
