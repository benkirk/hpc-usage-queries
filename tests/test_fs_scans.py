"""Tests for fs_scans query system."""

import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from fs_scans.core.models import Base, Directory, DirectoryStats
from fs_scans.core.database import get_engine, clear_engine_cache
from fs_scans.core.query_builder import DirectoryQueryBuilder, QueryResult
from fs_scans.cli.common import parse_size, parse_file_count
from fs_scans.queries.query_engine import normalize_path


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def fs_scan_engine():
    """Create an in-memory SQLite database for fs_scans testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def fs_scan_session(fs_scan_engine):
    """Create a session bound to the in-memory fs_scans database."""
    Session = sessionmaker(bind=fs_scan_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def populated_session(fs_scan_session):
    """Session with sample directory data.

    Creates hierarchy:
        /gpfs (depth 1)
        /gpfs/csfs1 (depth 2)
        /gpfs/csfs1/cisl (depth 3)
        /gpfs/csfs1/cisl/userA (depth 4, single owner 12345)
        /gpfs/csfs1/cisl/userB (depth 4, single owner 67890)
    """
    dirs = [
        Directory(dir_id=1, parent_id=None, name="gpfs", depth=1),
        Directory(dir_id=2, parent_id=1, name="csfs1", depth=2),
        Directory(dir_id=3, parent_id=2, name="cisl", depth=3),
        Directory(dir_id=4, parent_id=3, name="userA", depth=4),
        Directory(dir_id=5, parent_id=3, name="userB", depth=4),
    ]
    stats = [
        DirectoryStats(
            dir_id=1, file_count_r=1000, total_size_r=1000000, owner_uid=-1
        ),
        DirectoryStats(
            dir_id=2, file_count_r=900, total_size_r=900000, owner_uid=-1
        ),
        DirectoryStats(
            dir_id=3, file_count_r=800, total_size_r=800000, owner_uid=-1
        ),
        DirectoryStats(
            dir_id=4, file_count_r=500, total_size_r=500000, owner_uid=12345
        ),
        DirectoryStats(
            dir_id=5, file_count_r=300, total_size_r=300000, owner_uid=67890
        ),
    ]
    fs_scan_session.add_all(dirs + stats)
    fs_scan_session.commit()
    yield fs_scan_session


# ============================================================================
# Engine Caching Tests
# ============================================================================


class TestEngineCaching:
    """Tests for engine caching in database.py."""

    def test_cache_returns_same_engine(self, tmp_path, monkeypatch):
        """Verify same engine returned for same path."""
        monkeypatch.setenv("FS_SCAN_DATA_DIR", str(tmp_path))
        clear_engine_cache()

        engine1 = get_engine("test")
        engine2 = get_engine("test")

        assert engine1 is engine2

    def test_cache_different_paths(self, tmp_path, monkeypatch):
        """Verify different engines for different filesystems."""
        monkeypatch.setenv("FS_SCAN_DATA_DIR", str(tmp_path))
        clear_engine_cache()

        engine_asp = get_engine("asp")
        engine_cisl = get_engine("cisl")

        assert engine_asp is not engine_cisl

    def test_clear_cache(self, tmp_path, monkeypatch):
        """Verify cache clearing works."""
        monkeypatch.setenv("FS_SCAN_DATA_DIR", str(tmp_path))
        clear_engine_cache()

        engine1 = get_engine("test")
        clear_engine_cache()
        engine2 = get_engine("test")

        assert engine1 is not engine2


# ============================================================================
# DirectoryQueryBuilder Tests
# ============================================================================


class TestDirectoryQueryBuilder:
    """Tests for DirectoryQueryBuilder class."""

    def test_build_basic_query(self):
        """Test basic query without filters."""
        builder = DirectoryQueryBuilder()
        result = builder.build()

        assert isinstance(result, QueryResult)
        assert "SELECT" in result.sql
        assert "FROM directories" in result.sql
        assert "ORDER BY" in result.sql
        assert result.params == {"limit": None} or "limit" not in result.params

    def test_depth_filter_min_only(self):
        """Test depth range filtering with min only."""
        builder = DirectoryQueryBuilder()
        result = builder.with_depth_range(min_depth=4).build()

        assert "d.depth >= :min_depth" in result.sql
        assert result.params["min_depth"] == 4
        assert "max_depth" not in result.params

    def test_depth_filter_max_only(self):
        """Test depth range filtering with max only."""
        builder = DirectoryQueryBuilder()
        result = builder.with_depth_range(max_depth=6).build()

        assert "d.depth <= :max_depth" in result.sql
        assert result.params["max_depth"] == 6
        assert "min_depth" not in result.params

    def test_depth_filter_range(self):
        """Test depth range filtering with both min and max."""
        builder = DirectoryQueryBuilder()
        result = builder.with_depth_range(min_depth=4, max_depth=6).build()

        assert "d.depth >= :min_depth" in result.sql
        assert "d.depth <= :max_depth" in result.sql
        assert result.params["min_depth"] == 4
        assert result.params["max_depth"] == 6

    def test_single_owner_filter(self):
        """Test single owner filtering."""
        builder = DirectoryQueryBuilder()
        result = builder.with_single_owner().build()

        assert "s.owner_uid IS NOT NULL" in result.sql
        assert "s.owner_uid != -1" in result.sql

    def test_owner_filter(self):
        """Test specific owner filtering."""
        builder = DirectoryQueryBuilder()
        result = builder.with_owner(12345).build()

        assert "s.owner_uid = :owner_id" in result.sql
        assert result.params["owner_id"] == 12345

    def test_accessed_before_filter(self):
        """Test accessed before date filtering."""
        builder = DirectoryQueryBuilder()
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = builder.with_accessed_before(dt).build()

        assert "s.max_atime_r < :accessed_before" in result.sql
        assert result.params["accessed_before"] == "2024-01-15 10:30:00"

    def test_accessed_after_filter(self):
        """Test accessed after date filtering."""
        builder = DirectoryQueryBuilder()
        dt = datetime(2024, 6, 1, 0, 0, 0)
        result = builder.with_accessed_after(dt).build()

        assert "s.max_atime_r > :accessed_after" in result.sql
        assert result.params["accessed_after"] == "2024-06-01 00:00:00"

    def test_leaves_only_filter(self):
        """Test leaves only filtering."""
        builder = DirectoryQueryBuilder()
        result = builder.with_leaves_only().build()

        assert "NOT EXISTS" in result.sql
        assert "child.parent_id = d.dir_id" in result.sql

    def test_name_patterns_glob(self):
        """Test GLOB name pattern matching."""
        builder = DirectoryQueryBuilder()
        result = builder.with_name_patterns(["*scratch*", "*tmp*"]).build()

        assert "d.name GLOB :name_pattern_0" in result.sql
        assert "d.name GLOB :name_pattern_1" in result.sql
        assert " OR " in result.sql
        assert result.params["name_pattern_0"] == "*scratch*"
        assert result.params["name_pattern_1"] == "*tmp*"

    def test_name_patterns_like_ignore_case(self):
        """Test LIKE name pattern matching (case-insensitive)."""
        builder = DirectoryQueryBuilder()
        result = builder.with_name_patterns(["*TMP*"], ignore_case=True).build()

        assert "d.name LIKE :name_pattern_0" in result.sql
        assert result.params["name_pattern_0"] == "%TMP%"

    def test_name_patterns_empty_list(self):
        """Test empty patterns list is no-op."""
        builder = DirectoryQueryBuilder()
        result = builder.with_name_patterns([]).build()

        assert "GLOB" not in result.sql
        assert "LIKE" not in result.sql

    def test_path_prefix_cte_single(self):
        """Test single path prefix generates descendants CTE."""
        builder = DirectoryQueryBuilder()
        result = builder.with_path_prefix_ids([42]).build()

        assert "WITH RECURSIVE" in result.sql
        assert "ancestors AS" in result.sql
        assert "descendants AS" in result.sql
        assert "FROM descendants" in result.sql
        assert result.params["ancestor_id_0"] == 42

    def test_path_prefix_cte_multiple(self):
        """Test multiple path prefixes generate UNION'd descendants CTE."""
        builder = DirectoryQueryBuilder()
        result = builder.with_path_prefix_ids([42, 99]).build()

        assert "WITH RECURSIVE" in result.sql
        assert "ancestors AS" in result.sql
        assert "IN (:ancestor_id_0, :ancestor_id_1)" in result.sql
        assert "descendants AS" in result.sql
        assert result.params["ancestor_id_0"] == 42
        assert result.params["ancestor_id_1"] == 99

    def test_sort_options(self):
        """Test various sort options."""
        test_cases = [
            ("size_r", "s.total_size_r DESC"),
            ("size_nr", "s.total_size_nr DESC"),
            ("files_r", "s.file_count_r DESC"),
            ("files_nr", "s.file_count_nr DESC"),
            ("atime_r", "s.max_atime_r DESC"),
            ("path", "d.depth ASC, d.name ASC"),
            ("depth", "d.depth DESC"),
        ]
        for sort_by, expected_clause in test_cases:
            builder = DirectoryQueryBuilder()
            result = builder.with_sort(sort_by).build()
            assert expected_clause in result.sql, f"Failed for sort_by={sort_by}"

    def test_limit(self):
        """Test result limit."""
        builder = DirectoryQueryBuilder()
        result = builder.with_limit(50).build()

        assert "LIMIT :limit" in result.sql
        assert result.params["limit"] == 50

    def test_fluent_chaining(self):
        """Test fluent interface chaining."""
        builder = DirectoryQueryBuilder()
        result = (
            builder.with_depth_range(min_depth=4)
            .with_single_owner()
            .with_sort("files_r")
            .with_limit(50)
            .build()
        )

        assert "d.depth >= :min_depth" in result.sql
        assert "s.owner_uid IS NOT NULL" in result.sql
        assert "ORDER BY s.file_count_r DESC" in result.sql
        assert "LIMIT :limit" in result.sql
        assert result.params["min_depth"] == 4
        assert result.params["limit"] == 50

    def test_reset(self):
        """Test builder reset."""
        builder = DirectoryQueryBuilder()
        builder.with_depth_range(min_depth=4).with_limit(100)

        # Reset and build again
        builder.reset()
        result = builder.build()

        # Should have no conditions or limit
        assert "min_depth" not in result.params
        assert "limit" not in result.params or result.params.get("limit") is None

    def test_multiple_conditions_combined_with_and(self):
        """Test multiple conditions are combined with AND."""
        builder = DirectoryQueryBuilder()
        result = (
            builder.with_depth_range(min_depth=4)
            .with_single_owner()
            .with_owner(12345)
            .build()
        )

        # All conditions should be AND'd
        assert " AND " in result.sql
        assert "d.depth >= :min_depth" in result.sql
        assert "s.owner_uid IS NOT NULL" in result.sql
        assert "s.owner_uid = :owner_id" in result.sql


# ============================================================================
# Parse Helper Tests
# ============================================================================


class TestParseSize:
    """Tests for parse_size helper."""

    def test_plain_integer(self):
        assert parse_size("1024") == 1024

    def test_zero(self):
        assert parse_size("0") == 0

    def test_bytes_unit(self):
        assert parse_size("512B") == 512

    def test_si_units(self):
        assert parse_size("1KB") == 1000
        assert parse_size("1MB") == 1000**2
        assert parse_size("1GB") == 1000**3
        assert parse_size("1TB") == 1000**4
        assert parse_size("1PB") == 1000**5

    def test_binary_units(self):
        assert parse_size("1KiB") == 1024
        assert parse_size("1MiB") == 1024**2
        assert parse_size("1GiB") == 1024**3
        assert parse_size("1TiB") == 1024**4
        assert parse_size("1PiB") == 1024**5

    def test_shorthand_binary(self):
        assert parse_size("1K") == 1024
        assert parse_size("1M") == 1024**2
        assert parse_size("1G") == 1024**3
        assert parse_size("1T") == 1024**4
        assert parse_size("1P") == 1024**5

    def test_case_insensitive(self):
        assert parse_size("1gib") == 1024**3
        assert parse_size("1GIB") == 1024**3
        assert parse_size("500mb") == 500 * 1000**2

    def test_fractional_values(self):
        assert parse_size("0.5GiB") == int(0.5 * 1024**3)
        assert parse_size("1.5MB") == int(1.5 * 1000**2)

    def test_whitespace_tolerance(self):
        assert parse_size("  100MB  ") == 100 * 1000**2

    def test_invalid_unit_raises(self):
        with pytest.raises(Exception):
            parse_size("100XB")

    def test_invalid_format_raises(self):
        with pytest.raises(Exception):
            parse_size("abc")


class TestParseFileCount:
    """Tests for parse_file_count helper."""

    def test_plain_integer(self):
        assert parse_file_count("500") == 500

    def test_zero(self):
        assert parse_file_count("0") == 0

    def test_k_multiplier(self):
        assert parse_file_count("1K") == 1000
        assert parse_file_count("10K") == 10000

    def test_m_multiplier(self):
        assert parse_file_count("1M") == 1000000
        assert parse_file_count("10M") == 10000000

    def test_case_insensitive(self):
        assert parse_file_count("5k") == 5000
        assert parse_file_count("2m") == 2000000

    def test_fractional_values(self):
        assert parse_file_count("1.5K") == 1500

    def test_whitespace_tolerance(self):
        assert parse_file_count("  100K  ") == 100000

    def test_invalid_unit_raises(self):
        with pytest.raises(Exception):
            parse_file_count("100G")

    def test_invalid_format_raises(self):
        with pytest.raises(Exception):
            parse_file_count("abc")


# ============================================================================
# Size & File Count Query Builder Tests
# ============================================================================


class TestSizeRangeFilter:
    """Tests for with_size_range query builder method."""

    def test_min_size_only(self):
        builder = DirectoryQueryBuilder()
        result = builder.with_size_range(min_size=1073741824).build()

        assert "s.total_size_r >= :min_size" in result.sql
        assert result.params["min_size"] == 1073741824
        assert "max_size" not in result.params

    def test_max_size_only(self):
        builder = DirectoryQueryBuilder()
        result = builder.with_size_range(max_size=10737418240).build()

        assert "s.total_size_r <= :max_size" in result.sql
        assert result.params["max_size"] == 10737418240
        assert "min_size" not in result.params

    def test_size_range(self):
        builder = DirectoryQueryBuilder()
        result = builder.with_size_range(min_size=1024, max_size=1048576).build()

        assert "s.total_size_r >= :min_size" in result.sql
        assert "s.total_size_r <= :max_size" in result.sql
        assert result.params["min_size"] == 1024
        assert result.params["max_size"] == 1048576

    def test_no_args_is_noop(self):
        builder = DirectoryQueryBuilder()
        result = builder.with_size_range().build()

        assert ":min_size" not in result.sql
        assert ":max_size" not in result.sql
        assert "min_size" not in result.params
        assert "max_size" not in result.params


class TestFileCountRangeFilter:
    """Tests for with_file_count_range query builder method."""

    def test_min_files_only(self):
        builder = DirectoryQueryBuilder()
        result = builder.with_file_count_range(min_files=1000).build()

        assert "s.file_count_r >= :min_files" in result.sql
        assert result.params["min_files"] == 1000
        assert "max_files" not in result.params

    def test_max_files_only(self):
        builder = DirectoryQueryBuilder()
        result = builder.with_file_count_range(max_files=50000).build()

        assert "s.file_count_r <= :max_files" in result.sql
        assert result.params["max_files"] == 50000
        assert "min_files" not in result.params

    def test_file_count_range(self):
        builder = DirectoryQueryBuilder()
        result = builder.with_file_count_range(min_files=100, max_files=5000).build()

        assert "s.file_count_r >= :min_files" in result.sql
        assert "s.file_count_r <= :max_files" in result.sql
        assert result.params["min_files"] == 100
        assert result.params["max_files"] == 5000

    def test_no_args_is_noop(self):
        builder = DirectoryQueryBuilder()
        result = builder.with_file_count_range().build()

        assert ":min_files" not in result.sql
        assert ":max_files" not in result.sql
        assert "min_files" not in result.params
        assert "max_files" not in result.params

    def test_chained_with_size_range(self):
        builder = DirectoryQueryBuilder()
        result = (
            builder.with_size_range(min_size=1073741824)
            .with_file_count_range(min_files=1000)
            .build()
        )

        assert "s.total_size_r >= :min_size" in result.sql
        assert "s.file_count_r >= :min_files" in result.sql
        assert result.params["min_size"] == 1073741824
        assert result.params["min_files"] == 1000


# ============================================================================
# Path Normalization Tests
# ============================================================================


class TestNormalizePath:
    """Tests for normalize_path helper."""

    def test_strip_glade_campaign(self):
        assert normalize_path("/glade/campaign/cisl") == "/cisl"
        assert normalize_path("/glade/campaign/cisl/users") == "/cisl/users"

    def test_strip_gpfs_csfs1(self):
        assert normalize_path("/gpfs/csfs1/cisl") == "/cisl"
        assert normalize_path("/gpfs/csfs1/cisl/users") == "/cisl/users"

    def test_strip_glade_derecho_scratch(self):
        assert normalize_path("/glade/derecho/scratch/username") == "/username"
        assert normalize_path("/glade/derecho/scratch/username/data") == "/username/data"

    def test_strip_lustre_desc1(self):
        assert normalize_path("/lustre/desc1/data") == "/data"
        assert normalize_path("/lustre/desc1/data/users") == "/data/users"

    def test_already_normalized(self):
        assert normalize_path("/cisl") == "/cisl"
        assert normalize_path("/cisl/users") == "/cisl/users"
        assert normalize_path("/asp/data") == "/asp/data"

    def test_trailing_slash_stripped(self):
        assert normalize_path("/glade/campaign/cisl/") == "/cisl"
        assert normalize_path("/cisl/") == "/cisl"

    def test_no_match_returns_unchanged(self):
        assert normalize_path("/some/other/path") == "/some/other/path"
        assert normalize_path("/glade/other/path") == "/glade/other/path"
