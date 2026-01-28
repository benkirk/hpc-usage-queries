"""Tests for fs_scans query system."""

import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from fs_scans.models import Base, Directory, DirectoryStats
from fs_scans.database import get_engine, clear_engine_cache
from fs_scans.query_builder import DirectoryQueryBuilder, QueryResult


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

    def test_path_prefix_cte(self):
        """Test path prefix generates descendants CTE."""
        builder = DirectoryQueryBuilder()
        result = builder.with_path_prefix_id(ancestor_id=42).build()

        assert "WITH RECURSIVE" in result.sql
        assert "descendants AS" in result.sql
        assert "FROM descendants" in result.sql
        assert result.params["ancestor_id"] == 42

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
