"""Query builder for directory statistics queries.

Provides a fluent interface for constructing complex directory queries
with proper condition building, CTE generation, and parameter management.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class QueryResult:
    """Container for built query and parameters."""

    sql: str
    params: dict[str, Any]


@dataclass
class DirectoryQueryBuilder:
    """Builds directory queries with a fluent interface.

    Example usage:
        builder = DirectoryQueryBuilder()
        result = (
            builder
            .with_depth_range(min_depth=4, max_depth=6)
            .with_single_owner()
            .with_accessed_before(datetime(2024, 1, 1))
            .with_path_prefix_id(ancestor_id=42)
            .with_sort("size_r")
            .with_limit(100)
            .build()
        )
        # result.sql contains the query string
        # result.params contains the parameter dictionary
    """

    # Internal state - use default_factory to avoid mutable default issues
    _conditions: list[str] = field(default_factory=list)
    _params: dict[str, Any] = field(default_factory=dict)
    _ctes: list[str] = field(default_factory=list)
    _use_descendants_cte: bool = False
    _sort_by: str = "size_r"
    _limit: int | None = None

    # Sort field mapping
    SORT_MAP: dict[str, str] = field(
        default_factory=lambda: {
            "size": "s.total_size_r DESC",
            "size_r": "s.total_size_r DESC",
            "size_nr": "s.total_size_nr DESC",
            "files": "s.file_count_r DESC",
            "files_r": "s.file_count_r DESC",
            "files_nr": "s.file_count_nr DESC",
            "dirs": "s.dir_count_r DESC",
            "dirs_r": "s.dir_count_r DESC",
            "dirs_nr": "s.dir_count_nr DESC",
            "atime": "s.max_atime_r DESC",
            "atime_r": "s.max_atime_r DESC",
            "path": "d.depth ASC, d.name ASC",
            "depth": "d.depth DESC",
        }
    )

    def with_depth_range(
        self, min_depth: int | None = None, max_depth: int | None = None
    ) -> "DirectoryQueryBuilder":
        """Filter by path depth range.

        Args:
            min_depth: Minimum depth (inclusive)
            max_depth: Maximum depth (inclusive)

        Returns:
            self for chaining
        """
        if min_depth is not None:
            self._conditions.append("d.depth >= :min_depth")
            self._params["min_depth"] = min_depth
        if max_depth is not None:
            self._conditions.append("d.depth <= :max_depth")
            self._params["max_depth"] = max_depth
        return self

    def with_single_owner(self) -> "DirectoryQueryBuilder":
        """Filter to single-owner directories only.

        Excludes directories with NULL or -1 owner_uid.

        Returns:
            self for chaining
        """
        self._conditions.append("s.owner_uid IS NOT NULL AND s.owner_uid != -1")
        return self

    def with_owner(self, owner_id: int) -> "DirectoryQueryBuilder":
        """Filter to specific owner UID.

        Args:
            owner_id: The owner UID to filter by

        Returns:
            self for chaining
        """
        self._conditions.append("s.owner_uid = :owner_id")
        self._params["owner_id"] = owner_id
        return self

    def with_single_group(self) -> "DirectoryQueryBuilder":
        """Filter to single-group directories only.

        Excludes directories with NULL or -1 owner_gid.

        Returns:
            self for chaining
        """
        self._conditions.append("s.owner_gid IS NOT NULL AND s.owner_gid != -1")
        return self

    def with_group(self, group_id: int) -> "DirectoryQueryBuilder":
        """Filter to specific group GID.

        Args:
            group_id: The group GID to filter by

        Returns:
            self for chaining
        """
        self._conditions.append("s.owner_gid = :group_id")
        self._params["group_id"] = group_id
        return self

    def with_accessed_before(self, dt: datetime) -> "DirectoryQueryBuilder":
        """Filter to directories with max_atime_r before date.

        Args:
            dt: The cutoff datetime

        Returns:
            self for chaining
        """
        self._conditions.append("s.max_atime_r < :accessed_before")
        self._params["accessed_before"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        return self

    def with_accessed_after(self, dt: datetime) -> "DirectoryQueryBuilder":
        """Filter to directories with max_atime_r after date.

        Args:
            dt: The cutoff datetime

        Returns:
            self for chaining
        """
        self._conditions.append("s.max_atime_r > :accessed_after")
        self._params["accessed_after"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        return self

    def with_leaves_only(self) -> "DirectoryQueryBuilder":
        """Filter to directories with no subdirectories (leaf nodes).

        Returns:
            self for chaining
        """
        self._conditions.append("s.dir_count_nr = 0")
        return self

    def with_name_patterns(
        self, patterns: list[str], ignore_case: bool = False
    ) -> "DirectoryQueryBuilder":
        """Filter by name patterns (OR'd together).

        Args:
            patterns: List of GLOB patterns (case-sensitive) or LIKE patterns (case-insensitive)
            ignore_case: If True, converts patterns to LIKE for case-insensitive matching

        Returns:
            self for chaining
        """
        if not patterns:
            return self

        pattern_conditions = []
        for i, pattern in enumerate(patterns):
            param_name = f"name_pattern_{i}"
            if ignore_case:
                # Convert GLOB to LIKE pattern (* -> %, ? -> _)
                # LIKE is case-insensitive by default in SQLite
                like_pattern = pattern.replace("*", "%").replace("?", "_")
                pattern_conditions.append(f"d.name LIKE :{param_name}")
                self._params[param_name] = like_pattern
            else:
                pattern_conditions.append(f"d.name GLOB :{param_name}")
                self._params[param_name] = pattern

        self._conditions.append(f"({' OR '.join(pattern_conditions)})")
        return self

    def with_size_range(
        self, min_size: int | None = None, max_size: int | None = None
    ) -> "DirectoryQueryBuilder":
        """Filter by recursive total size range.

        Args:
            min_size: Minimum total_size_r in bytes (inclusive)
            max_size: Maximum total_size_r in bytes (inclusive)

        Returns:
            self for chaining
        """
        if min_size is not None:
            self._conditions.append("s.total_size_r >= :min_size")
            self._params["min_size"] = min_size
        if max_size is not None:
            self._conditions.append("s.total_size_r <= :max_size")
            self._params["max_size"] = max_size
        return self

    def with_file_count_range(
        self, min_files: int | None = None, max_files: int | None = None
    ) -> "DirectoryQueryBuilder":
        """Filter by recursive file count range.

        Args:
            min_files: Minimum file_count_r (inclusive)
            max_files: Maximum file_count_r (inclusive)

        Returns:
            self for chaining
        """
        if min_files is not None:
            self._conditions.append("s.file_count_r >= :min_files")
            self._params["min_files"] = min_files
        if max_files is not None:
            self._conditions.append("s.file_count_r <= :max_files")
            self._params["max_files"] = max_files
        return self

    def with_dir_count_range(
        self, min_dirs: int | None = None, max_dirs: int | None = None
    ) -> "DirectoryQueryBuilder":
        """Filter by recursive directory count range.

        Args:
            min_dirs: Minimum dir_count_r (inclusive)
            max_dirs: Maximum dir_count_r (inclusive)

        Returns:
            self for chaining
        """
        if min_dirs is not None:
            self._conditions.append("s.dir_count_r >= :min_dirs")
            self._params["min_dirs"] = min_dirs
        if max_dirs is not None:
            self._conditions.append("s.dir_count_r <= :max_dirs")
            self._params["max_dirs"] = max_dirs
        return self

    def with_path_prefix_ids(self, ancestor_ids: list[int]) -> "DirectoryQueryBuilder":
        """Filter to descendants of specific directory IDs (OR'd together).

        The ancestor_ids should be resolved externally via resolve_path_to_id().

        Args:
            ancestor_ids: List of directory IDs of the ancestors

        Returns:
            self for chaining
        """
        if not ancestor_ids:
            return self

        # Add each ancestor ID as a parameter
        for i, aid in enumerate(ancestor_ids):
            self._params[f"ancestor_id_{i}"] = aid

        # Build IN clause for multiple ancestors
        ancestor_params = ", ".join(f":ancestor_id_{i}" for i in range(len(ancestor_ids)))
        self._ctes.append(
            f"""
            ancestors AS (
                SELECT dir_id FROM directories WHERE dir_id IN ({ancestor_params})
            ),
            descendants AS (
                SELECT dir_id FROM ancestors
                UNION ALL
                SELECT d.dir_id FROM directories d
                JOIN descendants p ON d.parent_id = p.dir_id
            )"""
        )
        self._use_descendants_cte = True
        return self

    def with_sort(self, sort_by: str) -> "DirectoryQueryBuilder":
        """Set sort order.

        Args:
            sort_by: One of: size_r, size_nr, files_r, files_nr, atime_r, path, depth

        Returns:
            self for chaining
        """
        self._sort_by = sort_by
        return self

    def with_limit(self, limit: int) -> "DirectoryQueryBuilder":
        """Set result limit.

        Args:
            limit: Maximum number of results

        Returns:
            self for chaining
        """
        self._limit = limit
        return self

    def build(self) -> QueryResult:
        """Build the final query and parameters.

        Returns:
            QueryResult with sql string and params dictionary
        """
        # Build CTE clause
        cte_clause = ""
        if self._ctes:
            cte_clause = "WITH RECURSIVE " + ",".join(self._ctes)

        # Build SELECT clause based on CTE usage
        if self._use_descendants_cte:
            select_clause = """
                SELECT d.dir_id, d.parent_id, d.name, d.depth,
                       s.file_count_nr, s.total_size_nr, s.max_atime_nr, s.dir_count_nr,
                       s.file_count_r, s.total_size_r, s.max_atime_r, s.dir_count_r,
                       s.owner_uid, s.owner_gid
                FROM descendants
                JOIN directories d USING (dir_id)
                JOIN directory_stats s USING (dir_id)
            """
        else:
            select_clause = """
                SELECT d.dir_id, d.parent_id, d.name, d.depth,
                       s.file_count_nr, s.total_size_nr, s.max_atime_nr, s.dir_count_nr,
                       s.file_count_r, s.total_size_r, s.max_atime_r, s.dir_count_r,
                       s.owner_uid, s.owner_gid
                FROM directories d
                JOIN directory_stats s USING (dir_id)
            """

        # Assemble query
        query = cte_clause + select_clause

        # Add WHERE clause
        if self._conditions:
            query += " WHERE " + " AND ".join(self._conditions)

        # Add ORDER BY
        order_clause = self.SORT_MAP.get(self._sort_by, self.SORT_MAP["size_r"])
        query += f" ORDER BY {order_clause}"

        # Add LIMIT
        if self._limit:
            query += " LIMIT :limit"
            self._params["limit"] = self._limit

        return QueryResult(sql=query, params=self._params)

    def reset(self) -> "DirectoryQueryBuilder":
        """Reset builder state for reuse.

        Returns:
            self for chaining
        """
        self._conditions = []
        self._params = {}
        self._ctes = []
        self._use_descendants_cte = False
        self._sort_by = "size_r"
        self._limit = None
        return self
