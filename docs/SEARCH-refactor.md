# Plan: Query System Improvements for fs_scans

## Summary

Improve the query system in `fs_scans/query_db.py` to address:
1. Multi-database query performance
2. Path pattern matching (full paths, not just basename)
3. Code complexity with raw SQL

## Key Findings

### Multi-DB Performance
- Individual queries are fast (1-11ms per DB)
- The **real bottleneck** is recursive CTEs on large databases (e.g., cgd.db with 53M directories takes 55 seconds for path_prefix queries)
- Parallelization helps but won't fix CTE-heavy queries

### Path Pattern Matching
- Current `-N/--name-pattern` matches only `d.name` (directory basename)
- Pattern `*/COLD_STORAGE*` doesn't work because `*` applies to basename only
- Full path reconstruction happens **after** the query, so path patterns must be post-query filters

### ORM vs Raw SQL
- ORM wouldn't significantly help - recursive CTEs still require raw SQL
- A query builder class provides structure without ORM overhead

---

## Implementation Plan

### Phase 1: Multi-DB Performance (Quick Wins)

#### 1.1 Engine Caching
**File:** `fs_scans/database.py`

Add module-level engine cache to avoid repeated engine creation:
```python
_engine_cache: dict[str, Engine] = {}

def get_engine(filesystem: str, ...):
    cache_key = str(resolved_path)
    if cache_key not in _engine_cache:
        _engine_cache[cache_key] = create_engine(...)
    return _engine_cache[cache_key]
```

#### 1.2 Parallel Query Execution
**File:** `fs_scans/query_db.py`

Use `ThreadPoolExecutor` for multi-DB queries:
```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def query_single_filesystem(fs, query_params):
    session = get_session(fs)
    try:
        return query_directories(session, **query_params)
    finally:
        session.close()

# In main() for multi-DB:
with ThreadPoolExecutor(max_workers=min(len(filesystems), 8)) as executor:
    futures = {executor.submit(query_single_filesystem, fs, params): fs
               for fs in filesystems}
    for future in as_completed(futures):
        all_directories.extend(future.result())
```

### Phase 2: Path Pattern Matching

#### 2.1 Add `--path-pattern` CLI Option
**File:** `fs_scans/query_db.py`

Add new option (keep `-N/--name-pattern` for SQL-level basename matching):
```python
@click.option(
    "--path-pattern",
    "path_patterns",
    multiple=True,
    type=str,
    help="Filter by full path pattern (GLOB); applied post-query",
)
```

#### 2.2 Post-Query Path Filtering with Adaptive Retry
Apply pattern matching in Python after path reconstruction. Use adaptive over-fetching:

```python
def query_with_path_filter(session, path_patterns, limit, **query_params):
    """Query with path pattern post-filtering and adaptive retry."""
    import fnmatch

    multipliers = [2, 5, 10, 0]  # 0 = unlimited (final attempt)

    for mult in multipliers:
        fetch_limit = limit * mult if mult > 0 else None
        results = query_directories(session, limit=fetch_limit, **query_params)

        # Apply path pattern filter
        filtered = [
            d for d in results
            if any(fnmatch.fnmatch(d["path"], p) for p in path_patterns)
        ]

        if len(filtered) >= limit or mult == 0:
            return filtered[:limit] if limit else filtered

    return filtered
```

**Behavior:**
1. First try: fetch `limit * 2`, filter, check if we have enough
2. Second try: fetch `limit * 5` if needed
3. Third try: fetch `limit * 10` if needed
4. Final try: fetch unlimited (for sparse matches)

### Phase 3: Query Builder Refactoring

**File:** `fs_scans/query_builder.py` (new)

Create a `DirectoryQueryBuilder` class to encapsulate query construction:

```python
class DirectoryQueryBuilder:
    """Build directory queries with optional filters."""

    def __init__(self, session):
        self.session = session
        self.conditions = []
        self.params = {}
        self.ctes = []
        self.use_descendants_cte = False

    def with_depth_range(self, min_depth=None, max_depth=None):
        if min_depth is not None:
            self.conditions.append("d.depth >= :min_depth")
            self.params["min_depth"] = min_depth
        if max_depth is not None:
            self.conditions.append("d.depth <= :max_depth")
            self.params["max_depth"] = max_depth
        return self

    def with_path_prefix(self, path_prefix):
        ancestor_id = resolve_path_to_id(self.session, path_prefix)
        if ancestor_id is None:
            raise ValueError(f"Path not found: {path_prefix}")
        self.ctes.append(self._descendants_cte())
        self.params["ancestor_id"] = ancestor_id
        self.use_descendants_cte = True
        return self

    def with_name_patterns(self, patterns, ignore_case=False):
        # Build OR clause for patterns
        return self

    def with_owner(self, owner_id):
        self.conditions.append("s.owner_uid = :owner_id")
        self.params["owner_id"] = owner_id
        return self

    def with_access_time_range(self, before=None, after=None):
        # Add access time filters
        return self

    def build(self) -> tuple[str, dict]:
        """Return (sql_query, params) tuple."""
        # Assemble CTEs, SELECT, WHERE, ORDER BY
        return query, self.params

    def execute(self, limit=None, sort_by="size_r"):
        query, params = self.build()
        # Add ORDER BY and LIMIT, execute, transform results
        return results
```

**Benefits:**
- Fluent interface for building queries
- Each filter method is testable in isolation
- Reduces cognitive load in `query_directories()`
- Type hints for IDE support

**Migration:** Gradually replace inline query building in `query_directories()` with builder calls.

---

## Files to Modify

| File | Changes |
|------|---------|
| `fs_scans/database.py` | Add engine caching |
| `fs_scans/query_db.py` | Add parallel execution, `--path-pattern` option, integrate query builder |
| `fs_scans/query_builder.py` | New file: `DirectoryQueryBuilder` class |
| `fs_scans/tests/test_query_builder.py` | New file: Unit tests for query builder |

---

## Verification

1. **Single DB query (baseline):**
   ```bash
   query-fs-scan-db --sort-by files_r -N "*COLD_STORAGE*" cisl
   ```

2. **Multi-DB query (should be faster after parallelization):**
   ```bash
   time query-fs-scan-db --sort-by files_r -N "*COLD_STORAGE*"
   ```

3. **Path pattern matching (new feature):**
   ```bash
   query-fs-scan-db --path-pattern "*/COLD_STORAGE/*" cisl
   query-fs-scan-db --path-pattern "/gpfs/csfs1/*/COLD_STORAGE*"
   ```

4. **Run existing tests:**
   ```bash
   pytest fs_scans/tests/
   ```
