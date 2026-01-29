# Implementation Plan: Add `--dir-counts` Flag to query-fs-scan-db

## Overview

Add an optional `--dir-counts` flag that displays directory count columns (Dirs and Dirs(NR)) showing the number of subdirectories within each result directory, computed on-demand from existing parent-child relationships without modifying the database schema.

## Background

**Current State:**
- Database stores directories in normalized hierarchy with `parent_id` relationships
- Results show file counts (recursive and non-recursive) but not directory counts
- All data needed to compute directory counts already exists in the `directories` table

**Requirements:**
- Add `--dir-counts` CLI flag (optional, default off)
- Display two new columns when enabled:
  - **Dirs** (recursive): Total count of all descendant directories
  - **Dirs(NR)** (non-recursive): Count of direct child directories only
- Compute from existing data structure (no database schema changes)
- No need to support filtering/searching by these values
- Follow existing column naming pattern (recursive first, then NR variant)

## Implementation Strategy

### Architecture Decision: Batch Query Approach

Compute directory counts in a **separate batch query** after main results are retrieved, following the existing pattern of `get_full_paths_batch()` (query_db.py:184-222).

**Rationale:**
- Performance: Single batch query (1 query) vs N individual queries per result
- Maintainability: Clean separation from core query logic
- Consistency: Mirrors existing `get_full_paths_batch()` pattern
- Performance: ~900ms for 50 directories (acceptable given default limit of 50)

### Phase 1: Add Directory Count Batch Function

**File:** `fs_scans/query_db.py`
**Location:** Insert after `get_full_paths_batch()` (around line 223)

Create new function:
```python
def get_directory_counts_batch(session, dir_ids: list[int]) -> dict[int, tuple[int, int]]:
    """
    Compute directory counts for multiple directories in a single query.

    Args:
        session: SQLAlchemy session
        dir_ids: List of directory IDs to count

    Returns:
        Dictionary mapping dir_id to (ndirs_r, ndirs_nr) tuple
        - ndirs_r: Recursive descendant directory count
        - ndirs_nr: Direct child directory count (non-recursive)
    """
```

**Implementation approach:**
- Return empty dict `{}` if `dir_ids` is empty
- Build IN clause with named parameters (`:id_0`, `:id_1`, etc.)
- Use single `WITH RECURSIVE` CTE combining both counts:
  - Direct counts: `LEFT JOIN` children on `parent_id`, `GROUP BY` origin
  - Recursive counts: Recursive CTE walking descendants with origin tracking
- Return dict mapping `dir_id -> (ndirs_r, ndirs_nr)`

**SQL pattern:**
```sql
WITH RECURSIVE descendant_cte AS (
    -- Base: start from each target directory
    SELECT dir_id, dir_id as origin_id FROM directories WHERE dir_id IN (...)
    UNION ALL
    -- Recursive: find children
    SELECT d.dir_id, cte.origin_id
    FROM directories d
    JOIN descendant_cte cte ON d.parent_id = cte.dir_id
)
SELECT
    origin_id,
    COUNT(*) - 1 as ndirs_r,  -- Subtract 1 to exclude origin itself
    SUM(CASE WHEN d.parent_id = origin_id THEN 1 ELSE 0 END) as ndirs_nr
FROM descendant_cte cte
JOIN directories d ON d.dir_id = cte.dir_id
GROUP BY origin_id
```

### Phase 2: Extend `query_directories()` Function

**File:** `fs_scans/query_db.py`
**Location:** Function signature at line 225, implementation around line 366

**Changes:**

1. Add parameter to function signature:
   ```python
   def query_directories(
       session,
       # ... existing parameters ...
       compute_dir_counts: bool = False,  # NEW
   ) -> list[dict]:
   ```

2. After building `directories` list (line 366), add batch counting:
   ```python
   # Optionally compute directory counts in batch
   if compute_dir_counts and directories:
       dir_ids = [d["dir_id"] for d in directories]
       dir_count_map = get_directory_counts_batch(session, dir_ids)
       for d in directories:
           ndirs_r, ndirs_nr = dir_count_map.get(d["dir_id"], (0, 0))
           d["ndirs_r"] = ndirs_r
           d["ndirs_nr"] = ndirs_nr
   ```

3. Update docstring to document new parameter

### Phase 3: Update Output Functions

#### 3a. Modify `print_results()` Function

**File:** `fs_scans/query_db.py`
**Location:** Lines 369-475

**Changes:**

1. Add parameter to function signature:
   ```python
   def print_results(
       directories: list[dict],
       verbose: bool = False,
       leaves_only: bool = False,
       username_map: dict[int, str] | None = None,
       show_total: bool = False,
       show_dir_counts: bool = False,  # NEW
   ) -> None:
   ```

2. Add columns after Files columns (around line 398-400):
   ```python
   if show_dir_counts:
       if leaves_only:
           table.add_column("Dirs", justify="right")  # Simplified for leaves
       else:
           table.add_column("Dirs\n", justify="right")      # Recursive
           table.add_column("Dirs\n(NR)", justify="right")  # Non-recursive
   ```

3. Add row data when building result rows (around line 430):
   ```python
   if show_dir_counts:
       if leaves_only:
           row_data.append(f"{d.get('ndirs_nr', 0):,}")
       else:
           row_data.append(f"{d.get('ndirs_r', 0):,}")
           row_data.append(f"{d.get('ndirs_nr', 0):,}")
   ```

4. For totals row: Leave directory count cells empty (summing doesn't make sense)

#### 3b. Modify `write_tsv()` Function

**File:** `fs_scans/query_db.py`
**Location:** Lines 479-500

**Changes:**

1. Add parameter to function signature:
   ```python
   def write_tsv(
       directories: list[dict],
       output_path: Path,
       include_dir_counts: bool = False,  # NEW
   ) -> None:
   ```

2. Modify header to include optional columns:
   ```python
   header = "directory\tdepth\ttotal_size_r\ttotal_size_nr\tfile_count_r\tfile_count_nr\t"
   if include_dir_counts:
       header += "dir_count_r\tdir_count_nr\t"
   header += "max_atime_r\tmax_atime_nr\towner_uid\n"
   ```

3. Add data columns in row writing loop:
   ```python
   if include_dir_counts:
       line += f"{d.get('ndirs_r', 0)}\t{d.get('ndirs_nr', 0)}\t"
   ```

### Phase 4: Add CLI Option and Wire Through

**File:** `fs_scans/query_db.py`
**Locations:** Multiple sections

**Changes:**

1. **Add Click option** (around line 991, after `--show-total`):
   ```python
   @click.option(
       "--dir-counts",
       is_flag=True,
       help="Show directory counts (Dirs and Dirs(NR) columns)",
   )
   ```

2. **Add to `main()` signature** (line 992):
   ```python
   def main(
       # ... existing parameters ...
       show_total: bool,
       dir_counts: bool,  # NEW
   ):
   ```

3. **Thread through function calls:**
   - `query_single_filesystem()` signature (line 775): Add `compute_dir_counts: bool` parameter
   - `query_single_filesystem()` body: Pass `compute_dir_counts=compute_dir_counts` to `query_directories()` call
   - Direct `query_directories()` call (line 1269): Add `compute_dir_counts=dir_counts`
   - `print_results()` calls: Add `show_dir_counts=dir_counts`
   - `write_tsv()` call (line 1312): Add `include_dir_counts=dir_counts`

### Phase 5: Handle Edge Cases

**File:** `fs_scans/query_db.py`
**Location:** `main()` function, around line 1149

Add validation for `--group-by owner` mode (directory counts don't apply):
```python
if group_by == "owner":
    if dir_counts:
        console.print("[yellow]Warning: --dir-counts ignored with --group-by owner[/yellow]")
    # ... existing owner summary code continues ...
```

## Critical Files

### Files to Modify

- **fs_scans/query_db.py** (~130 lines of changes)
  - Add `get_directory_counts_batch()` function (~50 lines)
  - Modify `query_directories()` (~10 lines)
  - Modify `print_results()` (~30 lines)
  - Modify `write_tsv()` (~15 lines)
  - Modify `query_single_filesystem()` (~5 lines)
  - Add CLI option and wire through `main()` (~20 lines)

### Files for Reference (No Changes)

- **fs_scans/models.py** - Understanding `Directory.parent_id` relationship
- **fs_scans/query_builder.py** - Reference for CTE patterns

## Edge Cases & Considerations

1. **Empty results:** `get_directory_counts_batch([])` returns `{}`
2. **Large result sets:** Performance tested at ~900ms for 50 directories (acceptable)
3. **Multi-database queries:** Each filesystem computes counts independently in parallel
4. **Leaves-only mode:** Show only Dirs(NR) column (recursive always 0 for leaves)
5. **TSV export:** Include counts as `dir_count_r` and `dir_count_nr` columns
6. **Totals row:** Leave directory count cells empty (summing not meaningful)
7. **Group-by owner:** Warn and ignore flag (no specific directories shown)
8. **Missing counts:** Use `.get('ndirs_r', 0)` for safety if batch query fails

## Performance Impact

- **Without flag:** Zero impact (no extra queries)
- **With flag (50 results):** ~900ms additional query time
- **With flag (10 results):** ~200ms additional query time
- **Scaling:** Linear with result count, acceptable for typical use (default limit=50)

## Verification & Testing

### Manual Testing

1. **Basic functionality:**
   ```bash
   query-fs-scan-db cisl --dir-counts --limit 10
   ```
   Expected: Table shows Dirs and Dirs(NR) columns with counts

2. **Leaves-only mode:**
   ```bash
   query-fs-scan-db cisl --dir-counts --leaves-only --limit 10
   ```
   Expected: Only Dirs column shown (NR only, since leaves have no subdirs)

3. **TSV export:**
   ```bash
   query-fs-scan-db cisl --dir-counts -o test.tsv --limit 10
   ```
   Expected: TSV includes `dir_count_r` and `dir_count_nr` columns

4. **Group-by owner (should warn):**
   ```bash
   query-fs-scan-db cisl --dir-counts --group-by owner
   ```
   Expected: Warning message, counts not shown

5. **Multi-database:**
   ```bash
   query-fs-scan-db all --dir-counts --limit 5
   ```
   Expected: Counts computed correctly for each filesystem

6. **Verify accuracy:**
   - For a known directory, manually count subdirectories
   - Compare with `--dir-counts` output
   - Use `ls -la <path> | grep ^d | wc -l` for direct children validation

### Unit Tests (if implementing)

Create test file `tests/test_directory_counts.py`:

1. Test `get_directory_counts_batch()`:
   - Empty list returns `{}`
   - Single directory with children returns correct counts
   - Directory with deep hierarchy returns recursive count
   - Leaf directory returns `(0, 0)`
   - Batch of multiple directories returns all counts

2. Integration tests:
   - CLI with `--dir-counts` produces expected output format
   - TSV export includes count columns
   - Leaves-only shows simplified columns

## Implementation Notes

- Follow existing code style and patterns throughout
- Use f-strings for string formatting (consistent with codebase)
- Add comma formatting for directory counts (`f"{count:,}"`)
- Preserve existing behavior when flag is not set (zero performance impact)
- Ensure recursive CTE follows existing pattern in `get_full_paths_batch()`
- Column order: Recursive first, then non-recursive (matches Files/Size pattern)
