# Refactor Plan: Eliminate staging_dirs SQLite Table in pass1_discover_directories

## Executive Summary

**Goal:** Refactor `pass1_discover_directories` to eliminate the `staging_dirs` SQLite temporary table and use in-memory data structures instead, reducing memory footprint and improving performance.

**Key Insight:** The current approach stores directory paths twice:
1. In SQLite `staging_dirs` table during Phase 1a (~2-4GB in page cache + disk)
2. In final `path_to_id` dict returned to caller (~5-7GB)

By building the path mapping directly in memory and reusing the dict (path→depth becomes path→dir_id), we can:
- **Reduce peak memory by 35-40%** (from ~9-10GB to ~5.8GB for 70M directories)
- **Eliminate disk I/O** for staging table operations
- **Simplify code** by removing staging table and batch flushing logic
- **Improve performance** by avoiding SQLite overhead and fixing O(N*D) iteration bug

---

## Memory Analysis

### Current Approach (with SQLite staging_dirs)

| Phase | Memory Component | Size (70M dirs) |
|-------|------------------|-----------------|
| **Phase 1a** | Batch accumulation | ~1MB |
| | SQLite staging_dirs (page cache) | ~2-4GB |
| | SQLite staging_dirs (disk) | Variable |
| **Phase 1b** | SQLite staging_dirs | ~2-4GB |
| | path_to_id dict (building) | ~5.7GB |
| | **Peak Memory** | **~9-10GB** |
| **After cleanup** | path_to_id dict | ~5.7GB |

### Proposed Approach (in-memory with dict reuse)

| Phase | Memory Component | Size (70M dirs) |
|-------|------------------|-----------------|
| **Phase 1a** | path_to_depth dict | ~5.2GB |
| | **Peak Memory** | **~5.2GB** |
| **Phase 1b** | path_to_depth dict | ~5.2GB |
| | sorted_paths list (temporary) | ~560MB |
| | **Peak Memory** | **~5.8GB** |
| **After return** | path_to_id dict (reused) | ~5.2GB |

**Memory Savings:** ~35-40% reduction in peak memory (9-10GB → 5.8GB)

**Key optimizations:**
1. **No inode deduplication dict** - path-based dedup via dict keys is sufficient
2. **Dict reuse** - path_to_depth becomes path_to_id by overwriting values
3. **Sorted list iteration** - enables safe overwriting without collision bugs

---

## Critical Requirements Preserved

### 1. Path-Based Deduplication (Simplified)

**Current approach:** Inode-based deduplication via `PRIMARY KEY (fileset_id, inode)`

**New approach:** Path-based deduplication via dict keys

**Why this is sufficient:**
- The dict itself automatically deduplicates by path key
- If same path appears multiple times in scan file:
  - First: `path_to_depth["/gpfs/data"] = 2`
  - Second: `path_to_depth["/gpfs/data"] = 2` (overwrites with same value, no-op)
- Simpler and saves ~1.7GB by eliminating `inode_to_path` dict

**Implementation:**
```python
path_to_depth = {}  # Automatic dedup by path

# In merge callback
for parsed in dir_results:
    path_to_depth[parsed.path] = parsed.path.count('/')  # Dict handles dedup
```

### 2. path_to_id Return Value (MUST PRESERVE)

**Why it matters:**
- Used by Pass 2a for O(1) parent path lookups (fs_scans/importers/pass2a.py:279-286)
- Avoiding millions of database queries during statistics accumulation
- Must contain all directory paths with their assigned dir_ids

**No change to interface** - still returns `dict[str, int]` mapping paths to dir_ids

---

## Implementation Plan

### Phase 1a: Parallel Directory Discovery (Refactored)

**Old flow:**
1. Workers parse chunks → return list[ParsedEntry]
2. Main thread appends to batch list
3. Batch flushed to SQLite staging_dirs table
4. Repeat until file exhausted

**New flow:**
1. Workers parse chunks → return list[ParsedEntry] (unchanged)
2. Main thread merges into single dict:
   - `path_to_depth: dict[str, int]` - automatic dedup by path key
3. No flush needed (stays in memory)

**Code changes in pass1.py:**

```python
def pass1_discover_directories(
    input_file: Path,
    parser: FilesystemParser,
    session,
    progress_interval: int = 1_000_000,
    num_workers: int = 1,
) -> tuple[dict[str, int], dict]:
    console.print(f"[bold]Pass 1:[/bold] Discovering directories ({num_workers} workers)...")

    # In-memory structure replaces staging_dirs table
    path_to_depth = {}  # {path: depth} - will become path_to_id later

    line_count = 0
    start_time = time.time()

    with create_progress_bar(...) as progress:
        task = progress.add_task(...)

        def process_parsed_dirs(results):
            """Merge parsed directories into in-memory dict."""
            if results is None or not results[0]:
                return

            dir_results, _ = results
            for parsed in dir_results:
                # Dict key automatically handles deduplication
                path_to_depth[parsed.path] = parsed.path.count('/')

        # No flush needed - everything stays in memory
        line_count = run_parallel_file_processing(
            input_file=input_file,
            parser=parser,
            num_workers=num_workers,
            chunk_bytes=32 * 1024 * 1024,
            worker_parse_chunk=_worker_parse_chunk,
            process_results_fn=process_parsed_dirs,
            progress_callback=lambda est: progress.update(...),
            flush_callback=None,  # No flush needed!
            should_flush_fn=None,
        )

    console.print(f"    Lines scanned: {line_count:,}")
    console.print(f"    Found {len(path_to_depth):,} unique directories")

    # ... continue to Phase 1b
```

### Phase 1b: Depth-First Insertion with Dict Reuse

**Old flow:**
1. Read from staging_dirs ordered by depth
2. For each depth level:
   - Build directory insert batch
   - Get max dir_id before insert
   - Bulk insert directories
   - Assign IDs sequentially
   - Insert into path_to_id dict
3. Drop staging_dirs table

**New flow:**
1. Sort paths by depth (O(N log N) - fixes current O(N*D) iteration bug!)
2. For each depth level (grouped from sorted list):
   - Build directory insert batch
   - Bulk insert directories
   - Assign IDs sequentially
   - **Overwrite depth with dir_id in path_to_depth dict**
3. Return path_to_depth (now contains path→dir_id mappings)

**Key optimizations:**
1. **Dict reuse** - Overwrite depth values with dir_id values in same dict
2. **Sorted list iteration** - Avoids unsafe collision between dir_id and depth values
3. **Performance fix** - Current code does O(N*D) by scanning entire dict for each depth

**Why direct dict iteration is unsafe:**
```python
# UNSAFE - can process same path twice!
for depth in unique_depths:  # [0, 1, 2, 3, ...]
    paths = [p for p, d in path_to_depth.items() if d == depth]
    for p in paths:
        path_to_depth[p] = dir_id  # dir_id might equal a future depth!
        # If dir_id=3, this path will be extracted again at depth=3 iteration!
```

**Safe approach - iterate independent sorted list:**
```python
# SAFE - sorted list independent of dict values
sorted_paths = sorted(path_to_depth.keys(), key=lambda p: path_to_depth[p])
for depth, paths_iter in groupby(sorted_paths, key=lambda p: path_to_depth[p]):
    paths = list(paths_iter)
    for p in paths:
        path_to_depth[p] = dir_id  # Safe! Not iterating dict anymore
```

**Code changes in pass1.py:**

```python
    # Phase 1b: Insert directories depth-by-depth, reusing path_to_depth dict
    console.print("  [bold]Phase 1b:[/bold] Inserting into database...")

    # Sort paths by depth (O(N log N) - more efficient than O(N*D) dict scans)
    from itertools import groupby
    sorted_paths = sorted(path_to_depth.keys(), key=lambda p: path_to_depth[p])

    with create_progress_bar(show_rate=False) as progress:
        task = progress.add_task(
            "Inserting directories...",
            total=len(path_to_depth)
        )

        insert_batch_size = 25000

        # Group by depth using groupby (requires sorted input)
        for depth, paths_iter in groupby(sorted_paths, key=lambda p: path_to_depth[p]):
            paths_at_depth = list(paths_iter)

            # Prepare insertion data
            dir_inserts = []
            for p in paths_at_depth:
                parent_path, _, name = p.rpartition('/')
                if not name:  # Root case
                    name = p

                # Parent lookup: parent already has dir_id (processed earlier)
                parent_id = path_to_depth.get(parent_path) if parent_path else None

                dir_inserts.append({
                    "parent_id": parent_id,
                    "name": name,
                    "depth": depth,
                })

            # Get max dir_id before insert
            max_id_before = session.execute(
                text("SELECT COALESCE(MAX(dir_id), 0) FROM directories")
            ).scalar()

            # Bulk insert directories
            for i in range(0, len(dir_inserts), insert_batch_size):
                session.execute(
                    insert(Directory),
                    dir_inserts[i : i + insert_batch_size]
                )
            session.commit()

            # Assign IDs sequentially and OVERWRITE dict in place (safe now!)
            stats_inserts = []
            for idx, p in enumerate(paths_at_depth):
                dir_id = max_id_before + idx + 1
                path_to_depth[p] = dir_id  # Overwrites depth with dir_id
                stats_inserts.append({"dir_id": dir_id})

            # Bulk insert stats
            if stats_inserts:
                for i in range(0, len(stats_inserts), insert_batch_size):
                    stmt = sqlite_insert(DirectoryStats).values(
                        stats_inserts[i : i + insert_batch_size]
                    ).on_conflict_do_nothing(index_elements=['dir_id'])
                    session.execute(stmt)
                session.commit()

            progress.update(task, advance=len(paths_at_depth))

    console.print(f"    Inserted {len(path_to_depth):,} directories")

    # path_to_depth is now actually path_to_id (depths overwritten with dir_ids)
    path_to_id = path_to_depth

    # Return metadata
    metadata = {
        "total_lines": line_count,
        "dir_count": dir_count,
        "estimated_files": max(0, line_count - dir_count - 50),
    }

    return path_to_id, metadata
```

---

## Additional Changes

### 1. Remove staging_dirs table creation/cleanup

**Delete lines 58-73** (staging_dirs table creation and DROP TABLE)
**Delete line 152-155** (staging_dirs index creation)
**Delete line 248** (explicit DROP TABLE staging_dirs after Phase 1b)

### 2. Remove batch flushing logic

No longer need:
- `batch: list[dict] = []` accumulator (line 81)
- `flush_batch()` function (lines 120-132)
- BATCH_SIZE threshold checking (line 79)
- `session.execute(text("INSERT OR IGNORE INTO staging_dirs ..."), batch)` (lines 124-130)

### 3. Add itertools import

Add to top of file:
```python
from itertools import groupby
```

### 4. Simplify progress tracking

Update progress messages:
- Remove "Phase 1a/1b" split - just "Pass 1"
- Remove references to flushing batches to staging table

---

## Files to Modify

| File | Lines Changed | Complexity |
|------|---------------|------------|
| **fs_scans/importers/pass1.py** | 61-156, 165-249 | High - core refactoring |

---

## Testing & Verification

### 1. Unit Testing
- Verify path-based deduplication works correctly (duplicate paths handled)
- Test path→depth→dir_id dict reuse logic with sorted list iteration
- Confirm parent_id lookups work during depth-first insertion
- Test edge cases: root directories, deep paths (depth > 50)

### 2. Integration Testing
```bash
# Test on small dataset first
python -m fs_scans.importer import-scan \
  --scan-file fs_scans/test_data/small_scan.log \
  --database test_refactor.db

# Verify directory count matches
sqlite3 test_refactor.db "SELECT COUNT(*) FROM directories"

# Test on medium dataset (check memory usage)
/usr/bin/time -l python -m fs_scans.importer import-scan \
  --scan-file fs_scans/test_data/medium_scan.log \
  --database test_medium.db

# Compare memory usage before/after refactor
# Current: peak ~9-10GB for 70M dirs
# Expected: peak ~5.8GB for 70M dirs (35-40% reduction)
```

### 3. Full Dataset Testing
```bash
# Test on actual production scan (900M lines, 70M directories)
python -m fs_scans.importer import-scan \
  --scan-file fs_scans/20260111_csfs1_asp.list.list_all.log \
  --database production_test.db \
  --num-workers 8

# Monitor memory usage throughout import
# Verify Pass 2a still works (uses path_to_id for lookups)
```

### 4. Validation Queries
```sql
-- Verify all directories have parents (except root)
SELECT COUNT(*) FROM directories WHERE parent_id IS NULL AND depth > 0;
-- Expected: 0

-- Verify depth consistency
SELECT COUNT(*) FROM directories d1
JOIN directories d2 ON d1.parent_id = d2.dir_id
WHERE d1.depth != d2.depth + 1;
-- Expected: 0

-- Verify no orphaned stats
SELECT COUNT(*) FROM directory_stats
WHERE dir_id NOT IN (SELECT dir_id FROM directories);
-- Expected: 0
```

---

## Performance Impact

### Expected Improvements

1. **Memory reduction:** ~35-40% lower peak memory (9-10GB → 5.8GB)
2. **Faster Phase 1a:** No SQLite insert overhead, no disk I/O for staging table
3. **Faster Phase 1b:**
   - Fixes O(N*D) performance bug (scanning dict D times for each depth)
   - New O(N log N) sort-once approach
   - No SQL queries to read staging_dirs, no index creation
4. **Cleaner code:** ~60 fewer lines, elimination of staging table and batch logic
5. **Simpler deduplication:** Path-based dedup via dict keys (no inode tracking)

### Potential Risks

1. **Large memory spikes:** If worker memory isn't released properly, could OOM
   - Mitigation: Workers already build local dicts and return them (current behavior)
   - IPC marshalling forces copies, so worker memory is released

2. **Deduplication changes:** Path-based vs inode-based dedup
   - Old: Dedups by (fileset_id, inode) - handles hard links
   - New: Dedups by path - simpler, same result for typical scans
   - Impact: Minimal - scan files typically don't have duplicate paths
   - If needed later: Can add inode tracking back easily

3. **Dict reuse correctness:** Overwriting depth with dir_id during iteration
   - Risk: dir_id values could collide with depth values (both integers)
   - Mitigation: Use sorted list iteration (independent of dict values)
   - Parent lookups safe: parents processed before children (depth-first)

4. **Sorting overhead:** O(N log N) sort of 70M paths
   - Memory cost: ~560MB for sorted_paths list
   - Time cost: ~10-20 seconds to sort 70M strings
   - Benefit: Fixes existing O(N*D) bug, overall faster

---

## Alternative Considered (Rejected)

**Use separate dicts for depth and dir_id:**
- Phase 1a: Build `path_to_depth`
- Phase 1b: Build separate `path_to_id`
- Return `path_to_id`, discard `path_to_depth`

**Why rejected:**
- Double memory usage during Phase 1b (~10GB vs ~5GB)
- No performance benefit
- More complex code

**User's insight was correct:** Reusing the dict by overwriting values is more efficient.

---

## Summary

This refactoring eliminates the SQLite `staging_dirs` table entirely, building the directory hierarchy directly in memory. The approach combines three key innovations:

1. **Path-based deduplication** - Dict keys naturally handle duplicate paths (simpler than inode tracking)
2. **Dict reuse** - Same dict transitions from path→depth to path→dir_id by overwriting values
3. **Sorted list iteration** - Enables safe overwriting without dir_id/depth value collisions

**Benefits:**
- **35-40% memory reduction** (9-10GB → 5.8GB peak for 70M directories)
- **Faster execution:**
  - No SQLite overhead (inserts, queries, index creation)
  - No disk I/O for staging table
  - Fixes O(N*D) iteration bug → O(N log N) sort
- **Simpler code:** ~60 fewer lines, clearer logic flow
- **Performance fix:** Current code scans entire dict for each depth level (inefficient)

**Trade-offs:**
- Path-based dedup instead of inode-based (acceptable for typical scans)
- O(N log N) sort overhead (~10-20 seconds for 70M paths, but fixes worse O(N*D) bug)
- Temporary sorted_paths list uses ~560MB during Phase 1b

**Correctness preserved:**
- path_to_id interface unchanged (required by Pass 2a)
- Depth-first insertion order maintained
- Parent_id lookups work correctly (parents processed before children)

**Next steps:**
1. Implement refactoring in pass1.py
2. Test on small dataset to verify correctness
3. Test on medium dataset to measure memory reduction
4. Validate on production scan (900M lines, 70M directories)
5. Monitor memory usage to confirm ~5.8GB peak (vs ~9-10GB current)
