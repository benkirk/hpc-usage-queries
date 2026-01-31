# Staging Table Performance Issue

## Problem Description

### Symptoms
- Import process appears to hang after Phase 1b completes
- No visible progress or feedback to user
- Process shows as "running" but appears stuck
- Can last 1-3+ hours on large datasets (53M+ directories)

### Observed Behavior (2026-01-31)

Import of `20260125_csfs1_cgd.list.list_all.log`:
- **Scale**: 53,647,411 directories, ~660M files
- **Workers**: 12
- **Database size**: 29GB
- **Runtime**: 2h 21m total
  - Phase 1a (directory scan): 17m 42s
  - Phase 1b (directory insert): 1h 7m 12s
  - **Stuck after Phase 1b**: 56+ minutes with no output
  - Phase 2a: Never started (still blocked)

### Process Analysis

Using `gdb` and `strace` on stuck process (PID 81307):

**Main thread stack trace:**
```
#0  freeSpace() in libsqlite3.so
#1  dropCell() in libsqlite3.so
#2  sqlite3BtreeDelete() in libsqlite3.so
#3  sqlite3VdbeExec() - executing SQL
#4  sqlite3_step()
#5  Python SQLite cursor executing DELETE/UPDATE
```

**System calls:**
```
pread64(3, ..., 4096, 29133037568)  # Reading from cgd.db
pwrite64(3, ..., 4096, 16714289152) # Writing to cgd.db
```

**Conclusion**: Process is NOT deadlocked - it's actively executing expensive SQLite B-tree deletion operations.

## Root Cause

### Code Location
`fs_scans/importers/importer.py`, lines 445-625

### The Problem

**Phase 1a (lines 445-539)**: Creates and populates `staging_dirs` table
```python
# Create staging table
session.execute(text("""
    CREATE TABLE IF NOT EXISTS staging_dirs (
        depth INTEGER NOT NULL,
        path TEXT NOT NULL PRIMARY KEY
    )
"""))
session.execute(text("CREATE INDEX IF NOT EXISTS idx_staging_depth ON staging_dirs(depth)"))

# Insert 53M+ directories during scan
INSERT OR IGNORE INTO staging_dirs (depth, path) VALUES (:depth, :path)
```

**Phase 1b (lines 545-620)**: Reads from `staging_dirs` to insert into `directories` table
```python
for depth in depths:
    paths = session.execute(
        text("SELECT path FROM staging_dirs WHERE depth = :d"), {"d": depth}
    )
    # Process and insert into directories table...
```

**After Phase 1b (lines 623-624)**: Cleanup
```python
# Cleanup staging table
session.execute(text("DROP TABLE IF EXISTS staging_dirs"))
session.commit()
```

### Why DROP TABLE is Slow

With 53,647,411 rows and an index, SQLite must:

1. **Delete all B-tree nodes** (53M rows)
   - Each row deletion requires B-tree traversal
   - Index must be updated for each deletion
   - Free page management overhead

2. **Update system catalogs**
   - Remove table metadata
   - Remove index metadata
   - Update sqlite_master table

3. **Free disk pages**
   - Mark pages as free in freelist
   - Potentially reorganize database file
   - 29GB database makes page management expensive

This is fundamentally a design issue: **massive cleanup after work completes** instead of incremental or upfront cleanup.

## Proposed Fix (Implemented)

### Strategy: Move cleanup to START of Pass 1 instead of END of Phase 1b

**Before** (lines 623-624):
```python
# After Phase 1b completes
session.execute(text("DROP TABLE IF EXISTS staging_dirs"))
session.commit()
```

**After** (lines 445-460):
```python
# At START of Pass 1, before Phase 1a
session.execute(text("DROP TABLE IF EXISTS staging_dirs"))
session.execute(text("""
    CREATE TABLE staging_dirs (
        depth INTEGER NOT NULL,
        path TEXT NOT NULL PRIMARY KEY
    )
"""))
session.execute(text("CREATE INDEX idx_staging_depth ON staging_dirs(depth)"))
session.commit()
```

### Benefits

1. **First run**: No staging table exists → DROP is instant (no-op)
2. **Subsequent runs**: Cleanup happens **before** Phase 1a starts
   - User sees delay at predictable time (before work begins)
   - No surprise hang after Phase 1b
   - Better UX: clear when process is working vs waiting

3. **Progress visibility**: Any delay happens when user expects setup time
4. **No behavior change**: Same cleanup, just moved to a better time

### Limitations

- **Subsequent runs still slow**: DROP TABLE with 53M rows takes just as long, but happens at START instead of END
- **Does not eliminate the cleanup cost**: Just moves it to a more user-friendly location

## Alternative Approaches Considered

### Option 1: SQLite TEMPORARY TABLE ❌ Rejected

```python
CREATE TEMPORARY TABLE staging_dirs (...)
```

**How it works**:
- SQLite stores TEMPORARY tables in separate temp database file (on disk, not RAM)
- Automatic cleanup when connection closes
- DROP TEMPORARY TABLE is nearly instant (just delete temp file)

**Why rejected**:
- User concern about memory usage (though TEMP tables are on disk)
- Less predictable resource usage
- Temp file location might not be on same fast storage as main DB

### Option 2: DELETE FROM instead of DROP TABLE ❌ Not Better

```python
DELETE FROM staging_dirs  # Instead of DROP TABLE
```

**Why not better**:
- Still has to delete 53M rows
- B-tree traversal and index updates still required
- Similar or same performance to DROP TABLE
- Leaves table structure in database (minor disk space waste)

### Option 3: Keep and Reuse Table ❌ Not Optimal

```python
# At start: DELETE FROM staging_dirs (clear previous data)
# At end: Leave table in place
```

**Trade-offs**:
- DELETE FROM at start: Fast if table is empty (first run), slow if 53M rows (subsequent runs)
- Same problem as Option 2, just moved to start
- Minimal benefit over proposed fix

### Option 4: Partition/Incremental Cleanup ⏸️ Future Work

```python
# Delete in batches during Phase 1b as data is processed
for depth in depths:
    paths = fetch_paths_for_depth(depth)
    process_paths(paths)
    # Delete processed rows immediately
    session.execute(text("DELETE FROM staging_dirs WHERE depth = :d"), {"d": depth})
```

**Benefits**:
- Spreads cleanup cost across Phase 1b duration
- No large cleanup at start or end
- More consistent memory usage

**Complexity**:
- Requires significant code restructuring
- Adds transactional complexity
- May impact Phase 1b performance if not tuned carefully

**Recommendation**: Consider for future optimization if startup delay becomes problematic

## Alternative Architectural Approaches

### Approach A: Eliminate staging_dirs Table Entirely

**Current flow**:
```
Phase 1a: Scan file → Write to staging_dirs
Phase 1b: Read staging_dirs → Insert into directories
```

**Alternative**: Single-pass insertion
```
Phase 1: Scan file → Insert directly into directories (sorted by depth)
```

**Challenges**:
- Requires sorting 53M paths by depth during scan
- May need external sort for large datasets
- Memory pressure if done in-memory
- Complexity of maintaining sorted order during parallel scanning

### Approach B: External Sort File

```
Phase 1a: Scan file → Write to sorted temp file on disk
Phase 1b: Read temp file → Insert into directories
Cleanup: Delete temp file (instant)
```

**Benefits**:
- No SQLite overhead for temp storage
- File deletion is instant (no B-tree operations)
- Can use OS page cache efficiently

**Trade-offs**:
- Need to manage temp file location
- Need to implement sorting logic
- Less portable than SQLite-based solution

## Recommendations

### Immediate (Implemented)
✅ Move DROP TABLE to start of Pass 1 (better UX, no regression)

### Short-term (If startup delay becomes issue)
- Monitor user feedback on startup delay in subsequent runs
- If problematic, implement **Option 4: Incremental Cleanup** during Phase 1b

### Long-term (Performance optimization)
- Consider **Approach B: External Sort File** for staging data
- Benchmark against current SQLite-based staging approach
- Evaluate at scales beyond 100M directories

## Testing Recommendations

### Verify Fix Behavior

1. **First run** (no existing staging_dirs):
   ```bash
   time fs-scans-import --workers 12 --replace <logfile>
   # Should show no delay before Phase 1a starts
   ```

2. **Second run** (staging_dirs exists with 53M rows):
   ```bash
   time fs-scans-import --workers 12 --replace <logfile>
   # Delay should occur BEFORE Phase 1a, with visible progress message
   ```

3. **Interrupted run** (staging_dirs partially populated):
   ```bash
   # Ctrl+C during Phase 1a
   # Re-run should still clean up properly
   ```

### Performance Benchmarks

Track timing for each phase:
- **Pass 1 startup**: Time from "Pass 1" message to "Phase 1a" start
- **Phase 1a**: Directory scanning
- **Phase 1b**: Directory insertion
- **Phase 1b → Phase 2a transition**: Should be instant now

## Current Status

### Running Process (PID 81307)
- **Status**: Stuck in DROP TABLE operation after Phase 1b
- **Options**:
  1. Wait (could be 1-3 more hours)
  2. Kill and restart with fix
  3. Monitor database file size to verify progress

### Code Changes
- ✅ Implemented: Move DROP TABLE to start of Pass 1
- ✅ Location: `fs_scans/importers/importer.py` lines 445-460, 623-625
- ⏸️ Not deployed: Fix will apply to NEXT run, not current stuck process

## References

### Related Code
- `fs_scans/importers/importer.py`: Main import logic
  - Lines 445-460: staging_dirs creation (modified)
  - Lines 461-539: Phase 1a (directory scanning)
  - Lines 540-620: Phase 1b (directory insertion)
  - Lines 623-625: Cleanup (modified)

### SQLite Documentation
- [DROP TABLE](https://www.sqlite.org/lang_droptable.html)
- [TEMPORARY Tables](https://www.sqlite.org/tempfiles.html)
- [DELETE](https://www.sqlite.org/lang_delete.html)
- [Performance Tuning](https://www.sqlite.org/optoverview.html)

### Investigation Tools Used
- `ps aux` - Process listing
- `pstree -p` - Process tree
- `lsof -p` - Open files
- `strace -p` - System call tracing
- `gdb -p` - Stack trace analysis
- SQLite `.schema` - Table structure inspection

## Open Questions

1. **Is there value in keeping staging_dirs between runs?**
   - Could reuse for incremental imports
   - Would need mechanism to identify stale vs current data

2. **Should we add progress feedback for DROP TABLE?**
   - Custom message: "Cleaning up staging data from previous run..."
   - Technical challenge: DROP TABLE is blocking, hard to show progress

3. **What's the right scale threshold for alternative approaches?**
   - Current: 53M directories, ~2.5h runtime, ~1h cleanup
   - At what scale should we switch to external sort approach?

---

**Document created**: 2026-01-31
**Author**: Investigation of stuck import process PID 81307
**Status**: Fix implemented, awaiting deployment and validation
