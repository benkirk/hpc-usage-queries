# Histogram Collection Implementation

## Overview

Successfully implemented fine-grained histogram collection during filesystem scan import. Histograms track file distributions by access time and size at the per-user level, enabling instant queries without on-demand computation.

## Implementation Summary

### 1. Database Schema (fs_scans/core/models.py)

Added two new ORM models:

- **AccessHistogram**: Tracks file count and total size per user across 10 access time buckets
- **SizeHistogram**: Tracks file count and total size per user across 10 size buckets

Both tables use composite primary key (owner_uid, bucket_index) with indexes for efficient querying.

### 2. Bucket Definitions (fs_scans/importers/importer.py)

**Access Time Histogram** (10 buckets):
```python
ATIME_BUCKETS = [
    ("< 1 Month", 30),      # 0-30 days
    ("1-3 Months", 90),     # 30-90 days
    ("3-6 Months", 180),    # 90-180 days
    ("6-12 Months", 365),   # 180-365 days
    ("1-2 Years", 730),     # 1-2 years
    ("2-3 Years", 1095),    # 2-3 years
    ("3-4 Years", 1460),    # 3-4 years
    ("5-6 Years", 2190),    # 5-6 years
    ("6-7 Years", 2555),    # 6-7 years
    ("7+ Years", None),     # 7+ years
]
```

**Size Histogram** (10 buckets):
```python
SIZE_BUCKETS = [
    ("0 - 1 KiB", 0, 1024),
    ("1 KiB - 10 KiB", 1024, 10 * 1024),
    ("10 KiB - 100 KiB", 10 * 1024, 100 * 1024),
    ("100 KiB - 1 MiB", 100 * 1024, 1024 * 1024),
    ("1 MiB - 10 MiB", 1024 * 1024, 10 * 1024 * 1024),
    ("10 MiB - 100 MiB", 10 * 1024 * 1024, 100 * 1024 * 1024),
    ("100 MiB - 1 GiB", 100 * 1024 * 1024, 1024 * 1024 * 1024),
    ("1 GiB - 10 GiB", 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024),
    ("10 GiB - 100 GiB", 10 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024),
    ("100 GiB+", 100 * 1024 * 1024 * 1024, None),
]
```

### 3. Helper Functions

**classify_atime_bucket(atime, scan_date)**: Classifies files by age relative to scan date
**classify_size_bucket(size_bytes)**: Classifies files by allocated size

### 4. Worker-Level Collection

Modified `_worker_parse_chunk()` to:
- Accept scan_date parameter
- Track histograms per UID alongside directory aggregation
- Return both directory results and histogram results
- Convert to tuples before IPC for efficiency

Memory overhead: ~100 bytes per UID per worker (negligible)

### 5. Main Thread Merging

Modified `pass2a_nonrecursive_stats()` to:
- Accept scan_date parameter
- Maintain `pending_histograms` dictionary
- Merge worker histogram results
- Handle both parallel and single-threaded modes

### 6. Database Flushing

Added `flush_histograms()` function:
- Bulk inserts histogram data after Pass 2a completes
- Skips empty buckets to save space
- Single flush at end (simple, efficient, prevents partial data)

### 7. Integration

Modified `run_import()` to:
- Extract scan_date from filename early
- Pass scan_date to Pass 2a
- Display scan date to user

## Verification

Created comprehensive test suite that verifies:

1. **Bucket Classification**: Correct classification of files into buckets
2. **Database Storage**: Histograms stored correctly in database
3. **Query Functionality**: Data can be queried and aggregated
4. **Data Integrity**: Histogram totals match directory_stats totals

### Test Results

```
✓ Access time bucket classification (10 test cases)
✓ Size bucket classification (10 test cases + edge cases)
✓ Bucket coverage (no gaps in ranges)
✓ ORM model creation
✓ End-to-end import with 7 files across 2 users
✓ Histogram data matches expected buckets
✓ Totals match: access_histogram = size_histogram = directory_stats
✓ Filesystem-wide aggregation (SUM across users)
```

## Database Schema

```sql
CREATE TABLE access_histogram (
    owner_uid INTEGER NOT NULL,
    bucket_index INTEGER NOT NULL,
    file_count INTEGER DEFAULT 0,
    total_size INTEGER DEFAULT 0,
    PRIMARY KEY (owner_uid, bucket_index)
);

CREATE INDEX ix_access_hist_uid ON access_histogram (owner_uid);
CREATE INDEX ix_access_hist_bucket ON access_histogram (bucket_index);

CREATE TABLE size_histogram (
    owner_uid INTEGER NOT NULL,
    bucket_index INTEGER NOT NULL,
    file_count INTEGER DEFAULT 0,
    total_size INTEGER DEFAULT 0,
    PRIMARY KEY (owner_uid, bucket_index)
);

CREATE INDEX ix_size_hist_uid ON size_histogram (owner_uid);
CREATE INDEX ix_size_hist_bucket ON size_histogram (bucket_index);
```

## Usage Examples

### Per-User Access History
```sql
SELECT
    bucket_index,
    file_count,
    total_size,
    CASE bucket_index
        WHEN 0 THEN '< 1 Month'
        WHEN 1 THEN '1-3 Months'
        -- ... etc
    END as age_range
FROM access_histogram
WHERE owner_uid = 1000
ORDER BY bucket_index;
```

### Filesystem-Wide Size Distribution
```sql
SELECT
    bucket_index,
    SUM(file_count) as total_files,
    SUM(total_size) as total_bytes
FROM size_histogram
GROUP BY bucket_index
ORDER BY bucket_index;
```

### Find Users with Old Files
```sql
SELECT
    owner_uid,
    SUM(file_count) as old_files,
    SUM(total_size) as old_bytes
FROM access_histogram
WHERE bucket_index >= 5  -- 2+ years old
GROUP BY owner_uid
ORDER BY old_bytes DESC
LIMIT 10;
```

## Performance Impact

Based on implementation analysis:

- **Memory**: ~50KB per worker, ~100KB in main thread (negligible)
- **CPU**: ~10-20 integer comparisons + 4 array increments per file (minimal)
- **Storage**: ~16 rows per user × 1000 users = ~16K rows (~1-2 MB)
- **Import Time**: Expected increase <5% (dominated by I/O, not computation)

## Files Modified

1. **fs_scans/core/models.py**: Added AccessHistogram and SizeHistogram models
2. **fs_scans/importers/importer.py**:
   - Added bucket definitions and helper functions
   - Modified worker function signature and logic
   - Modified main thread processing
   - Added flush_histograms() function
   - Modified run_import() to extract and pass scan_date

## Future Enhancements (Out of Scope)

The following were explicitly **NOT** implemented but are enabled by this work:

1. Refactor analyze phase to use pre-computed histograms
2. Add `fs-scans analyze --file-sizes` command
3. On-demand histograms for specific paths
4. Cross-filesystem histogram aggregation
5. Historical trend analysis across scan dates
6. CLI commands to query histogram data directly

## Notes

- Histograms track **allocated size** (actual disk space), matching existing system behavior
- Empty buckets are not stored (sparse representation saves space)
- Scan date is extracted from filename (format: YYYYMMDD_...)
- If scan date cannot be extracted, a warning is shown but import continues
- Histogram collection is automatic during all imports (no flag required)
- Tables are created automatically during database initialization
- Backwards compatible: existing databases will have tables added automatically
