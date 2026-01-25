# Optimization Plan: Memory & Performance for scan_to_db.py

## Problem Statement
The current implementation holds all directory paths in Python memory during import. For large filesystems (100M+ lines, millions of directories), this causes high peak memory usage.

## Key Insight: Inode-based Keys
GPFS scan output provides `(fileset_id, inode)` as a unique identifier for each entry:
```
<thread> inode fileset_id snapshot  fields -- /path
```
- Two integers (16 bytes) vs path strings (~80 bytes avg) = **~5x memory reduction**
- Guaranteed unique across the filesystem

---

## Phase 1 Optimization: SQLite Staging Table

### Current Approach (Memory-Heavy)
```
Phase 1a: Collect all (path, depth) tuples in Python list
Phase 1b: Sort in Python, insert to DB, build path_to_id dict
```
Peak memory: ~220 bytes/directory (list + dict co-existing)

### Proposed Approach (Streaming to SQLite)
```
Phase 1a: Stream directories to SQLite staging table, flush periodically
Phase 1b: SELECT from staging ORDER BY depth, insert to directories table
```

#### Staging Table Schema
```sql
CREATE TABLE staging_dirs (
    inode INTEGER NOT NULL,
    fileset_id INTEGER NOT NULL,
    depth INTEGER NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY (fileset_id, inode)
);
CREATE INDEX idx_staging_depth ON staging_dirs(depth);
```

#### Phase 1a: Streaming Insert
```python
def pass1_discover_directories(...):
    # Create staging table
    session.execute(text("""
        CREATE TABLE IF NOT EXISTS staging_dirs (
            inode INTEGER NOT NULL,
            fileset_id INTEGER NOT NULL,
            depth INTEGER NOT NULL,
            path TEXT NOT NULL,
            PRIMARY KEY (fileset_id, inode)
        )
    """))
    session.execute(text("CREATE INDEX IF NOT EXISTS idx_staging_depth ON staging_dirs(depth)"))

    # Phase 1a: Stream to staging table
    batch = []
    BATCH_SIZE = 10000

    for line in file:
        parsed = parse_line_with_inode(line)  # Extract inode, fileset_id too
        if not parsed or not parsed["is_dir"]:
            continue

        batch.append({
            "inode": parsed["inode"],
            "fileset_id": parsed["fileset_id"],
            "depth": parsed["path"].count("/"),
            "path": parsed["path"]
        })

        if len(batch) >= BATCH_SIZE:
            session.execute(text("""
                INSERT OR IGNORE INTO staging_dirs (inode, fileset_id, depth, path)
                VALUES (:inode, :fileset_id, :depth, :path)
            """), batch)
            session.commit()
            batch.clear()

    # Flush remaining
    if batch:
        session.execute(...)
```

#### Phase 1b: Ordered Insert with Inode Mapping
```python
    # Phase 1b: Read from staging ordered by depth
    # Build inode_to_id instead of path_to_id (much smaller keys)
    inode_to_id = {}  # (fileset_id, inode) -> dir_id

    cursor = session.execute(text(
        "SELECT inode, fileset_id, depth, path FROM staging_dirs ORDER BY depth"
    ))

    for row in cursor:
        parent_path = os.path.dirname(row.path)
        # Need parent lookup - see options below
        parent_id = lookup_parent(parent_path, ...)

        entry = Directory(parent_id=parent_id, name=os.path.basename(row.path), depth=row.depth)
        session.add(entry)
        session.flush()
        inode_to_id[(row.fileset_id, row.inode)] = entry.dir_id

    # Cleanup staging
    session.execute(text("DROP TABLE staging_dirs"))
```

### Parent Lookup Strategy
- Keep `path_to_id` dict for parent resolution (fast lookups, simpler code)
- Key benefit of staging table: eliminates `dir_entries` list (~100 bytes/dir saved)
- `path_to_id` persists through Phase 2 (~120 bytes/dir, acceptable for balanced approach)

---

## Design Decisions

Based on requirements:
- **Goal**: Balanced approach (reduce memory while keeping runtime reasonable)
- **Phase 2**: Keep `path_to_id` dict (simpler, faster, ~120MB for 1M dirs is acceptable)
- **Parallelism**: Include basic multiprocessing with `--workers` flag

---

## Implementation Plan

### Step 1: Extract inode/fileset_id in Parser
Modify `parse_line()` to extract inode and fileset_id from scan lines.

```python
# Update LINE_PATTERN to capture inode and fileset_id
LINE_PATTERN = re.compile(
    r"^<\d+>\s+(\d+)\s+(\d+)\s+\d+\s+"  # <thread> inode fileset_id snapshot
    r"(.+?)\s+--\s+(.+)$"                # fields -- path
)

def parse_line(line: str) -> dict | None:
    match = LINE_PATTERN.match(line)
    if not match:
        return None
    inode, fileset_id, fields_str, path = match.groups()
    # ... rest of parsing ...
    return {
        "inode": int(inode),
        "fileset_id": int(fileset_id),
        "path": path,
        "size": ...,
        "user_id": ...,
        "is_dir": ...,
        "atime": ...,
    }
```

### Step 2: SQLite Staging Table for Phase 1a
Replace in-memory `dir_entries` list with streaming inserts to SQLite.

**Key changes to `pass1_discover_directories()`:**
1. Create staging table at start
2. Batch-insert directories as parsed (every 10K)
3. Phase 1b: SELECT with ORDER BY depth
4. Build `path_to_id` as directories are inserted
5. Drop staging table at end

### Step 3: Improved Progress Tracking
Pass line count and directory count from Phase 1 to Phase 2 for proper progress bars.

**Phase 1 output:**
```python
# Return additional metadata
return path_to_id, {
    "total_lines": line_count,
    "dir_count": len(path_to_id),
    "file_count": line_count - len(path_to_id) - header_lines,  # Inferred
}
```

**Phase 2 progress bar:**
```python
def pass2_accumulate_stats(..., total_lines: int):
    with Progress(...) as progress:
        task = progress.add_task(
            f"[green]Processing {input_file.name}...",
            total=total_lines,  # Now determinate!
            files=0,
            flushes=0,
        )
        for line in f:
            progress.update(task, advance=1)  # Percentage-based progress
```

**Console output after Phase 1:**
```
Pass 1: Discovering directories...
  Phase 1a: Scanning for directories...
    Lines scanned: 10,209,240
    Found 38,975 directories
    Inferred ~10,170,265 files
```

### Step 4: Add `--workers` Flag for Parallel Parsing
Add optional multiprocessing for Phase 2 (stats accumulation).

```python
@click.option(
    "--workers", "-w",
    type=int,
    default=1,
    help="Number of worker processes for parsing (default: 1, single-threaded)",
)
```

**Architecture:**
- Workers parse lines into dicts (CPU-bound regex work)
- Main process handles DB writes (SQLite single-writer)
- Queue-based communication

```python
def worker_parse(input_queue, output_queue):
    """Worker process: parse lines from input queue."""
    while True:
        chunk = input_queue.get()
        if chunk is None:  # Sentinel
            break
        results = [parse_line(line.rstrip("\n")) for line in chunk]
        output_queue.put([r for r in results if r])

def pass2_accumulate_stats_parallel(input_file, session, path_to_id, num_workers):
    if num_workers <= 1:
        return pass2_accumulate_stats(...)  # Original single-threaded

    input_queue = mp.Queue(maxsize=num_workers * 2)
    output_queue = mp.Queue()

    # Start workers
    workers = [
        mp.Process(target=worker_parse, args=(input_queue, output_queue))
        for _ in range(num_workers)
    ]
    for w in workers:
        w.start()

    # Producer: read file, send chunks
    # Consumer: receive parsed results, accumulate stats, write DB
    ...
```

**Note**: For xz-compressed files, decompression may be the bottleneck. Consider warning user or auto-detecting.

---

## Files to Modify
1. **`fs_scans/scan_to_db.py`**:
   - Update `LINE_PATTERN` and `parse_line()` to extract inode/fileset_id
   - Rewrite `pass1_discover_directories()` with staging table
   - Return line/dir/file counts from Phase 1
   - Update `pass2_accumulate_stats()` to use determinate progress bar
   - Add `pass2_accumulate_stats_parallel()` for multiprocessing
   - Add `--workers` CLI option

## Verification
1. **Correctness**: Run on asp file, compare directory counts and stats with current implementation
2. **Memory**: Measure peak memory using `/usr/bin/time -v` (MaxRSS)
3. **Performance**: Compare runtime with and without `--workers`
4. **Scale**: Test on larger files (hao, cisl) to validate at scale

## Expected Benefits
| Metric | Current | After Optimization |
|--------|---------|-------------------|
| Phase 1a peak memory | ~100 bytes/dir (Python list) | ~batch_size × 100 bytes (SQLite staging) |
| Phase 1b peak memory | ~220 bytes/dir (list + dict) | ~120 bytes/dir (path_to_id only) |
| Phase 2 with workers | Single-threaded | N workers for parsing |

For 1M directories: ~100MB savings in Phase 1 peak memory.
