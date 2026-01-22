# GPFS Policy Scan Log Parser - Directory-Level Metrics

## Summary
Create a Python script to parse GPFS policy scan log files and compute **directory-level** metrics via streaming accumulators. This is a prototype for a future "cs-queries" database tool.

**Key constraint**: No file-level data stored - only directory summaries.

## Log File Format
Data lines: `<node> inode gen snapshot  key=value pairs -- /path`
- `s=` FILE_SIZE (bytes)
- `a=` KB_ALLOCATED (KB)
- `u=` USER_ID (numeric)
- `p=` permissions (first char: `-`=file, `d`=dir)
- `ac=` ACCESS_TIME (timestamp)

## Input Files
- 4 filesystems: asp (~10M lines, 3GB), cisl (~36GB), eol (~6GB), hao (~40GB)
- xz-compressed versions also available

## Metrics Per Directory

Six metrics for each directory path:

| Metric | Non-Recursive | Recursive |
|--------|---------------|-----------|
| File count | Direct children only | All descendants |
| Total size | Direct children only | All descendants |
| Max atime | Direct children only | All descendants |

**Plus**: Identify directories where all recursive contents share a single owner (user_id).

## Implementation Plan

### 1. Create `fs_scans/parse_gpfs_scan.py`

**Data structure per directory:**
```python
@dataclass
class DirStats:
    # Non-recursive (direct children only)
    file_count: int = 0
    total_size: int = 0
    max_atime: datetime | None = None

    # Recursive (all descendants)
    file_count_recursive: int = 0
    total_size_recursive: int = 0
    max_atime_recursive: datetime | None = None

    # Single-owner tracking (memory-efficient approach)
    owner_id: int = -1      # First owner encountered (-1 = not yet seen)
    single_owner: bool = True  # False once a different owner is seen
```

**Storage**: `dict[str, DirStats]` keyed by directory path

### 2. Streaming Algorithm

For each file entry:
1. Parse line, extract: path, size, atime, user_id, file_type
2. Skip if directory entry (only count files)
3. Get parent directory: `parent = os.path.dirname(path)`
4. Update parent's **non-recursive** stats
5. Walk up path tree, updating each ancestor's **recursive** stats
6. Update single-owner tracking for parent and all ancestors

```python
def update_owner(dir_stats, user_id):
    """Track single ownership efficiently - O(1) memory per directory."""
    if dir_stats.owner_id == -1:
        dir_stats.owner_id = user_id  # First file seen
    elif dir_stats.owner_id != user_id:
        dir_stats.single_owner = False  # Different owner found

def process_file(path, size, atime, user_id, stats_dict):
    parent = os.path.dirname(path)

    # Non-recursive: only direct parent
    stats = stats_dict.setdefault(parent, DirStats())
    stats.file_count += 1
    stats.total_size += size
    stats.max_atime = max(stats.max_atime, atime) if stats.max_atime else atime

    # Recursive: all ancestors
    current = parent
    while current and current != '/':
        stats = stats_dict.setdefault(current, DirStats())
        stats.file_count_recursive += 1
        stats.total_size_recursive += size
        stats.max_atime_recursive = max(...) if stats.max_atime_recursive else atime
        update_owner(stats, user_id)
        current = os.path.dirname(current)
```

### 3. Memory Considerations

- **Directory count**: Likely millions of directories (much smaller than file count)
- **Owner tracking**: O(1) per directory - just one int and one bool
- **Total per-dir**: ~64 bytes (6 ints + 2 datetimes + 1 bool) vs potentially KB with set approach

### 4. Output

Text summary format:
```
Directory: /gpfs/csfs1/asp/username/project
  Non-recursive: 1,234 files, 45.6 GB, max atime 2024-01-15
  Recursive:     56,789 files, 1.2 TB, max atime 2025-12-01
  Single owner: Yes (uid=12345)
```

Query by user_id (for future use):
- Find all directories where `single_owner=True` and `owner_id=<uid>`
- These are "top-level" directories owned entirely by that user

### 5. CLI Interface

```bash
python fs_scans/parse_gpfs_scan.py <input_file> [options]

Options:
  --output FILE          Write results to file (default: stdout)
  --min-depth N          Only report dirs at depth >= N (default: 3)
  --single-owner-only    Only report single-owner directories
  --owner-id UID         Filter to single-owner dirs owned by UID
  --progress-interval N  Report progress every N lines (default: 1000000)
```

### 6. File to Create

```
fs_scans/parse_gpfs_scan.py
```

## Verification
1. Test parsing on first 10,000 lines of asp file
2. Verify non-recursive vs recursive counts are correct
3. Run on full asp file, check memory usage
4. Verify single-owner detection works correctly
