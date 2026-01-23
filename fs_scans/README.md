# GPFS Policy Scan Tools

Tools for parsing GPFS policy scan log files and computing **directory-level metrics**. This includes both a streaming parser for quick analysis and a database importer for persistent storage and querying.

## Tools

| Tool | Purpose |
|------|---------|
| `parse_gpfs_scan.py` | Streaming parser - quick analysis, no persistence |
| `scan_to_db.py` | Database importer - SQLite storage for complex queries |
| `query_db.py` | Query interface for the SQLite database |

## Overview

Both tools process GPFS scan logs and aggregate statistics at the directory level:

| Metric | Non-Recursive | Recursive |
|--------|---------------|-----------|
| File count | Direct children only | All descendants |
| Total size | Direct children only | All descendants |
| Max access time | Direct children only | All descendants |

Additionally, it tracks **single-owner directories** - directories where all recursive contents share a single owner (user_id).

## Installation

Requires Python 3.10+ with `click` and `rich`. From the project root:

```bash
source etc/config_env.sh
```

## Usage

```bash
python fs_scans/parse_gpfs_scan.py <input_file> [options]
```

### Input Files

- Plain text log files (`.log`)
- XZ-compressed files (`.log.xz`) - automatically detected

### Options

| Option | Description |
|--------|-------------|
| `-o, --output FILE` | Write results to TSV file |
| `-d, --min-depth N` | Only report directories at depth >= N (default: 3) |
| `-s, --single-owner-only` | Only report single-owner directories |
| `-u, --owner-id UID` | Filter to directories owned entirely by UID |
| `-n, --max-results N` | Limit output to N directories |
| `--sort-by FIELD` | Sort by: `size_recursive`, `size`, `files_recursive`, `files`, `atime_recursive`, `atime`, `path` |
| `-p, --progress-interval N` | Progress reporting interval (default: 1M lines) |

### Examples

```bash
# Basic usage - show top 50 directories by recursive size
python fs_scans/parse_gpfs_scan.py fs_scans/20260111_csfs1_asp.list.list_all.log

# Export to TSV for analysis
python fs_scans/parse_gpfs_scan.py fs_scans/20260111_csfs1_asp.list.list_all.log -o results.tsv

# Find single-owner directories for a specific user
python fs_scans/parse_gpfs_scan.py fs_scans/20260111_csfs1_asp.list.list_all.log \
    --owner-id 12345 --min-depth 4

# Process compressed file, sort by file count
python fs_scans/parse_gpfs_scan.py fs_scans/20260111_csfs1_asp.list.list_all.log.xz \
    --sort-by files_recursive --max-results 100
```

## Log File Format

The parser expects GPFS policy scan output with lines in this format:

```
<node> inode gen snapshot  key=value pairs -- /path
```

Key fields extracted:
- `s=` FILE_SIZE (bytes)
- `u=` USER_ID (numeric)
- `p=` permissions (first char: `-`=file, `d`=directory)
- `ac=` ACCESS_TIME (timestamp)

Directory entries are skipped; only files contribute to statistics.

## Memory Efficiency

The parser uses a streaming approach with constant memory per directory:
- ~64 bytes per directory (6 integers, 2 datetimes, 1 boolean)
- No file-level data stored
- Single-owner tracking uses O(1) space (one int + one bool per directory)

For a filesystem with millions of files but hundreds of thousands of directories, memory usage remains manageable.

## Output Formats

### Table (stdout)

```
Directory Statistics (20 directories)
┏━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┓
┃ Directory  ┃ Files ┃ Size    ┃ Max Atime┃ Files(R)┃ Size (R) ┃Max At(R)┃ Owner ┃
┡━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━┩
│ /gpfs/...  │ 1,234 │ 45.6 GB │ 2024-01..│  56,789 │   1.2 TB │ 2025-12.│ 12345 │
└────────────┴───────┴─────────┴──────────┴─────────┴──────────┴─────────┴───────┘
```

### TSV (--output)

Tab-separated values with columns:
```
directory  file_count  total_size  max_atime  file_count_recursive  total_size_recursive  max_atime_recursive  owner_id  single_owner
```

## Available Data Files

| Filesystem | Size | Lines (approx) |
|------------|------|----------------|
| asp | 3 GB | ~10M |
| cisl | 36 GB | ~100M+ |
| eol | 6 GB | ~20M |
| hao | 40 GB | ~100M+ |

XZ-compressed versions are also available (roughly 10x smaller).

---

## Database Importer (scan_to_db.py)

For persistent storage and complex queries, use the database importer to load scan data into SQLite.

### Usage

```bash
python -m fs_scans.scan_to_db <input_file> [options]
```

### Options

| Option | Description |
|--------|-------------|
| `--db PATH` | Override database path (default: `fs_scans/<filesystem>.db`) |
| `-f, --filesystem NAME` | Override filesystem name (default: extracted from filename) |
| `--batch-size N` | Batch size for DB updates (default: 10000) |
| `-p, --progress-interval N` | Progress reporting interval (default: 1M lines) |
| `--replace` | Drop and recreate tables before import |
| `--echo` | Echo SQL statements (for debugging) |

### Examples

```bash
# Import a scan file (database auto-created as fs_scans/asp.db)
python -m fs_scans.scan_to_db fs_scans/20260111_csfs1_asp.list.list_all.log

# Import compressed file with custom database path
python -m fs_scans.scan_to_db fs_scans/20260111_csfs1_asp.list.list_all.log.xz --db /tmp/asp.db

# Replace existing data
python -m fs_scans.scan_to_db fs_scans/20260111_csfs1_asp.list.list_all.log --replace
```

### Two-Pass Algorithm

The importer uses a two-pass algorithm:

**Pass 1: Directory Discovery** - Identifies all directories and builds a normalized hierarchy in the database.

**Pass 2: Statistics Accumulation** - Re-scans the file to accumulate file statistics into each directory.

### Memory Optimization

Pass 1 uses a three-phase approach to minimize peak memory usage:

| Phase | Operation | Data Structures |
|-------|-----------|-----------------|
| 1a | Scan file | `seen_hashes` (8 bytes/dir) + `dir_tuples` (~26 bytes/dir) |
| 1b | Insert to DB | `dir_tuples` + `hash_to_id` (~16 bytes/dir) |
| 1c | Build lookup | `hash_to_id` + `path_to_id` (growing) |

**Key optimizations:**

1. **Hash-based discovery** - During Phase 1a, directories are tracked by their hash values (8 bytes) rather than full path strings (~60+ bytes). Only the minimal tuple `(parent_hash, basename, depth, own_hash)` is stored.

2. **Staged memory release** - Each data structure is explicitly deleted (`del`) as soon as it's no longer needed, preventing memory peaks from overlapping allocations.

3. **File re-scan for path mapping** - Phase 1c re-reads the input file (likely still in OS cache) to build the final `path_to_id` dictionary, avoiding the need to keep full paths in memory during discovery.

**Memory comparison:**

| Approach | Peak Memory per Directory |
|----------|---------------------------|
| Original (full paths) | ~128 bytes |
| Optimized (hash-based) | ~42 bytes |

This ~3x reduction is significant when processing filesystems with millions of directories.

### Database Schema

The importer creates two tables:

**directories** - Normalized directory hierarchy
- `dir_id` - Primary key
- `parent_id` - Foreign key to parent directory
- `name` - Directory basename
- `depth` - Depth in hierarchy (for efficient queries)

**directory_stats** - Aggregated statistics per directory
- `dir_id` - Foreign key to directories
- `file_count_nr` / `file_count_r` - Non-recursive / recursive file counts
- `total_size_nr` / `total_size_r` - Non-recursive / recursive sizes
- `max_atime_nr` / `max_atime_r` - Non-recursive / recursive max access times
- `owner_uid` - Single owner UID (NULL if multiple owners)
