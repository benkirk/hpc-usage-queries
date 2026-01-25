# GPFS Policy Scan Tools

Tools for parsing GPFS policy scan log files and computing **directory-level metrics**. Provides a database importer for persistent storage and a query interface for analysis.

## Tools

| Tool | CLI Command | Purpose |
|------|-------------|---------|
| `scan_to_db.py` | `fs-scan-to-db` | Database importer - SQLite storage for complex queries |
| `query_db.py` | `query-fs-scan-db` | Query interface for the SQLite database |

## Overview

These tools process GPFS scan logs and aggregate statistics at the directory level:

| Metric | Non-Recursive | Recursive |
|--------|---------------|-----------|
| File count | Direct children only | All descendants |
| Total size | Direct children only | All descendants |
| Max access time | Direct children only | All descendants |

Additionally, it tracks **single-owner directories** - directories where all recursive contents share a single owner (user_id).

## Installation

Requires Python 3.10+ with `click`, `rich`, and `sqlalchemy`. From the project root:

```bash
source etc/config_env.sh
pip install -e .
```

This installs the CLI commands `fs-scan-to-db` and `query-fs-scan-db`.

## Configuration

Database location can be configured via environment variables or CLI options. Precedence (highest to lowest):

| Configuration | CLI Option | Environment Variable | Default |
|---------------|------------|---------------------|---------|
| Data directory | `--data-dir` | `FS_SCAN_DATA_DIR` | Module directory (`fs_scans/`) |
| Database file | `--db` | `FS_SCAN_DB` | `{data_dir}/{filesystem}.db` |

### Examples

```bash
# Use environment variable for data directory
export FS_SCAN_DATA_DIR=/data/gpfs_scans
query-fs-scan-db --summary

# Override via CLI (takes precedence over env var)
query-fs-scan-db --data-dir /alt/path --summary

# Specify exact database file
fs-scan-to-db input.log --db /tmp/custom.db

# Or via environment variable
export FS_SCAN_DB=/tmp/custom.db
fs-scan-to-db input.log
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

## Available Data Files

| Filesystem | Size | Lines (approx) |
|------------|------|----------------|
| asp | 3 GB | ~10M |
| cisl | 36 GB | ~100M+ |
| eol | 6 GB | ~20M |
| hao | 40 GB | ~100M+ |

Data files should be decompressed before processing.

---

## Database Importer (scan_to_db.py)

For persistent storage and complex queries, use the database importer to load scan data into SQLite.

### Usage

```bash
fs-scan-to-db <input_file> [options]
```

### Options

| Option | Description |
|--------|-------------|
| `--db PATH` | Override database file path (highest precedence) |
| `--data-dir DIR` | Override directory for database files |
| `-f, --filesystem NAME` | Override filesystem name (default: extracted from filename) |
| `--batch-size N` | Batch size for DB updates (default: 50000) |
| `-p, --progress-interval N` | Progress reporting interval (default: 1M lines) |
| `--replace` | Drop and recreate tables before import |
| `-w, --workers N` | Number of worker processes for parsing (default: 1) |
| `--echo` | Echo SQL statements (for debugging) |

### Examples

```bash
# Import a scan file (database auto-created as fs_scans/asp.db)
fs-scan-to-db fs_scans/20260111_csfs1_asp.list.list_all.log

# Import with custom database path
fs-scan-to-db fs_scans/20260111_csfs1_asp.list.list_all.log --db /tmp/asp.db

# Replace existing data
fs-scan-to-db fs_scans/20260111_csfs1_asp.list.list_all.log --replace

# Use parallel workers for faster parsing (best with uncompressed files)
fs-scan-to-db fs_scans/20260111_csfs1_asp.list.list_all.log --workers 4
```

### Multi-Pass Algorithm

The importer uses a multi-pass algorithm optimized for large filesystems:

**Pass 1: Directory Discovery** - Identifies all directories and builds a normalized hierarchy in the database using bulk-optimized level-by-level insertion.

**Pass 2a: Non-Recursive Stats** - Re-scans the file to accumulate statistics for each file's direct parent directory only. Optimized with vectorized bulk updates.

**Pass 2b: Recursive Aggregation** - Bottom-up SQL aggregation computes recursive stats from non-recursive stats. Uses high-performance `UPDATE ... FROM` with CTEs to aggregate children stats in a single pass per depth level.

This approach is significantly faster than computing recursive stats during file scanning. Instead of walking up all ancestors for every file (O(files × depth)), Pass 2a processes each file once (O(files)), and Pass 2b aggregates in SQL (O(depth_levels)).

### Pass 1 Implementation

Since GPFS scan files explicitly list all directories as separate lines, Pass 1 uses a memory-optimized two-phase approach with SQLite staging:

| Phase | Operation | Data Structures |
|-------|-----------|-----------------|
| 1a | Stream to staging | SQLite `staging_dirs` table (parallelizable) |
| 1b | Bulk Insert to DB | Level-by-level bulk insertion from staging |

**How it works:**

1. **Phase 1a** - Streams directories to a temporary SQLite staging table with batch inserts (10K rows per batch). Extracts inode and fileset_id for unique identification.
2. **Phase 1b** - Optimized level-by-level insertion. For each depth, it bulk inserts directories, retrieves assigned IDs in a single query to update the in-memory `path_to_id` map, and bulk inserts the corresponding stats records. This reduces DB round-trips from $O(Total Dirs)$ to $O(Depth)$.
3. Staging table is dropped after use to reclaim space.

**Memory optimization:** By using SQLite staging instead of an in-memory list, peak memory is reduced from ~220 bytes/dir to ~120 bytes/dir (only `path_to_id` dict in memory). For 1M directories, this saves ~100MB of peak memory.

**Progress tracking:** Phase 1 reports line count, directory count, and inferred file count. Pass 2 uses the known line count for a determinate progress bar with percentage completion.

No deduplication or parent directory discovery is needed since all directories are explicitly listed in the scan output.

### Parallel Processing

Both Phase 1a (directory discovery) and Pass 2 (stats accumulation) support parallel processing with the `--workers` flag:

- Workers handle CPU-bound regex parsing of log lines
- Main process handles file I/O and database writes (SQLite single-writer constraint)
- Queue-based communication between workers and main process
- Phase 1b remains sequential (parent-child ordering requirement)

**Note:** Parallel workers are most effective when the input file is stored on fast local storage. Upon completion, the tool reports the total runtime and the final size of the generated SQLite database.

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

---

## Database Query Tool (query_db.py)

Query directory statistics from the SQLite database with filtering and sorting options.
Supports querying across all databases or a specific filesystem.

### Usage

```bash
query-fs-scan-db [filesystem] [options]
```

The `filesystem` argument is optional and defaults to `all`, which queries all available `.db` files and combines results. Specify a filesystem name (e.g., `asp`, `cisl`) to query only that database.

### Options

| Option | Description |
|--------|-------------|
| `--data-dir DIR` | Override directory containing database files |
| `-d, --min-depth N` | Filter by minimum path depth |
| `--max-depth N` | Filter by maximum path depth |
| `-s, --single-owner` | Only show single-owner directories |
| `-u, --owner-id UID` | Filter to specific owner UID |
| `-P, --path-prefix PATH` | Filter to paths starting with prefix |
| `-n, --limit N` | Limit results (default: 50, 0 for unlimited) |
| `--sort-by FIELD` | Sort by: `size_r`, `size_nr`, `files_r`, `files_nr`, `atime_r`, `path`, `depth` |
| `-o, --output FILE` | Write TSV output to file |
| `--accessed-before DATE` | Filter to max_atime_r before date (YYYY-MM-DD or Nyrs/Nmo) |
| `--accessed-after DATE` | Filter to max_atime_r after date (YYYY-MM-DD or Nyrs/Nmo) |
| `-v, --verbose` | Show additional columns (Depth) |
| `--leaves-only` | Only show leaf directories (no subdirectories) |
| `--summary` | Show database summary only |

### Examples

```bash
# Query all filesystems (default)
query-fs-scan-db

# Query a specific filesystem
query-fs-scan-db asp

# Filter to a specific path prefix
query-fs-scan-db asp --path-prefix /gpfs/csfs1/asp/username

# Show only single-owner directories at depth 4+
query-fs-scan-db -d 4 --single-owner

# Filter by access time (files not accessed in 3+ years)
query-fs-scan-db --accessed-before 3yrs

# Filter by access time range (accessed 3-5 years ago)
query-fs-scan-db --accessed-after 5yrs --accessed-before 3yrs

# Show only leaf directories (no subdirectories)
query-fs-scan-db --leaves-only

# Export all directories to TSV
query-fs-scan-db --limit 0 -o all_dirs.tsv

# Show database summary for all filesystems
query-fs-scan-db --summary

# Query databases from a different directory
query-fs-scan-db --data-dir /data/gpfs_scans --summary
```
