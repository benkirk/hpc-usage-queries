# Filesystem Scan Tools

Tools for parsing filesystem scan log files (GPFS, Lustre, POSIX) and computing **directory-level metrics**. Provides a unified CLI with database import and query capabilities for large-scale filesystem analysis.

## Quick Start

```bash
# Install
pip install -e .

# Import a scan log (auto-detect format)
fs-scans import scan.log

# Query all filesystems
fs-scans query

# Query a specific filesystem
fs-scans query asp --min-size 10GiB -d 4

# Analyze access patterns (instant results)
fs-scans analyze --access-history
fs-scans analyze --file-size --owner jsmith

# Get help
fs-scans -h
fs-scans import -h
fs-scans query -h
fs-scans analyze -h
```

## Overview

These tools process GPFS scan logs and aggregate statistics at the directory level:

| Metric | Non-Recursive | Recursive |
|--------|---------------|-----------|
| File count | Direct children only | All descendants |
| Directory count | Direct children only | All descendants |
| Total size | Direct children only | All descendants |
| Max access time | Direct children only | All descendants |

Additionally, it tracks **single-owner directories** - directories where all recursive contents share a single owner (UID) and **single-group directories** - directories where all recursive contents share a single group (GID).

## Project Structure

The codebase is organized into modular components:

```
fs_scans/
├── cli/                    # Command-line interface
│   ├── main.py            # Unified CLI entry point (fs-scans)
│   ├── import_cmd.py      # Import subcommand
│   ├── query_cmd.py       # Query subcommand
│   ├── analyze_cmd.py     # Analyze subcommand (histograms)
│   └── common.py          # Shared CLI utilities (console, formatting, parsing)
├── core/                   # Core business logic
│   ├── models.py          # SQLAlchemy ORM models
│   ├── database.py        # Database connection and session management
│   └── query_builder.py   # SQL query construction
├── parsers/                # Scan file parsers
│   ├── base.py            # Base parser interface
│   ├── gpfs.py            # GPFS-specific parser
│   ├── lustre.py          # Lustre-specific parser
│   └── posix.py           # POSIX-specific parser
├── importers/              # Import pipeline
│   └── importer.py        # Multi-pass import algorithm
├── queries/                # Query engine
│   ├── query_engine.py    # Query execution and filtering
│   ├── display.py         # Result formatting and display
│   ├── access_history.py  # Access time histogram queries
│   ├── file_size.py       # File size histogram queries
│   └── histogram_common.py # Shared histogram utilities
└── wrappers/               # Legacy wrapper scripts (deprecated)
```

**Design principles:**
- **Separation of concerns**: CLI, business logic, and data access are cleanly separated
- **Modularity**: Each component has a single, well-defined responsibility
- **Testability**: Core logic is independent of CLI and can be tested in isolation
- **Extensibility**: New parsers and query features can be added without changing existing code

## Installation

Requires Python 3.10+ with `click`, `rich`, and `sqlalchemy`. From the project root:

```bash
source etc/config_env.sh
pip install -e .
```

This installs the unified `fs-scans` CLI with three subcommands:
- `fs-scans import` - Import scan logs into SQLite databases
- `fs-scans query` - Query directory statistics
- `fs-scans analyze` - Histogram-based analytics (access patterns, size distributions)

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
fs-scans query --summary

# Override via CLI (takes precedence over env var)
fs-scans query --data-dir /alt/path --summary

# Specify exact database file
fs-scans import input.log --db /tmp/custom.db

# Or via environment variable
export FS_SCAN_DB=/tmp/custom.db
fs-scans import input.log
```

## Log File Format

The parser expects GPFS policy scan output with lines in this format:

```
<node> inode gen snapshot  key=value pairs -- /path
```

Key fields extracted:
- `s=` FILE_SIZE (bytes)
- `u=` USER_ID (numeric)
- `g=` GROUP_ID (numeric)
- `p=` permissions (first char: `-`=file, `d`=directory)
- `ac=` ACCESS_TIME (timestamp)

Files contribute to file count, size, and access time statistics. Directories contribute to directory count statistics.

## Available Data Files

| Filesystem | Size | Lines (approx) |
|------------|------|----------------|
| asp | 3 GB | ~10M |
| cisl | 36 GB | ~100M+ |
| eol | 6 GB | ~20M |
| hao | 40 GB | ~100M+ |

Data files should be decompressed before processing.

---

## CLI Commands

### Import Command

Import filesystem scan logs into SQLite databases for persistent storage and complex queries.

**Usage:**

```bash
fs-scans import <input_file> [options]
```

**Supported formats:** GPFS, Lustre, POSIX (auto-detected from filename)

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
# Import a scan file (database auto-created as fs_scans/data/{filesystem}.db)
fs-scans import 20260111_csfs1_asp.list.list_all.log

# Import with explicit format
fs-scans import scan.log --format gpfs

# Import with custom database path
fs-scans import 20260111_csfs1_asp.list.list_all.log --db /tmp/asp.db

# Replace existing data
fs-scans import 20260111_csfs1_asp.list.list_all.log --replace

# Use parallel workers for faster parsing (best with uncompressed files)
fs-scans import 20260111_csfs1_asp.list.list_all.log --workers 4
```

### Multi-Pass Algorithm

The importer uses a multi-pass algorithm optimized for large filesystems:

**Pass 1: Directory Discovery** - Identifies all directories and builds a normalized hierarchy in the database using bulk-optimized level-by-level insertion.

**Pass 2a: Non-Recursive Stats & Histograms** - Re-scans the file to accumulate statistics for each entry's direct parent directory. Files contribute to file counts, sizes, and histograms; directories contribute to directory counts. Simultaneously builds access time and file size histograms per owner (files only). Optimized with vectorized bulk updates.

**Pass 2b: Recursive Aggregation** - Bottom-up SQL aggregation computes recursive stats (file counts, directory counts, sizes, access times, owner UID/GID) from non-recursive stats. Uses high-performance `UPDATE ... FROM` with CTEs to aggregate children stats in a single pass per depth level.

**Pass 3: Summary Tables** - Populates auxiliary tables for fast queries:
- **Phase 3a**: Resolves UIDs to usernames via `pwd.getpwuid()` and GIDs to groupnames via `grp.getgrgid()`, stores in `user_info` and `group_info` tables
- **Phase 3b**: Pre-aggregates per-owner statistics into `owner_summary` table and per-group statistics into `group_summary` table
- **Phase 3c**: Records scan metadata (source file, timestamps, totals) in `scan_metadata` table

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

The importer creates the following tables:

**directories** - Normalized directory hierarchy
- `dir_id` - Primary key
- `parent_id` - Foreign key to parent directory
- `name` - Directory basename
- `depth` - Depth in hierarchy (for efficient queries)

**directory_stats** - Aggregated statistics per directory
- `dir_id` - Foreign key to directories
- `file_count_nr` / `file_count_r` - Non-recursive / recursive file counts
- `dir_count_nr` / `dir_count_r` - Non-recursive / recursive directory counts
- `total_size_nr` / `total_size_r` - Non-recursive / recursive sizes
- `max_atime_nr` / `max_atime_r` - Non-recursive / recursive max access times
- `owner_uid` - Single owner UID (NULL if multiple owners, -1 if no files)
- `owner_gid` - Single group GID (NULL if multiple groups, -1 if no files)

**scan_metadata** - Scan provenance and totals
- `scan_id` - Primary key
- `source_file` - Original log filename
- `scan_timestamp` - Parsed from YYYYMMDD in filename
- `import_timestamp` - When the import occurred
- `filesystem`, `total_directories`, `total_files`, `total_size`

**owner_summary** - Pre-computed per-owner aggregates (enables fast `--group-by owner`)
- `owner_uid` - Primary key
- `total_size`, `total_files`, `directory_count`

**user_info** - UID-to-username cache
- `uid` - Primary key
- `username`, `full_name` (GECOS field)

**group_summary** - Pre-computed per-group aggregates (enables fast `--group-by group`)
- `owner_gid` - Primary key
- `total_size`, `total_files`, `directory_count`

**group_info** - GID-to-groupname cache
- `gid` - Primary key
- `groupname`

**access_histogram** - Pre-computed access time distribution (10 buckets)
- `owner_uid`, `bucket_index` - Composite primary key
- `file_count`, `total_size` - Files and bytes per bucket per owner
- Enables instant access-age queries without scanning directory_stats

**size_histogram** - Pre-computed file size distribution (10 buckets)
- `owner_uid`, `bucket_index` - Composite primary key
- `file_count`, `total_size` - Files and bytes per bucket per owner
- Enables instant size distribution queries without scanning directory_stats

**Histogram Buckets:**
- **Access Time** (10 buckets): < 1 Month, 1-3 Months, 3-6 Months, 6-12 Months, 1-2 Years, 2-3 Years, 3-4 Years, 5-6 Years, 6-7 Years, 7+ Years
- **File Size** (10 buckets): 0-1 KiB, 1-10 KiB, 10-100 KiB, 100 KiB-1 MiB, 1-10 MiB, 10-100 MiB, 100 MiB-1 GiB, 1-10 GiB, 10-100 GiB, 100 GiB+

---

### Query Command

Query directory statistics from SQLite databases with powerful filtering and sorting options.
Supports querying across all databases or a specific filesystem.

**Usage:**

```bash
fs-scans query [filesystem] [options]
```

The `filesystem` argument is optional and defaults to `all`, which queries all available `.db` files and combines results. Specify a filesystem name (e.g., `asp`, `cisl`) to query only that database.

### Options

| Option | Description |
|--------|-------------|
| `--data-dir DIR` | Override directory containing database files |
| `-d, --min-depth N` | Filter by minimum path depth |
| `--max-depth N` | Filter by maximum path depth |
| `-s, --single-owner` | Only show single-owner directories |
| `-u, --owner UID` | Filter to specific owner (UID or username) |
| `--mine` | Filter to current user's UID |
| `-P, --path-prefix PATH` | Filter to paths starting with prefix (mount point auto-stripped) |
| `-E, --exclude PATH` | Exclude path and descendants (mount point auto-stripped, can repeat) |
| `-N, --name-pattern PAT` | Filter by name (GLOB pattern); can repeat for OR matching |
| `-i, --ignore-case` | Make `--name-pattern` matching case-insensitive |
| `--min-size SIZE` | Min total recursive size (e.g., 500MB, 2GiB; default: 1GiB) |
| `--max-size SIZE` | Max total recursive size |
| `--min-files COUNT` | Min recursive file count (e.g., 500, 10K) |
| `--max-files COUNT` | Max recursive file count |
| `--group-by owner` | Show per-user summary instead of directory list |
| `--group-by group` | Show per-group summary instead of directory list |
| `-n, --limit N` | Limit results (default: 50, 0 for unlimited) |
| `--sort-by FIELD` | Sort by: `size_r`, `size_nr`, `files_r`, `files_nr`, `atime_r`, `path`, `depth` |
| `-o, --output FILE` | Write TSV output to file |
| `--accessed-before DATE` | Filter to max_atime_r before date (YYYY-MM-DD or Nyrs/Nmo) |
| `--accessed-after DATE` | Filter to max_atime_r after date (YYYY-MM-DD or Nyrs/Nmo) |
| `-v, --verbose` | Show additional columns (Depth) |
| `--leaves-only` | Only show leaf directories (dir_count_nr = 0) |
| `--show-total` | Show totals row at bottom of results |
| `--dir-counts` | Show directory counts (Dirs and Dirs(NR) columns) |
| `--summary` | Show database summary only |
| `--show-config` | Show data directory configuration and available databases |

### Examples

```bash
# Query all filesystems (default)
fs-scans query

# Query a specific filesystem
fs-scans query asp

# Filter to a specific path prefix (mount point auto-stripped)
fs-scans query cisl --path-prefix /cisl/users
fs-scans query cisl --path-prefix /glade/campaign/cisl/users  # Same result
fs-scans query cisl --path-prefix /gpfs/csfs1/cisl/users      # Same result

# Show only single-owner directories at depth 4+
fs-scans query -d 4 --single-owner

# Filter by access time (files not accessed in 3+ years)
fs-scans query --accessed-before 3yrs

# Filter by access time range (accessed 3-5 years ago)
fs-scans query --accessed-after 5yrs --accessed-before 3yrs

# Show only leaf directories (no subdirectories)
fs-scans query --leaves-only

# Filter directories by name pattern
fs-scans query -N "*scratch*"

# Multiple name patterns (OR matching)
fs-scans query -N "*scratch*" -N "*tmp*"

# Case-insensitive name pattern
fs-scans query -N "*SCRATCH*" -i

# Filter by size (default: directories >= 1GiB)
fs-scans query --min-size 100GiB

# Find large directories with few files
fs-scans query --min-size 10GiB --max-files 100

# Size range query
fs-scans query --min-size 1GiB --max-size 10GiB --leaves-only

# Disable default size filter to see all directories
fs-scans query --min-size 0

# Filter by both size and file count
fs-scans query --min-size 1GiB --min-files 1K

# Show per-user summary (uses pre-computed owner_summary table)
fs-scans query --group-by owner

# Per-user summary with filters (computes dynamically)
fs-scans query --group-by owner -d 4 -P /gpfs/csfs1/cisl

# Show per-group summary (uses pre-computed group_summary table)
fs-scans query --group-by group

# Per-group summary with filters (computes dynamically)
fs-scans query --group-by group -d 4 -P /gpfs/csfs1/cisl

# Sort by different fields
fs-scans query --sort-by files_r --limit 20  # Top 20 by file count
fs-scans query --sort-by atime_r             # Oldest by access time

# Export all directories to TSV
fs-scans query --limit 0 -o all_dirs.tsv

# Show database summary for all filesystems
fs-scans query --summary

# Show configuration and available databases
fs-scans query --show-config

# Query databases from a different directory
fs-scans query --data-dir /data/gpfs_scans --summary
```

### Performance Notes

**Size and file count filters** (`--min-size`, `--max-size`, `--min-files`, `--max-files`) leverage existing indexes on `total_size_r` and `file_count_r` to narrow the candidate set before applying unindexed name pattern GLOBs. This significantly improves query performance on large databases.

**Default filter:** By default, `--min-size 1GiB` is active to focus on large directories. This can be disabled with `--min-size 0`. There is no default on `--min-files` to allow finding large directories with few files (e.g., video archives).

**Name pattern filtering** uses SQLite GLOB (case-sensitive) or LIKE (case-insensitive with `-i`). Patterns with leading wildcards (e.g., `*scratch*`) cannot use indexes and require sequential scans, but size/file-count filters run first to minimize the scan set.

**Mount point normalization:** Path arguments (`-P`/`--path-prefix` and `-E`/`--exclude`) automatically strip known mount point prefixes (`/glade/campaign`, `/gpfs/csfs1`, `/glade/derecho/scratch`, `/lustre/desc1`). This allows users to provide full filesystem paths as they appear on the system, which are then normalized to match the database's stripped paths.

---

### Analyze Command

Generate histogram-based analytics for filesystem usage patterns. The analyze command provides instant insights into file access patterns and size distributions using pre-computed histogram tables.

**Usage:**

```bash
fs-scans analyze [filesystem] [options]
```

The `filesystem` argument is optional and defaults to `all`. Supports two types of analysis:
- `--access-history` - File distribution by last access time
- `--file-size` - File distribution by size

### Options

| Option | Description |
|--------|-------------|
| `--access-history` | Generate access time distribution histogram |
| `--file-size` | Generate file size distribution histogram |
| `-u, --owner TEXT` | Filter to specific owner (UID or username) |
| `--mine` | Filter to current user's UID |
| `-P, --path-prefix PATH` | Filter to paths (uses slower fallback computation) |
| `-d, --min-depth N` | Filter by minimum path depth (uses slower fallback) |
| `--max-depth N` | Filter by maximum path depth (uses slower fallback) |
| `--top-n N` | Number of top users to show per bucket (default: 10) |
| `--data-dir DIR` | Override directory containing database files |

### Performance Characteristics

The analyze command uses **two execution paths** depending on query filters:

**Fast Path (ORM-based, <100ms):**
- No path or depth filters
- Uses pre-computed `access_histogram` and `size_histogram` tables
- Instant results even for 100M+ file filesystems

**Slow Path (On-the-fly computation, 5-30 seconds):**
- With path filters (`-P`), depth filters (`-d`, `--max-depth`)
- Computes from `directory_stats` table
- Shows warning message: `"Note: Path filtering requires on-the-fly computation (slower)"`
- For `--file-size`: Uses approximate distribution based on directory-level averages

### Examples

```bash
# Access time distribution (all filesystems, fast path)
fs-scans analyze --access-history

# Access time for specific filesystem
fs-scans analyze asp --access-history

# Filter to specific user (fast path)
fs-scans analyze asp --access-history --owner jsmith
fs-scans analyze asp --access-history --owner 32455  # By UID

# Current user's data only (fast path)
fs-scans analyze --access-history --mine

# File size distribution (fast path)
fs-scans analyze all --file-size
fs-scans analyze cisl --file-size --owner jdoe

# With path filter (slow path - shows warning)
fs-scans analyze asp --access-history -P /asp/users
fs-scans analyze asp --file-size -P /asp/scratch

# Show more users per bucket
fs-scans analyze --access-history --top-n 20
```

### Output Format

Both histogram types display:
1. **Summary table** - Total files and data per bucket with percentages
2. **Per-user breakdown** - Top-N users for each bucket with their contribution

**Access Time Buckets (10):**
```
< 1 Month       - 0-30 days old
1-3 Months      - 30-90 days old
3-6 Months      - 90-180 days old
6-12 Months     - 180-365 days old
1-2 Years       - 1-2 years old
2-3 Years       - 2-3 years old
3-4 Years       - 3-4 years old
5-6 Years       - 5-6 years old
6-7 Years       - 6-7 years old
7+ Years        - 7+ years old
```

**File Size Buckets (10):**
```
0 - 1 KiB       - Tiny files
1 KiB - 10 KiB
10 KiB - 100 KiB
100 KiB - 1 MiB
1 MiB - 10 MiB
10 MiB - 100 MiB
100 MiB - 1 GiB
1 GiB - 10 GiB
10 GiB - 100 GiB
100 GiB+        - Huge files
```

### Example Output

```
================================================================================
Access Time Distribution
Scan date: 2026-01-11
Directory: /asp
Total Files: 10.2 M
Total Data: 234.4 TiB

Bucket                             Data                         # Files
================================================================================
< 1 Month                    1.2 TiB ( 0.50%)           3.9 K ( 0.04%)
1-3 Months                   2.5 TiB ( 1.05%)          12.5 K ( 0.12%)
3-6 Months                  25.9 TiB (11.07%)         500.9 K ( 4.93%)
...

User Data per Bucket               Data                         # Files
================================================================================
< 1 Month:                   1.2 TiB                             3.9 K
   1. jsmith                 1.1 TiB (90.40%)           1.7 K (43.41%)
   2. jdoe                 114.4 GiB ( 9.60%)           2.2 K (56.15%)
...
```

### Use Cases

**Access History Analysis:**
- Identify cold data for archival or deletion
- Find data retention policy violations
- Track data lifecycle patterns
- Prioritize cleanup efforts by age and size

**File Size Distribution:**
- Optimize storage tiering strategies
- Identify storage inefficiencies (many tiny files vs. few huge files)
- Plan compression strategies (small text files vs. binary blobs)
- Understand I/O patterns (many small files = metadata-heavy)

### Technical Notes

**Histogram Generation:**
- Histograms are pre-computed during `fs-scans import` (no performance penalty)
- Stored in `access_histogram` and `size_histogram` tables
- Bucket boundaries are fixed and consistent across all scans
- Per-owner tracking allows user-filtered queries without re-scanning

**Approximation for Path Filters:**
- File size histograms with path filters use directory-level average file sizes
- Assumes uniform file sizes within each directory
- Accurate for homogeneous data (e.g., simulation outputs, image collections)
- Less accurate for mixed directories (e.g., home directories)
- Warning displayed: `"Note: Size distribution is approximate for path-filtered queries"`

---

## Development Branch Status

This implementation is on the `histogram` branch. Key commits:
- `8715c61` - Add histogram collection during filesystem scan import
- `a1459f1` - Remove single-threaded fallback code from import pipeline

To merge into main, ensure all databases are re-imported to populate histogram tables.
