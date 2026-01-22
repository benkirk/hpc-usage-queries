# GPFS Policy Scan Log Parser

A streaming parser for GPFS policy scan log files that computes **directory-level metrics** without storing file-level data. This is a prototype for future "cs-queries" database tooling.

## Overview

The parser processes GPFS scan logs and aggregates statistics at the directory level:

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
