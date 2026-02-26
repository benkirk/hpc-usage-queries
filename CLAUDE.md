# CLAUDE.md — HPC Usage Queries

## Project Overview

Two **wholly independent** modules in one repo. Never mix their concerns.

| Module | Purpose | CLI |
|--------|---------|-----|
| `job_history/` | PBS job history, charging, daily summaries | `jobhist` |
| `fs_scans/` | Filesystem metadata analysis (GPFS/Lustre) | `fs-scans` |

## Tests

```bash
pytest                        # both suites (283 tests)
pytest job_history/tests/     # job_history only
pytest fs_scans/tests/        # fs_scans only
```

Tests live **inside** each module: `job_history/tests/` and `fs_scans/tests/`.
Shared fixtures (in-memory SQLite DB, job data) are in `job_history/tests/conftest.py`.
`fs_scans/tests/` has no shared fixtures.

**Always run tests before committing.**

## CLI Entry Points

```bash
# Unified CLIs
jobhist --help          # history | resource | sync subcommands
fs-scans --help         # import | query | analyze subcommands

# Selective-deployment wrappers (for controlled access)
jobhist-sync            # → jobhist sync   (restrict to admins)
jobhist-history         # → jobhist history
jobhist-resource        # → jobhist resource
fs-scans-import         # → fs-scans import
fs-scans-query          # → fs-scans query
fs-scans-analyze        # → fs-scans analyze
```

## Databases

- `data/casper.db`, `data/derecho.db` — gitignored, per-machine SQLite
- `get_session("derecho")` / `get_session("casper")` from `job_history.database`
- Machine name passed via `-m`/`--machine` on all CLI commands
- Migration script: `bin/update_jobs_db.sh` — adds new columns idempotently

## job_history Architecture

### Schema key points
- `jobs` table: normalized FKs (`user_id`, `account_id`, `queue_id`) to lookup tables
- `user`, `account`, `queue` are **`@hybrid_property`** — look like text columns to app code but use integer FK joins
- `job_charges`: pre-computed `cpu_hours`, `gpu_hours`, `memory_hours`, `qos_factor` (default 1.0)
- `daily_summary`: pre-aggregated by `(date, user_id, account_id, queue_id)`; NULL FKs = NO_JOBS marker rows

### Critical patterns

**Bulk insert with FKs** — use ORM not Core API:
```python
session.bulk_insert_mappings(Model, list_of_dicts, render_nulls=True)
# NOT: sqlite_insert().values(list_of_dicts)  ← causes CompileError
```

**Hybrid property setters** defer FK resolution via `_pending_*` attributes; a
`before_flush` Session event listener resolves them (builds lookup cache, handles
missing tables gracefully).

**Datetime comparison** — SQLite stores naive datetimes; parsers produce UTC-aware.
Normalize before comparing: `dt.replace(tzinfo=None)`.

### Key files
| File | Role |
|------|------|
| `job_history/models.py` | ORM models: Job, JobCharge, DailySummary, JobRecord, lookup tables |
| `job_history/queries.py` | `JobQueries` class — high-level query API |
| `job_history/charging.py` | `derecho_charge()`, `casper_charge()` — machine-specific rules |
| `job_history/summary.py` | `generate_daily_summary()` — aggregates jobs → daily_summary |
| `job_history/sync.py` | Import pipeline: FK resolution, charge calculation |
| `job_history/sync_cli/sync.py` | `jobhist sync` Click command |
| `job_history/cli.py` | `history` and `resource` Click groups + all subcommands |
| `job_history/SCHEMA.md` | Full schema documentation |

## fs_scans Architecture

### Key files
| File | Role |
|------|------|
| `fs_scans/core/models.py` | ORM models: Directory, DirectoryStats, histograms |
| `fs_scans/core/query_builder.py` | `DirectoryQueryBuilder` — fluent filter API |
| `fs_scans/importers/importer.py` | Multi-pass import (directory discovery → stats → aggregation) |
| `fs_scans/parsers/` | GPFS, Lustre, POSIX parsers |
| `fs_scans/queries/` | Query engine + histogram analytics |
| `fs_scans/cli/` | `import_cmd`, `query_cmd`, `analyze_cmd` |

### Performance notes
- Import is 3-pass: directory discovery → non-recursive stats + histograms → recursive aggregation
- `access_histogram` and `size_histogram` tables enable `<100ms` analytics (fast path)
- Path/depth filters force slower on-the-fly computation from `directory_stats`

## Commit Style

- Brief imperative subject line (50 chars ideal)
- Body explains *why*, not *what*
- Co-Authored-By trailer: `Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>`
- **Never auto-commit** — only commit when explicitly asked
- **Never push** without explicit instruction

## Current Branch

`charging_calucations` (note: intentional misspelling in branch name)
