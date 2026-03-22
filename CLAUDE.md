# CLAUDE.md — HPC Usage Queries

## Project Overview

Two **wholly independent** modules in one repo. Never mix their concerns.

| Module | Purpose | CLI |
|--------|---------|-----|
| `job_history/` | PBS job history, charging, daily summaries | `jobhist` |
| `fs_scans/` | Filesystem metadata analysis (GPFS/Lustre) | `fs-scans` |

## Tests

```bash
pytest                        # both suites (340+ tests)
pytest job_history/tests/     # job_history only (217 tests)
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
- Migration script: `bin/update_jobs_db.py` — adds new columns idempotently (SQLite + PostgreSQL)

## job_history Architecture

### Schema key points
- `jobs` table: normalized FKs (`user_id`, `account_id`, `queue_id`) to lookup tables
- `user`, `account`, `queue` are **`@hybrid_property`** — look like text columns to app code but use integer FK joins
- **Timestamps are naive UTC** — Unix epoch → UTC then `tzinfo=None` stripped before storage.
  psycopg2 converts tz-aware datetimes to the PG server's local timezone when writing to
  `TIMESTAMP WITHOUT TIME ZONE`; naive values bypass that conversion and are portable across
  any PG server timezone and SQLite.
- `job_charges`: pre-computed `cpu_hours`, `gpu_hours`, `memory_hours`, `qos_factor`;
  **1:1 with jobs enforced by `trg_ensure_job_charge` trigger** (fires AFTER INSERT ON jobs,
  inserts placeholder with `charge_version=0`; sync overwrites with `charge_version=1`)
- `daily_summary`: pre-aggregated by `(date, user_id, account_id, queue_id)`; NULL FKs = NO_JOBS marker rows;
  stores both raw hours (`cpu_hours`) and QoS-weighted charges (`cpu_charges = cpu_hours × qos_factor`)
- Day boundaries use Mountain Time midnight → naive UTC for comparisons (matches stored `end` timestamps)

### Critical patterns

**Bulk insert with FKs** — use ORM not Core API:
```python
session.bulk_insert_mappings(Model, list_of_dicts, render_nulls=True)
# NOT: sqlite_insert().values(list_of_dicts)  ← causes CompileError
```

**Hybrid property setters** defer FK resolution via `_pending_*` attributes; a
`before_flush` Session event listener resolves them (builds lookup cache, handles
missing tables gracefully).

**Datetime comparison** — all datetimes stored as naive UTC. Parsers call
`normalize_datetime_to_naive(dt)` before comparing against DB values.

**`db_available(machine)`** — lives in `job_history.database` (not `qhist_plugin`).

**Sync flags** — `jobhist sync` supports five modes, mutually exclusive:
- `--incremental`: insert new records only; fills missing charges for existing records; re-summarizes only if new records inserted. Safe for frequent intra-day crons.
- `--upsert`: re-parse logs, update existing Job/JobCharge/JobRecord rows (via `_update_batch()`); bypasses summarized-day skip; always regenerates summaries.
- `--recalculate`: recompute charges from DB jobs without re-parsing logs (uses `_recalculate_charges()` querying by Mountain-Time day boundaries); regenerates summaries. Use for historical backfill or charging-rule changes.
- `--resummarize`: recompute `daily_summary` only from current DB state, no logs needed.
- plain (default): insert new records, fill missing charges for existing records, summarize if any fetched.

**Charge invariant** — every job row is guaranteed to have a `job_charges` row. Enforced
at DB level by `trg_ensure_job_charge` trigger (created by `_ensure_db_triggers()` in
`session.py`, called from `init_db()`). Application code uses `_upsert_charges()` to
overwrite placeholders. Live on `casper_jobs` and `derecho_jobs` PostgreSQL DBs.

**`DerechoRecord`** — vendored at `job_history/_vendor/pbs_parser_ncar/ncar.py`
(underscore rename makes it a proper Python package). Imported via standard dotted
path `job_history._vendor.pbs_parser_ncar.ncar`; pickle round-trips work without
any shims. See `_get_record_class()` in `sync/pbs.py`.

### Key files
| File | Role |
|------|------|
| `job_history/database/models.py` | ORM models: Job, JobCharge, DailySummary, JobRecord, lookup tables |
| `job_history/database/session.py` | Engine/session factory, `db_available()`, PRAGMA tuning, `init_db` |
| `job_history/queries/jobs.py` | `JobQueries` class — high-level query API |
| `job_history/database/session.py` | Engine/session factory, `_ensure_db_triggers()`, `init_db()` |
| `job_history/sync/base.py` | `SyncBase` ABC; full sync lifecycle; `_compute_charges_for_jobs()`, `_upsert_charges()`, `_fill_missing_charges()`, `_recalculate_charges()`; `UPDATABLE_JOB_FIELDS` |
| `job_history/sync/pbs.py` | PBS field parsers, `SyncPBSLogs` driver; `parse_pbs_timestamp()` → naive UTC |
| `job_history/sync/charging.py` | `SystemCharging` ABC + `DerechoCharging`, `CasperCharging` |
| `job_history/sync/summary.py` | `generate_daily_summary()` — naive UTC bounds, QoS-weighted charges |
| `job_history/sync/cli.py` | `jobhist sync` Click command (`--upsert`, `--incremental`, `--recalculate`, `--resummarize`) |
| `job_history/cli.py` | `history` and `resource` Click groups + all subcommands |
| `job_history/_vendor/pbs_parser_ncar/ncar.py` | Vendored `DerechoRecord` (extends `PbsRecord` with power metrics) |
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
