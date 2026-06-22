# Plan: PostgreSQL (CNPG) backend for `fs_scans/` via SQLite→PG consolidation

## Context

`fs_scans/` produces, once a week and in parallel (PBS, `fs_scans/PBS/`), ~13 per-collection
SQLite databases (`cgd.db`, `acom.db`, …) totaling ~43 GB, then `rsync`s them to a deploy
directory. These are **not** separate filesystems — they are org-level *collections* of one
filesystem, `/glade/campaign/` (bind-mount of `/gpfs/csfs1/`); the split exists only so the
weekly run can scan/build them in parallel and keep each file manageable.

The goal is to **additionally** expose this data as a **networked database visible on a webserver**.
Both outputs are first-class and published every week: the existing `.db` files stay for local
CLI access on this machine (`sqlite` backend, the default), and the same data is loaded into a CNPG
(Cloud Native Postgres) instance for the networked/webserver path (`postgres` backend). Consolidation
is purely **additive** — it does not replace the `.db` rsync deploy.

The CNPG server (`csg-postgres.k8s.ucar.edu:5432`) is already live — `job_history` runs against it
(`JOB_HISTORY_DB_BACKEND=postgres`). Credentials live in the gitignored `.env` as `CIRRUS_PG_*` base
vars referenced by `${...}` interpolation; `fs_scans` will reuse the same server/credentials.

**Decided approach** (see Strategy/Layout below): keep the proven, fast SQLite generation pipeline
**completely unchanged**, and add a new post-creation step that bulk-loads each finished `.db` into
Postgres, builds indexes *after* load (preserving the deferred-indexing performance win), and
atomically swaps it into place. This is the lowest-risk path: it doesn't touch the 2.5h cgd import,
its SQLite-only pragmas, or the parallel PBS fan-out, and it gives a genuinely non-disruptive weekly
swap that direct-to-Postgres generation cannot.

### Layout (decided)
- **Database = filesystem.** One PG database `campaign` (future Lustre `desc1` → a second database).
- **Schema = collection.** One schema per collection (`cgd`, `acom`, `univ`, …), each holding the
  identical table set. Maps the 13 `.db` files → 13 schemas, 1:1.
- **Swap = `ALTER SCHEMA RENAME`** — transactional, millisecond-scale lock, per-collection
  (a slow/failed `cgd` load never blocks the other 12), live readers on other schemas undisturbed.
- Webserver role is **read-only**; `query all` fans across schemas in one connection.

### Capacity & disk space — the PRIMARY constraint
The CNPG instance (`csg-postgres`, namespace `pg-testing`, 2 pods primary+replica, each a **512 Gi PVC ≈ 502.9 GB usable**) is **shared with production** `job_history`: `casper_jobs` 43 GB + `derecho_jobs` 30 GB + `system_status` 527 MB. Current data-dir usage **75.1 GB / 502.9 GB (15%), ~427 GB free**. Filling this PVC or exhausting connections would jeopardize production job_history — so disk is the gating concern, not an afterthought.

- **PG footprint is larger than SQLite.** ~43 GB of `.db` will likely become **~80–150 GB** in Postgres (per-row tuple overhead + the ~9 secondary indexes on `directory_stats` over tens of millions of rows; cgd alone is 57M+57M rows). **This must be measured empirically (see perf testing), not assumed.**
- **Swap transiently multiplies footprint.** Holding `<c>` + `<c>_old` + `<c>_staging` simultaneously can reach ~3× one collection. Mitigation, applied in the consolidate step: process collections **one at a time**, drop `<c>_old` **immediately after** a successful swap (don't defer to next week), and `VACUUM`/let autovacuum reclaim. Never stage all 13 at once.
- **Mitigations to evaluate against measured sizes:** trim the index set to only what the web read path needs (the full SQLite index set may be overkill for Postgres); consider `BigInteger` only where required; keep consolidation connection count tiny (1–2). If measured footprint threatens the shared PVC, escalate to the CNPG admin for a dedicated database/PVC or a larger volume **before** enabling the weekly job.
- A successful smoke + perf pass on small/medium collections **gates** running cgd and enabling the weekly PBS job.

## Reference: how `job_history/` already does dual-backend (mirror these)
- `job_history/database/config.py` — `JobHistoryConfig`: `*_DB_BACKEND` env var, `PG_HOST/PORT/USER/PASSWORD/REQUIRE_SSL`, `validate_postgres()`.
- `job_history/database/session.py` — `get_engine()` branches on backend to build the URL; `_ensure_pg_database()` (AUTOCOMMIT admin connection creates the DB); `_set_sqlite_pragma` attached only for sqlite engines; `_ensure_db_triggers()` branches on `engine.dialect.name`.
- `job_history/queries/builders.py:17-46` — dialect-aware SQL via `@compiles()` / `@compiles(..., 'postgresql')` custom `FunctionElement`.
- `job_history/sync/base.py:417-452` — dialect-aware upsert (`pg_insert` vs `sqlite_insert`, `.on_conflict_do_update`).
- `pyproject.toml` — `psycopg2-binary` under `[project.optional-dependencies] postgres`.

---

## Implementation

### 1. Backend abstraction (`fs_scans/core/`)

**New `fs_scans/core/config.py`** (model on `job_history/database/config.py`):
- `load_dotenv()` at import; `FsScanConfig` class.
- `DB_BACKEND = os.getenv("FS_SCAN_DB_BACKEND", "sqlite").lower()`.
- SQLite: defer to existing `get_data_dir()` precedence (keep `--data-dir`/`FS_SCAN_DATA_DIR` —
  the PBS job sets `FS_SCAN_DATA_DIR=$(pwd)` at `fs_scan.pbs:53`, so the dynamic override must survive).
- Postgres: `PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_REQUIRE_SSL` (read from `FS_SCAN_PG_*`), plus `PG_DB_NAME = os.getenv("FS_SCAN_PG_DB", "campaign")` (the filesystem→database name).
- `pg_schema_name(collection)` → `collection.lower()` (override via env). `validate_postgres()` fail-fast.

**`.env` / `.env.example`** — add an `FS_SCAN_*` block mirroring the existing `JOB_HISTORY_*` block. Reuse the same CNPG server by referencing the existing base vars: `FS_SCAN_PG_HOST=${CIRRUS_PG_HOST}`, `FS_SCAN_PG_USER=${CIRRUS_PG_USER}`, `FS_SCAN_PG_PASSWORD=${CIRRUS_PG_PASSWORD}`, `FS_SCAN_PG_PORT=5432`, `FS_SCAN_PG_DB=campaign`. Leave `FS_SCAN_DB_BACKEND` unset/`sqlite` for local CLI (the default); the webserver and the consolidate PBS job set `FS_SCAN_DB_BACKEND=postgres`.

**Refactor `fs_scans/core/database.py`** (mirror `job_history/database/session.py`):
- `get_engine(filesystem, *, schema=None, …)` branches on `FsScanConfig.DB_BACKEND`:
  - sqlite: current behavior unchanged (`sqlite:///…`, `check_same_thread=False`); keep `_set_sqlite_pragma` listener **sqlite-only**.
  - postgres: `validate_postgres()`, URL `postgresql+psycopg2://…/{PG_DB_NAME}`, `connect_args={"sslmode":"require"}` when required, **and set search_path via `connect_args={"options": f"-csearch_path={schema},public"}`** so the existing raw `text()` queries (bare table names) resolve to the target schema with no rewrite. This is the key enabler for schema-per-collection.
- Cache key must include backend + schema + pool kwargs (current cache keys on path string only, `database.py:178`).
- `get_db_path()`: raise for non-sqlite (only meaningful for sqlite); add `get_db_url()` for display.
- `init_db`: add postgres branch — `_ensure_pg_database()` (AUTOCOMMIT, port from `session.py:280-301`) once for `campaign`, then driven per-schema by the consolidate command.

**`get_all_filesystems()` (`fs_scans/queries/query_engine.py:50-60`)** — make backend-aware:
- sqlite: unchanged glob of `*.db`.
- postgres: query `information_schema.schemata` for `campaign`, excluding `public`, `information_schema`, `pg_*`, and `*_staging`/`*_old`.

### 2. Consolidation step

**New subpackage `fs_scans/consolidate/`** with `consolidator.py`:
`consolidate_sqlite_to_postgres(sqlite_path, collection, *, swap=True, batch_size=…)`:
1. `validate_postgres()`; ensure `campaign` DB exists (`_ensure_pg_database`).
2. `DROP SCHEMA IF EXISTS <c>_staging CASCADE; CREATE SCHEMA <c>_staging;`
3. Create tables in `<c>_staging` (`Base.metadata.create_all` against the schema) **without secondary indexes and without the self-FK** on `directories.parent_id`.
4. **Bulk load via psycopg2 `COPY` (`copy_expert`)** — far faster than executemany for cgd's ~57M+57M rows. Stream from SQLite read-only (`?mode=ro&uri=true`) with `yield_per` so memory stays bounded. Load in FK order: `directories` → `directory_stats` → independent tables (`scan_metadata`, `owner_summary`, `group_summary`, `user_info`, `group_info`, `access_histogram`, `size_histogram`). Self-referential `parent_id`: add the FK constraint **after** the full COPY (one validated `ALTER TABLE ADD CONSTRAINT`), matching the deferred-indexing philosophy.
5. **Deferred indexing:** reuse the exact statements in `fs_scans/importers/add_table_indexing.py` (already `CREATE INDEX IF NOT EXISTS`, portable) against `<c>_staging`. Then `ANALYZE` the schema (PG analogue of `PRAGMA optimize`).
6. **GRANT** `USAGE` on schema + `SELECT` on all tables to the read-only web role (do this *before* the swap — newly COPY-created tables don't inherit grants).
7. **Atomic swap** (when `swap=True`), one transaction:
   ```sql
   ALTER SCHEMA <c> RENAME TO <c>_old;        -- only if <c> exists
   ALTER SCHEMA <c>_staging RENAME TO <c>;
   ```
   Drop `<c>_old` after a short grace (next run, or end-of-job) so in-flight reads finish cleanly.
8. Per-collection failures isolated — one bad `.db` must not block the other 12 swaps; emit a summary.

**New `fs_scans/cli/consolidate_cmd.py`** (Click, registered in `fs_scans/cli/main.py` beside import/query/analyze): `[FILESYSTEM]...` positional or `--all`; `--db`/`--data-dir` (mirror `import_cmd.py`); `--no-swap`, `--keep-old`, `--batch-size`. Reuse `console`/helpers from `fs_scans/cli/common.py`. Add `fs-scans-consolidate` wrapper in `fs_scans/wrappers/` + `pyproject.toml [project.scripts]` for selective deployment.

### 3. Portability fixes

**Must-fix (read path executes against Postgres on the webserver):**
- **`GLOB` → dialect-aware** — `fs_scans/core/query_builder.py:195`. `build()` emits raw SQL strings executed via `text()`, so thread the bind dialect into `build(dialect=…)` and emit: sqlite `name GLOB :pat` (case-sensitive) / `LIKE` (insensitive); postgres POSIX regex `name ~ :pat` (convert `*`→`.*`, `?`→`.`, anchor `^…$`, escape metachars) / `~*` or `ILIKE` for insensitive. (The `@compiles` element pattern from `builders.py` applies only to ORM-expression call sites, if any.)
- **`sqlite_master` table-existence check** — `fs_scans/queries/histogram_common.py:221`. Replace `SELECT name FROM sqlite_master …` with dialect-agnostic `inspect(engine).has_table(name, schema=…)`.
- **Datetime binding** — `query_builder.py:145,158` bind `strftime(...)` strings; bind `datetime` objects directly instead (real PG `TIMESTAMP` columns; avoid fragile string-vs-timestamp casts).

**Lower priority (consolidation path runs these only against SQLite, but cheap + future-proof):**
- Guard `configure_sqlite_pragmas`/`finalize_sqlite_pragmas` (`fs_scans/importers/file_handling.py:78-98`) with `if dialect == "sqlite"`.
- `INSERT OR REPLACE` → dialect-aware `on_conflict_do_update` (`fs_scans/importers/pass3.py:68,100`), porting `job_history/sync/base.py:417-452`.

**Verified portable (no change):** `JOIN … USING (dir_id)`, `WITH RECURSIVE` CTEs, `||` concat, `CREATE INDEX IF NOT EXISTS`. SQLAlchemy emits correct per-dialect DDL for the `Integer` PK; consolidation COPYs explicit `dir_id` values (no sequence fix needed — published schema is read-only).

**Decide now:** make `Directory.dir_id`/`parent_id` `BigInteger`? cgd is 57M today (fine for 32-bit), but changing later forces a re-import.

### 4. PBS workflow (`fs_scans/PBS/`)

- **New `fs_scans/PBS/consolidate.pbs`** — single job after the existing `collect_results` rsync step. Runs `fs-scans consolidate --all` with `FS_SCAN_DB_BACKEND=postgres` + `FS_SCAN_PG_*` set (from a `.env`/`etc/config_env.sh`). Modest local resources (streams to PG; no `mem=128G`), generous walltime. The PG-side index build + ANALYZE for cgd is the main runtime unknown — set `maintenance_work_mem` high for the session.
- **Modify `fs_scans/PBS/submit_all.sh:57-59`** — add `qsub -W depend=afterok:<collect_jobid>` for `consolidate.pbs` (use `afterok`, not `afterany`).
- **Keep the existing `.db` rsync deploy unchanged** — it is the co-equal local-CLI path, not a fallback. After the weekly run, both outputs are current: `.db` files in the deploy dir (read by `sqlite` backend, the CLI default) and the CNPG schemas (read by `postgres` backend, the webserver). Backend is selected purely by `FS_SCAN_DB_BACKEND`.

---

## Execution & delivery

Already on branch `cnpg_fs_scans` (clean). Land as a **series of focused commits**, each self-contained and leaving `pytest` green, then open a **PR against `staging`** (not `main`). Per CLAUDE.md: never auto-commit/push — commit only when asked, and use the `Co-Authored-By: Claude` trailer.

Proposed commit sequence:
1. **Backend abstraction** — new `fs_scans/core/config.py`; refactor `fs_scans/core/database.py` to be backend-aware (sqlite stays the default → zero behavior change for local CLI); `.env`/`.env.example` `FS_SCAN_*` block. Existing tests pass unchanged.
2. **Portability fixes** — GLOB→dialect-aware, `sqlite_master`→`has_table`, datetime binding, pragma guards, `INSERT OR REPLACE`→`on_conflict_do_update`; add no-DB unit tests for the dialect branches.
3. **Backend-aware discovery** — `get_all_filesystems()` + any read-path schema handling for postgres.
4. **Consolidation** — `fs_scans/consolidate/consolidator.py`, `fs_scans/cli/consolidate_cmd.py`, wrapper + `pyproject.toml` script; PG-gated tests (round-trip, swap, disk-cleanup).
5. **PBS wiring** — `fs_scans/PBS/consolidate.pbs` + `submit_all.sh` dependency (gated on perf results).
6. **Docs** — `fs_scans/README.md` / `CLAUDE.md` for the new backend + `consolidate` command.

(Commit 5 is wired only after the performance pass below succeeds.)

## Performance testing (against the current production `.db` files)

Run **manually** before enabling the weekly job. Consolidation reads the existing `.db` files (on glade) and writes to CNPG — run it on a glade node that can reach `csg-postgres`; use the MacBook-on-VPN psql access for inspection/verification. Drive smallest → largest so a problem surfaces cheaply:
`asp` (10 MiB) → a mid one (`cisl` ~680 MiB / `acom` ~1 GB) → `univ` (4.7 GB) / `uwyo` (6.6 GB) → **`cgd` (16 GB) last**.

For each collection record:
- **Consolidation wall time**, broken into COPY / FK-add / index-build / ANALYZE phases.
- **Resulting PG size** via `pg_total_relation_size` per table + `SELECT pg_size_pretty(...)` for the schema; track free-space delta on the PVC (section 5 of `cirrus_healthcheck.sh`).
- **Transient peak** during swap (staging + live + old) to validate the one-at-a-time + immediate-drop strategy stays within free space.
- **Query parity + latency**: run representative `fs-scans query` / `analyze` against the PG schema vs the SQLite `.db`; assert identical top-N results and compare latency.

Decision gates from the measurements: final index set (trim if PG footprint is too large), `_old` retention policy, whether the shared PVC needs admin escalation before cgd/weekly enablement.

## Verification

- `pytest fs_scans/tests/` — must stay green on machines without Docker/PG (gate PG tests with a skip marker).
- **New `fs_scans/tests/conftest.py`** (none today): `sqlite_db(tmp_path)` fixture (run importer on a small fixture log, reusing fixtures from `test_fs_scans.py`/`test_histogram_import.py`); `pg_engine` fixture via **testcontainers-postgres**, else `pytest.skip("no postgres")`; mark PG tests `@pytest.mark.postgres`. Add `testcontainers` to a dev/test extra in `pyproject.toml`.
- **No-DB unit tests:** assert `query_builder.build(dialect="postgresql")` emits `~`/`ILIKE` vs sqlite `GLOB`/`LIKE`; assert pragmas no-op on non-sqlite bind; assert `_set_sqlite_pragma` attached only for sqlite engines.
- **Round-trip (PG-gated, highest value):** import fixture log → SQLite, `consolidate(..., swap=False)` into a testcontainer staging schema, run representative `query_directories` + a histogram query against PG, assert identical results to the SQLite source. Validates COPY, FK ordering, search_path, GLOB fix, and `has_table` fix together.
- **Swap test (PG-gated):** consolidate twice; assert `<c>_old` created then dropped; a connection opened before the swap still reads old data until reconnect (non-disruption).
- **End-to-end smoke:** against the live CNPG dev DB, consolidate one small collection (e.g. `asp`, 10 MiB), point `cs-usage`/`fs-scans query` at `FS_SCAN_DB_BACKEND=postgres`, confirm `--show-config` lists schemas and a query returns the same top-N as the `.db`.

## Open items to confirm with CNPG admin
- Consolidation can run as the existing `postgres` superuser in `.env` (already has `CREATE/DROP SCHEMA` + `CREATE DATABASE`). Still to create: a dedicated **read-only web role** (e.g. `app`/`pguser`, already stubbed in `.env`) granted only `USAGE`+`SELECT`, used by the webserver.
- **Shared-PVC headroom** (see Capacity section): measured PG footprint must leave comfortable margin above production `casper_jobs`/`derecho_jobs` on the 502 GB PVC. If not, request a dedicated database/PVC or volume expansion before cgd/weekly enablement.
- Connection budget: consolidation must use few connections (1–2); production sits at ~73 in use.
- Webserver uses short transactions / `pool_pre_ping` so the millisecond swap lock isn't held against a long transaction.
