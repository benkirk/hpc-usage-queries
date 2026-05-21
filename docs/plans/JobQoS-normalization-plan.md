# Plan: Normalize QoS via a new `JobQoS` lookup table

## Context

Today, `Job.priority` (TEXT, parsed from PBS `Resource_List["job_priority"]`) and `JobCharge.qos_factor` (FLOAT, computed at sync time) are **functionally independent** — there is no FK, no constraint, no shared source of truth. The priority→factor mapping is hardcoded in `_get_qos_factor()` at `job_history/sync/charging.py:109-119`:

```python
if queue == "jhublogin": return 0.0
if priority == "premium": return 1.5
if priority == "economy": return 0.7
return 1.0
```

Test fixtures already drift (e.g. `test_jobs_search.py` uses `qos_factor=0.5` with no matching priority string). A normalized `JobQoS(id, name, factor, active)` lookup table — referenced by FK from `jobs.qos_id`, mirroring the existing `User`/`Account`/`Queue` pattern — gives one source of truth for the priority→factor mapping, makes the `jhublogin` special case a first-class row instead of a code override, and is non-destructive to back-port (`job_charges.qos_factor` stays as a materialized cache so `daily_summary` SQL is unchanged).

**Locked design decisions:**
1. FK on `Job` (`jobs.qos_id`). `JobCharge.qos_factor` remains as a denormalized cache populated from `JobQoS.factor` at sync time.
2. `jhublogin` becomes a real `job_qos` row (`name='jhublogin', factor=0.0`).
3. Schema: `id INTEGER PK, name TEXT UNIQUE NOT NULL, factor REAL NOT NULL, active BOOLEAN NOT NULL DEFAULT TRUE`. Seed rows: `premium=1.5`, `regular=1.0`, `economy=0.7`, `jhublogin=0.0`.

---

## 1. ORM model changes — `job_history/database/models.py`

Mirror the existing `Queue` lookup pattern exactly.

- **Import** `Boolean` from SQLAlchemy (line 6).
- **New `JobQoS` class** (after `Queue` ~line 44): `id`, `name` (unique), `factor`, `active`. Reuse `Queue` class as the template.
- **`LookupCache` (lines 46-110):** add `self._qos = {q.name: q for q in session.query(JobQoS).all()}` to `__init__`, plus a `get_or_create_qos(name)` method that mirrors `get_or_create_queue` but uses the savepoint INSERT pattern in `_get_or_create()` (line 86-110). The seed table provides canonical factors, so the cache only ever has to resolve names — `factor` is read from the row, never injected by the cache.
- **`LookupMixin` (lines 113-234):** add three pieces alongside the existing `queue` triplet:
  - `qos_id = Column(Integer, ForeignKey('job_qos.id'), index=True)` via `@declared_attr` (next to `queue_id` at line 159).
  - `qos_obj = relationship("JobQoS")` via `@declared_attr` (next to `queue_obj` at line 173).
  - Full `@hybrid_property qos` triplet (getter/setter/expression) modeled exactly on the `queue` triplet at lines 217-233. The setter stashes `_pending_qos_name`.
- **`Job.to_dict()` (lines 307-318):** add `result['qos'] = self.qos` for parity.
- **`before_flush` listener (lines 507-537):** inside the existing `for obj in list(session.new): if isinstance(obj, LookupMixin):` block, add a fourth branch resolving `_pending_qos_name` via `cache.get_or_create_qos()`. The existing `try/except` already covers fs_scans (missing-table fallback) — no change to the surrounding error handling.
- **`JobCharge`:** no changes. `qos_factor` stays as the materialized float.

## 2. Sync-time wiring

**`job_history/sync/charging.py:109-119` — replace `_get_qos_factor()`:**

Split into two helpers. `_resolve_qos_name(job)` maps `(priority, queue)` → canonical name (`jhublogin` short-circuit BEFORE priority check, preserving current precedence). `_get_qos_factor(job)` prefers `job.qos_obj.factor` when the FK is resolved, else falls back to mapping the raw strings against hard-coded canonical values — this fallback is needed for `types.SimpleNamespace` test fixtures that bypass the ORM.

The two callers at lines 176 (Derecho) and 208 (Casper) are unchanged.

**`job_history/sync/base.py` — `_insert_batch()` (~line 319-325):**

Where queue is resolved, add a sibling step that calls `SystemCharging._resolve_qos_name(rec)` and `cache.get_or_create_qos(name).id`, then writes `rec['qos_id']`. This honors the CLAUDE.md "bulk insert with FKs — use ORM not Core API" rule by resolving FK ids via the ORM cache **before** the Core-API insert, then passing `qos_id` as a plain integer column.

**`job_history/sync/base.py` — `UPDATABLE_JOB_FIELDS` (~line 30):** add `qos_id`. In `_update_batch()` (~line 695), re-resolve `qos_id` from updated `priority`+`queue` before writing.

`_fill_missing_charges`, `_recalculate_charges`: no change. They re-read `JobCharge.qos_factor` from `job.qos_obj.factor` if resolved, else the fallback mapping handles legacy rows missing `qos_id`.

The PBS parser at `sync/pbs.py:254` stays untouched — qos resolution happens at `_insert_batch` time because that's where both `priority` and `queue` are present in the merged record.

## 3. Database init + seed — `job_history/database/session.py`

- **`trg_ensure_job_charge` trigger (lines 304-352):** unchanged. Still inserts `qos_factor=1.0` placeholder.
- **New `_ensure_qos_seed_rows(engine)`** (after `_ensure_db_triggers`): per-dialect idempotent INSERT for the four canonical rows. PG uses `ON CONFLICT (name) DO NOTHING`; SQLite uses `INSERT OR IGNORE`.
- **`init_db()` (lines 355-386):** call `_ensure_qos_seed_rows(engine)` immediately after `_ensure_db_triggers(engine)` in both single-machine and all-machines branches.

## 4. Migration script — `bin/update_jobs_db.py`

Extend the existing idempotent script:

1. Add `Base.metadata.create_all(engine)` at the top of `migrate()` so the new `job_qos` table is created on existing DBs.
2. Append `("jobs", "qos_id", "INTEGER")` to `ADD_COLUMNS`.
3. Call `_ensure_qos_seed_rows(engine)` after the `ALTER TABLE` loop.
4. Call `_ensure_db_triggers(engine)` (defensive — keeps DBs aligned).
5. New `backfill_qos_id(engine)` step: SELECT the seed row ids, then issue an UPDATE on `jobs` mapping `(priority, queue_name)` → `qos_id` via a CASE expression. PG variant uses `UPDATE … FROM (subquery)`; SQLite variant uses inline correlated subquery to `queues`. The UPDATE is restricted to rows where `qos_id` differs from the expected value, so re-running matches zero rows.

This is **non-destructive on Postgres**: `job_charges.qos_factor` values are untouched, and the new `jobs.qos_id` is derived from existing data. Safe to re-run.

## 5. Tests — `job_history/tests/`

Existing fixtures keep passing because the `_get_qos_factor()` string-fallback covers `SimpleNamespace` jobs without `qos_obj`. The orphan `qos_factor=0.5` in `test_jobs_search.py:51-57` is preserved — it proves `JobCharge.qos_factor` and `JobQoS.factor` can diverge (the cache is authoritative for summary SQL).

New file `test_job_qos.py` covering:
- Seed: exactly 4 rows after `init_db()`, with expected factors.
- Hybrid roundtrip: `Job(qos="premium")` → flush → `job.qos == "premium"`, `job.qos_obj.factor == 1.5`.
- `before_flush` resolves `_pending_qos_name` without explicit cache calls.
- `LookupCache.get_or_create_qos` dedupes within and across sessions (savepoint pattern).
- `_resolve_qos_name` precedence: `queue=jhublogin, priority=regular` → `"jhublogin"`.
- Backfill correctness: seed fixture jobs with various priority/queue combos, run `backfill_qos_id`, assert mapping.
- `Job.to_dict()` includes `qos`.

Optional: add a `seeded_session` fixture to `conftest.py` that pre-populates the four JobQoS rows.

## 6. Docs

- **`job_history/SCHEMA.md`:** add `job_qos` to the normalization list (~line 17); new table section after `queues` (~line 117); add `qos_id` FK and `qos` hybrid to the `jobs` table section (~line 54); note on the QoS factors table (~line 137) that it's now seeded into `job_qos` and `JobCharge.qos_factor` is a materialized copy.
- **`CLAUDE.md`:** mention `qos_id` in the normalized FK list (~line 62).
- **`job_history/README.md`:** brief migration note pointing at `bin/update_jobs_db.py`.

## 7. CLI surfacing — deferred follow-up

Add a `qos` column to `job_history/cli/search/columns.py` (~line 36) showing `job.qos`, distinct from raw `priority` and numeric `qos_factor`. Skip in this PR per user direction.

---

## Files modified (anchor summary)

| File | Lines | Change |
|------|-------|--------|
| `job_history/database/models.py` | 6, ~44, 64-110, 159, 173, 233, 307-318, 524-536 | `JobQoS` class, LookupCache, LookupMixin hybrid, `to_dict`, before_flush |
| `job_history/database/session.py` | ~353, 375, 385 | `_ensure_qos_seed_rows`, wired into `init_db` |
| `job_history/sync/charging.py` | 109-119 | Replace `_get_qos_factor`; add `_resolve_qos_name` |
| `job_history/sync/base.py` | 30-37, 319-325, ~695 | `UPDATABLE_JOB_FIELDS`, `_insert_batch`, `_update_batch` |
| `bin/update_jobs_db.py` | 18-24, 55-65, new func | Table create, column add, seed, backfill |
| `job_history/SCHEMA.md` | 17, 54, 117, 137 | Doc updates |
| `CLAUDE.md` | 62 | Mention `qos_id` |
| `job_history/tests/test_job_qos.py` | new | Coverage for normalization |

---

## Verification

1. **Unit tests**: `pytest job_history/tests/ -x` — all 289 existing tests pass; new `test_job_qos.py` cases pass.
2. **Fresh DB**: `python -c "from job_history.database.session import init_db; init_db('casper')"` against an empty `data/casper.db`; `sqlite3 data/casper.db "SELECT * FROM job_qos"` shows exactly the 4 seed rows.
3. **Postgres backport on a copy**: `pg_dump casper_jobs | psql casper_jobs_copy`, then run `bin/update_jobs_db.py` against the copy. Assert: no errors; `SELECT COUNT(*) FROM jobs WHERE qos_id IS NULL` returns 0; spot-check 10 random jobs that `jobs.priority`+`queues.queue_name` map correctly to `job_qos.name`.
4. **Sync round-trip**: `jobhist sync --machine casper --upsert` over a recent day; confirm new jobs have `qos_id` populated and `job_charges.qos_factor == job_qos.factor` for the resolved row.
5. **Summary invariance**: run `jobhist sync --resummarize` and confirm `daily_summary.cpu_charges` totals are unchanged vs. a pre-migration snapshot — proves the materialized `qos_factor` cache stayed correct.
6. **Idempotency**: re-run `bin/update_jobs_db.py casper derecho`; output reports all changes as "already exists" and the backfill UPDATE affects zero rows.
7. **jhublogin sanity**: pick a jhublogin job: assert `job.qos == "uncharged"`, `job.qos_obj.factor == 0.0`, `JobCharge.qos_factor == 0.0` regardless of `job.priority`.

---

## Post-design amendments (pre-merge)

These tweaks were applied to the as-built code after the initial design above:

1. **`jhublogin` QoS row → general `uncharged` QoS.** Renamed the
   canonical row from `jhublogin` to `uncharged` (factor still `0.0`),
   and introduced `_QUEUE_TO_QOS_NAME = {"jhublogin": "uncharged"}` in
   `sync/charging.py`. The queue named `jhublogin` continues to bypass
   priority-based charging via the same precedence path, but routing
   another non-chargeable queue is now a one-line dict edit instead of
   a code branch. `_rename_jhublogin_to_uncharged()` (in
   `database/session.py`) is wired into `init_db()` and
   `bin/update_jobs_db.py` so existing dev/prod DBs with a `jhublogin`
   QoS row get renamed in place — `jobs.qos_id` FK references are
   preserved.
2. **New `special` QoS** (factor `1.0`, active). Recognized as a
   priority string (`_PRIORITY_TO_QOS_NAME`) and as a backfill
   target. Seed list in `JOB_QOS_SEED` is now five rows.
