# Runbook: completing the `eligible_secs` migration on prod

**Scope:** PR #97 (`jobs_plugin_enhancements` → `staging`). This is a transient
runbook for finishing a one-time migration. **Delete it when the PR closes.**

**Snapshot taken:** 2026-07-26. Everything in [State](#state-as-of-2026-07-26) is
point-in-time — re-run the queries rather than trusting the numbers below.

---

## What the PR changes

Wait time was measured as `start - eligible` (PBS `etime`), which is in practice
`start - submit` — `qtime == etime` on 77,152 of 77,154 sampled derecho `E`
records. That counts held, dependency-blocked, and `qsub -a`-deferred time as if
the site had made the user wait for resources.

PBS already computes the right number in `eligible_time`, which accrues *only*
while a job is blocked on resource scarcity. We were parsing it and discarding it.

Three new `jobs` columns, all populated from the PBS `E` record:

| column | PBS field | notes |
|---|---|---|
| `eligible_secs` | `eligible_time` | seconds; the wait metric. NULL ≠ 0 — see [the gap](#the-derecho-history-gap-not-a-bug) |
| `queued` | `qtime` | entered *current* queue; reset by routing |
| `run_count` | `run_count` | `> 1` means requeued |

`JobQueries.job_waits_by_resource()` now aggregates `eligible_secs` and filters
`IS NOT NULL`. Full semantics live in `job_history/SCHEMA.md` §*Wait time: use
`eligible_secs`* — that is the durable documentation; this file is not.

---

## State as of 2026-07-26

| target | schema | backfill |
|---|---|---|
| local dev PG (containers) | done, both machines | **complete**, both — acceptance passed |
| **prod CNPG derecho** (15.73M rows) | **done** | **in progress**, ~45% |
| **prod CNPG casper** (26.65M rows) | not started | not started |

Prod backfill was running from a laptop over the WAN at ~3,600–3,970 rows/s
(local was 10,441/s). Not the bottleneck it might have been; see
[performance](#performance-notes).

Not yet done anywhere on prod: `VACUUM (ANALYZE) jobs`, and deploying this branch
so that *newly synced* jobs populate the columns going forward.

---

## Quickstart

Assumes `.env` has `JOB_HISTORY_DB_BACKEND=postgres` and the `JOB_HISTORY_PG_*`
vars pointed at CIRRUS/CNPG (they indirect through `${CIRRUS_PG_*}`). Never put
credentials on a command line — they are visible in `ps`.

### 0. Confirm you are pointed where you think you are

```bash
python - <<'PY'
from sqlalchemy import text
from job_history.database.session import get_engine, JobHistoryConfig
print("backend:", JobHistoryConfig.DB_BACKEND)
for m in ("derecho", "casper"):
    with get_engine(m).connect() as c:
        print(m,
              c.execute(text("SELECT current_database()")).scalar(),
              "loopback=", c.execute(text(
                  "SELECT inet_server_addr() IS NULL "
                  "OR host(inet_server_addr()) = '127.0.0.1'")).scalar(),
              "rows=", f"{c.execute(text('SELECT COUNT(*) FROM jobs')).scalar():,}")
PY
```

`loopback=False` and the larger row counts mean prod. `jobs` must be owned by the
connecting role for the `ALTER TABLE` to succeed — it was `postgres` on both.

### 1. Migrate schema + backfill

```bash
python bin/update_jobs_db.py derecho
python bin/update_jobs_db.py casper
```

One command does both: `ALTER TABLE ADD COLUMN` (metadata-only in PG 11+,
instant) then the chunked backfill.

**It is safe to Ctrl-C at any point.** Chunks commit individually and the resume
guard reprocesses only rows where all three columns are NULL. Re-running after an
interrupt picks up where it stopped; re-running after completion updates 0 rows.

It reads the archived `job_records` pickles, so it needs **no accounting-log
access** and works against CNPG directly. Expect it to spend a couple of minutes
in the pre-existing `qos_id` backfill (158 chunks, 0 updates) before reaching
ours.

### 2. Deploy this branch where sync runs

Until the branch is deployed, newly synced jobs land with the columns NULL. After
deploying, `jobhist sync` populates them going forward. **Order matters:** the
schema migration (step 1) must precede any sync from this branch.

To repopulate a specific historical range without the blob path,
`jobhist sync --upsert` also works — `UPDATABLE_JOB_FIELDS` already covers all
three columns — provided the host has the accounting-log archive.

### 3. Vacuum

The backfill rewrites every row, leaving an equal count of dead tuples and stale
planner statistics on a heavily indexed table. Needs autocommit, so it cannot run
inside the migration script's transaction:

```bash
python -c "
from job_history.database.session import get_engine
for m in ('derecho','casper'):
    with get_engine(m).execution_options(isolation_level='AUTOCOMMIT').connect() as c:
        c.exec_driver_sql('VACUUM (ANALYZE) jobs')
        print(m, 'done')
"
```

Locally this took 13.7 s and 22.1 s. It is fast; do not skip it.

### 4. Validate

```sql
SELECT to_char(date_trunc('month', "end"), 'YYYY-MM') AS mon,
       COUNT(*)              AS jobs,
       COUNT(queued)         AS queued,
       COUNT(run_count)      AS run_count,
       COUNT(eligible_secs)  AS eligible_secs
FROM jobs GROUP BY 1 ORDER BY 1;
```

Pass criteria:

- **`queued` and `run_count` must be 100% of `jobs` in every month, both
  machines.** These appear in every accounting record back to the beginning.
  Anything less is a real bug — stop and investigate.
- **`eligible_secs`: casper 100% throughout; derecho 0% before 2025-01, a partial
  month at 2025-01, then 100%.** A *clean step* is correct. Ragged partial
  coverage spread across many months would indicate a parse bug.

Then confirm the reports run and the metric actually moved:

```bash
jobhist --format rich resource -m derecho \
        --start-date 2026-06-01 --end-date 2026-06-30 job-waits
```

On local dev, June 2026 moved from 4.0619 h to 1.9621 h average on derecho
(−51.7%) and 0.2487 h → 0.2355 h on casper (−5.3%). Casper barely moving is the
sanity check, not a problem: it is a short-wait HTC system with little dependency
chaining, so there is little distortion to remove. If casper swings as hard as
derecho, suspect the new column rather than the old metric.

### 5. Close the PR

Once both machines pass step 4 and the reports look right. Delete this file as
part of that.

---

## The derecho history gap (not a bug)

`eligible_secs` is **permanently NULL for derecho jobs before 2025-01-08.**

PBS only accrues and emits `eligible_time` when the `eligible_time_enable` server
attribute is True; it defaults to False. Someone enabled it on derecho's PBS
server around 2025-01-07. Per the PBS Admin Guide §4.9.13.6:

> When `eligible_time_enable` is set to False, PBS does not track `eligible_time`.
> … Accounting logs do not show `eligible_time` for any job submitted before or
> after turning `eligible_time_enable` off.

So the field is absent from the **accounting logs themselves**. Neither a
re-import, nor `sync --upsert`, nor a different backfill path can recover it.
This has been verified three independent ways: field enumeration over 20,000
January-2024 derecho `E` records (absent), bisecting archived `JobRecord` pickles
(2025-01-07 → 0/40, 2025-01-08 → 40/40), and the calendar signature in the
acceptance query (2025-01 came in at 72.2% locally, against 74.2% expected for 23
of 31 days).

Casper is unaffected — it has full coverage across all history held.

**Expect prod derecho's coverage to be lower than local dev's (~58%).** Prod
carries history back to 2023-03-29 versus local's 2024-01-01, and all of that
extra history is pre-cutover. Projected ~48–49%. Lower coverage here means *more
pre-cutover history*, not less data recovered.

Decided and settled, not to be relitigated: the reports **exclude** those jobs
rather than falling back to `start - eligible`, because a silent fallback would
reintroduce mid-report exactly the misleading measurement this change exists to
remove.

---

## Performance notes

- **Local rates do not predict prod.** Local dev (containers, loopback) ran at
  10,441 rows/s; prod CNPG over the WAN ran at ~3,600–3,970 rows/s.
- The backfill must pull every `job_records` blob to the client: ~17.7 GB for
  derecho, ~21.4 GB for casper. Effective wire rate was only ~4.5 MB/s, which
  suggests server-side TOAST detoasting rather than bandwidth is the limit — so
  running in-cluster would likely help less than the raw GB figure implies. At
  ~3,600 rows/s the full prod job is roughly 3 hours for both machines.
- **`execute_batch` is load-bearing, not an optimization.** SQLAlchemy's psycopg2
  dialect batches only INSERT, so the UPDATE path would issue one round-trip per
  row — 15.7M on derecho alone, which over a remote link is infeasible rather
  than merely slow. See `_executemany_update()` in `bin/update_jobs_db.py`.
- Progress is line-buffered, so `| tee` and redirects show per-chunk lines live.

## Other gotchas

- **Claude Code's auto-mode classifier blocks `bin/update_jobs_db.py`** and
  ad-hoc DDL. Read-only queries pass fine. Run the migration yourself with the
  `!` prefix, or take Claude out of auto mode.
- `_TimeDiffHours` in `job_history/queries/builders.py` is now dead code. Left in
  place deliberately pending a check of SAM's `src/cli/` for a counterpart that
  should stay in step — `CLAUDE.md` notes the layers mirror each other.
- The JSON envelope shape is unchanged, so no SAM coordination is required. SAM
  pins this package at `HPC_USAGE_QUERIES_REF` (default `main`), so it picks the
  change up on its next rebuild after merge — which is why prod's schema must be
  migrated before the PR lands.
- Array-*parent* rows (`…[].desched1`, ~1% of rows) carry an `eligible_time`
  accrued across the whole array's lifetime, so it can exceed that row's own
  `start - submit` by hours. Ordinary jobs and subjobs are well behaved
  (median +2 s). Documented in `SCHEMA.md`; no action needed.
