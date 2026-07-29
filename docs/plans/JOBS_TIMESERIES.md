# `jobs_timeseries` + charges on every aggregate

> **Status:** implemented on branch `jobs_timeseries`. This is the plugin
> half of a two-repo change; the SAM consumer (a stacked activity timeline on
> the job-history card's **Jobs** tab) lands only after this merges and the
> container is rebuilt.

## Context

SAM's job-history card ships six tabs — Jobs · By User · By Project · Wait
Times · Job Sizes · Durations. Five carry a chart and **all five are
distributional or categorical**: nothing on the card has a time axis, on a
page whose entire framing is a date window.

SAM already renders that chart for *summaries* (the stacked-by-user Usage
Trend on `/user/resource-details`), but it is fed from SAM's
`comp_charge_summary` and so cannot answer anything about queues, job sizes,
wait times or exit status.

Nothing here could back a filter-aware version:

| existing method | groups by period? | honours the `jobs_search` filter set? |
|---|---|---|
| `usage_history` | yes | **no** — dates only, and CPU/GPU split by hardcoded queue lists |
| `jobs_by_entity_period` | yes | **no** — dates only |
| `unique_users_by_period` / `unique_projects_by_period` | yes | **no** — dates only |
| `daily_summary_report` | yes | **no** — dates only, and reads the pre-aggregated `daily_summary` table |
| `jobs_histogram` / `jobs_usage_by` | **no** | yes |

A chart built on any of the first four would silently ignore queue / size /
exit-status filters while sitting directly above a table that honours them.

## What this adds

### 1. `charges` on every aggregate

`jobs_histogram`, `jobs_usage_by` and the new `jobs_timeseries` now carry the
full metric vector `_METRIC_KEYS`:

```
job_count, cpu_hours, gpu_hours, cpu_charges, gpu_charges
```

`*_charges` is `SUM(hours * COALESCE(qos_factor, 1.0))` (`_charge_expr`) —
the same formula as `columns._compute_charge` (per-row) and `sync/summary.py`
(the daily rollup), so all three surfaces state it once. No new join: the
`job_charges` outer join was already there and `qos_factor` rides the row
already read for the hour sums.

`_USAGE_SORT_KEYS` gains `charges` / `cpu_charges` / `gpu_charges` so
`sort_by` / `owners_sort_by` can follow a charges metric pill — without that,
a top-N cut ranked by hours hides the biggest *charge* consumers (the
documented GPU-hours one-wedge bug, in a new coat).

**Charges are not proportional to hours.** `qos_factor` is a genuine `0.0`
for the `uncharged` QoS, so a slice can report hours > 0 and charges == 0. A
consumer must not render that as "no activity".

### 2. `JobQueries.jobs_timeseries(period='day', …)`

Per-period activity bands over the full `jobs_search` filter set. Returns a
zero-filled band vector in chronological order; each band carries the metric
vector and, with `owners_limit`, a top-N owner breakdown.

Two design points differ deliberately from `jobs_histogram`:

**Bands are site-local calendar periods built in Python, not `PeriodGrouper`.**
`PeriodGrouper` formats the raw column (`to_char(job.end, 'YYYY-MM-DD')` /
`strftime`) with **no timezone conversion**, so it buckets by *UTC* day —
while `_apply_date_filter` bounds the window by *site-local* days and
`sync/summary.py` bins `DailySummary` the same way. Mixing them offsets every
band from its own window, makes the first and last partial, and disagrees
with the daily summaries. Generating boundaries with `zoneinfo`
(`_site_midnight_utc` → `_period_bands` → `_period_case`) keeps one
convention, is DST-exact (a spring-forward day really is 23 h wide), compiles
identically on SQLite and PostgreSQL, and gets `week` for free — a
granularity `PeriodGrouper` does not support at all.

**Owners are ranked once over the whole window, not per band.**
`jobs_histogram`'s top-N is per-bucket, which is right for a distribution. A
stacked bar chart needs one legend and a colour that means the same owner in
every bar, so every band carries the *same* N keys in global rank order,
zero-filled where an owner was idle.

Band replay is `jobs_search(start=band['start'], end=band['end'], **filters)`
— the window filters *are* this dimension, so there is no
`min_param`/`max_param` to echo. Bands are clipped to the window so a
Monday-snapped week band cannot pull in jobs from before it.

## Measured cost

PostgreSQL 18 in Docker, `casper_jobs` (21.0 M jobs, 2024-01-01 … 2026-07-26),
windows anchored on `max(job.end)`. **MIN of N runs** — timing noise is
one-sided, and the spread on this local instance is wide (a baseline arm
ranged 1241–2127 ms across 10 runs), so the minimum is the only stable
estimator. Absolute numbers are pessimistic versus production; the **ratios**
are the point.

### Gate A — does adding charges regress the existing aggregates?

Interleaved A/B of the added expressions only: the same grouped aggregate
built once with 2 hour-SUMs and once with 2 hour-SUMs + 2 charge-SUMs,
alternated so cache state hits both arms equally. Casper, 30 d (555,225 jobs),
min of 10:

| shape | min |
|---|---|
| 2 SUMs (previous shape) | 1241.1 ms |
| 4 SUMs (with charges) | 1338.1 ms |
| **delta** | **+7.8 %** |

Consistent with "two more aggregate transitions per row" (~180 ns/row), not
with a changed query plan. Under the 10 % bar → shipped as designed.

### Gate B — what does the new aggregation cost?

Casper, 30 d, `owners_limit=10`, min of 10:

| query | min | vs one histogram |
|---|---|---|
| `jobs_histogram('cpus', owners_limit=10)` | 1288.9 ms | 1.00× |
| `jobs_timeseries('day', owners_limit=10)` — 30 bands | 2704.0 ms | **2.10×** |
| `jobs_timeseries('week', owners_limit=10)` — 5 bands | 2650.2 ms | **2.06×** |

Exactly the predicted two scans (rank + series). One statement when
`owners_limit` is None.

### Band-count sensitivity

Fixed 180-day window (4,278,829 jobs) at three granularities, so row count and
filters are identical and **ladder width is the only variable**. The three
periods are **interleaved** across 6 rounds — measuring them in sequence lets
buffer-cache warming masquerade as a band-count effect (a first, sequential
pass suggested only +27 %; interleaving shows the real figure):

| period | bands | min | vs month |
|---|---|---|---|
| month | 7 | 3221 ms | 1.00× |
| week | 26 | 3059 ms | 0.95× |
| day | 180 | 4971 ms | **1.54×** |

7 → 26 bands is flat (within noise); 7 → 180 costs **+54 %**. The `CASE`
ladder is ~O(bands/2) comparisons per scanned row, so band count is a real
cost knob — a bigger one than a sequential measurement implies. Hence
`_MAX_TIMESERIES_BANDS = 400` as a backstop and, on the SAM side,
auto-coarsening day → week → month to hold a chart near ≤120 bars. If this
ever needs to go wider, the fix is a dialect-specific `date_trunc` fast path
on PostgreSQL (O(1) per row) with the ladder retained for SQLite, not a
higher cap.

**Reproduce:** `scripts/bench_jobs_aggregates.py` (see below).

## Tests

`job_history/tests/test_jobs_search.py`, +50 tests:

- `TestFilterSignatureParity` gains `TIMESERIES_ONLY` — the filter set is
  enforced statically, as for the other four methods.
- `TestJobsTimeseries` — zero-filled chronological bands with interior zeros
  preserved; `total_count == jobs_count`; **every band replays into
  `jobs_count` at all three granularities**; totals are granularity-invariant
  (day/week/month agree, the strongest single check that the ladder tiles);
  derived window; empty slice; filter narrowing; owners legend identical in
  every band; owners zero-filled when idle; remainder derivable; `owners_by`,
  `owners_sort_by`; two-aggregate-scan guard; and the full `ValueError` set.
- **`test_site_local_day_binning_not_utc`** is the regression guard: a job
  ending 02:00 UTC (20:00 MDT the previous day) must bin into the earlier
  band. It fails under UTC bucketing.
- `TestPeriodBands` — DB-free structural guards: bands tile the window
  exactly at 6 spans × 3 periods, `hi_utc` strictly increasing, labels unique
  and lexicographically chronological, DST days 23 h/25 h, week clipping.
- `TestChargesAcrossAggregations` — pins `hours x qos_factor` in
  `jobs_usage_by`, `jobs_histogram` *and* `jobs_timeseries`, and asserts they
  agree. Uses the new `timeseries_jobs` fixture (factors 1.0 / 0.5 / 0.0)
  because `histogram_jobs` is uniformly 1.0, where charges and hours coincide
  and a swapped formula would pass unnoticed.

`job_history/tests/`: **673 passed**. The 38 `fs_scans` failures on this
branch are pre-existing — verified by stashing these changes and re-running.

## Docs touched

- `CLAUDE.md` key-files: `jobs_timeseries` in the shared-filter family, the
  `_METRIC_KEYS` note, and a warning on `PeriodGrouper`'s UTC formatting.
- `job_history/SCHEMA.md` § *Composite Indexes* — **corrected**. It claimed
  `(queue_id, end)`, `(queue_id, user_id, end)`, `(queue_id, account_id, end)`
  and a planner trace through `ix_jobs_queue_end`; none of those indexes
  exist. Replaced with the real inventory plus the known `(account_id, end)`
  gap.

## Not in scope

- `(account_id, end)` / `(queue_id, end)` composites — already deferred in
  `JOB_HIST_PLUGIN_ENHANCEMENTS.md`; an account-scoped series is the query
  that would benefit most.
- `quarter` / `year` periods — a chart that wide wants month bands, and
  keeping the vocabularies independent avoids implying `PeriodGrouper`
  compatibility.
- `memory_hours` / `memory_charges` in the metric vector — omitted until a
  consumer needs them, consistent with the existing note in `jobs_usage_by`.
