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

> **Corrected 2026-07-29.** This section previously reported **+54 % at 180
> bands** and concluded band count was a large cost knob. That figure was a
> measurement artifact: the three periods were timed **sequentially**, so
> buffer-cache warming rode along with band count — the exact failure this
> section warned about while committing it. Re-measured with the periods
> **interleaved inside one loop**, 180 bands costs ~10 %, not 54 %. The
> `date_trunc` follow-up it proposed was also measured, and is not a fast
> path. Corrected tables below.

Fixed window at three granularities, so row count and filters are identical
and **ladder width is the only variable**. Periods interleaved across rounds;
min of N.

| window | period | bands | min | vs month |
|---|---|---|---|---|
| 180 d (4,306,697 jobs) | month | 7 | 6735 ms | 1.00× |
| | week | 26 | 6848 ms | 1.02× |
| | day | 180 | 7390 ms | **1.10×** |
| 730 d (16,604,039 jobs) | month | 25 | 8909 ms | 1.00× |
| | week | 105 | 9505 ms | 1.07× |
| | day | 730 | 14699 ms | **1.65×** |

The `CASE` ladder really is ~O(bands/2) comparisons per scanned row — the
original instinct was right — but the cost does not become visible until the
ladder is several hundred arms wide. ~10 % at 180, ~65 % at 730. So
`_MAX_TIMESERIES_BANDS = 400` sits just below the knee, and SAM's
auto-coarsening to ≤120 bars is comfortably inside it (and is really a
legibility limit, not a cost one).

#### `date_trunc` is not the escape hatch

The previous text named a PostgreSQL `date_trunc` fast path (O(1) per row,
ladder retained for SQLite) as the fix if this ever needed to go wider. It was
built and A/B'd against the ladder, interleaved, with bit-identical labels:

| case | bands | ladder | `date_trunc` | ratio |
|---|---|---|---|---|
| 30 d / day, unscoped | 30 | 1558.8 ms | 1516.0 ms | 0.97× |
| 180 d / day, unscoped | 180 | 7948.5 ms | 7921.8 ms | 1.00× |
| 180 d / week, unscoped | 26 | 7131.7 ms | 7790.1 ms | 1.09× |
| 365 d / day, unscoped | 365 | 10383.6 ms | 9871.9 ms | 0.95× |
| 30 d / day, account-scoped | 30 | 127.6 ms | 135.4 ms | 1.06× |
| 180 d / day, account-scoped | 180 | 764.6 ms | 655.8 ms | 0.86× |

Parity within noise everywhere. The ladder is not what these queries spend
their time on — see below.

### Where the time actually goes

`EXPLAIN (ANALYZE, BUFFERS)` on the unscoped 180 d / day series: a full
`Parallel Seq Scan` on `jobs` (1.18 M blocks read from disk, 4.8 s)
hash-joined against a full `Parallel Seq Scan` of *all* of `job_charges`.
Isolating it, interleaved:

| shape | min |
|---|---|
| with the `job_charges` join + 4 SUMs | 7739.7 ms |
| `COUNT` only, no join | 4253.8 ms |
| **join's share of total** | **45 %** |

So the outer join to `job_charges` — not the ladder — is the dominant cost of
every wide aggregate in this module.

### The `daily_summary` fast path

`daily_summary` is keyed `(date, user_id, account_id, queue_id)`, bins
site-local by the same convention `_period_bands` computes with, and already
stores both hours and QoS-weighted charges. Where the filter set is expressible
in that key it answers the same question without touching `jobs`:

| query | scan `jobs` | `daily_summary` | speedup |
|---|---|---|---|
| 180 d / day, unscoped | 7390 ms | 15.6 ms | **474×** |
| 730 d / day, unscoped | 14699 ms | 28.0 ms | **525×** |

Fast-path figures are **end to end** — routing predicate plus query, i.e. what
a caller actually waits for. The aggregate alone is 9.1 ms / 21.3 ms; the
routing coverage check (below) is the ~7 ms difference.

Agreement, 180 d window: `job_count` 4,306,697 == 4,306,697 == `jobs_count()`;
`cpu_hours` 9,175,159.7 both ways; `cpu_charges` 7,352,227.1 both ways.
Verified across 11 filter shapes — counts, band labels, window echo and owner
keys match exactly; the float metrics to ~1e-13 relative, since the rollup
pre-sums each group and the series re-folds those subtotals while the scan
sums every job in one pass.

Implemented as `JobQueries._timeseries_uses_summary` (routing) plus
`_timeseries_from_summary` (execution), the same fast/slow split `fs_scans`
uses for its precomputed histograms. `qos`, `exit_status`, `job_id`, `name`
and every `min_*`/`max_*` bound force the scan path. Because the fast path has
no ladder at all, it gets the looser `_MAX_SUMMARY_BANDS = 1200` — three years
of daily bands — while the scan path keeps 400.

**The MIN/MAX probe must stay pure.** A caller that pins only `start` — which
is the timeline's shape, since "up to now" has no natural `end` — derives
`win_end` from `MAX(Job.end)`. The first cut folded the NULL-`end` count into
that probe as `COUNT(*) FILTER (WHERE end IS NULL)`, which reads tidier and is
a disaster: PostgreSQL only rewrites MIN/MAX into index InitPlans when the
select list is *purely* min/max aggregates, so the extra aggregate turned a
0.4 ms index probe into a full parallel seq scan.

| probe shape | min | plan |
|---|---|---|
| `min/max` alone | **0.4 ms** | InitPlan (index) |
| `min/max` + `count FILTER` | **4630 ms** | Parallel Seq Scan, 21.0M rows |
| `count WHERE end IS NULL` alone | **0.4 ms** | Index Scan on `ix_jobs_end` |

Split into two statements (both served by `ix_jobs_end` — PostgreSQL btrees
index NULLs and `IS NULL` is index-scannable), and the count skipped entirely
unless *both* bounds are missing, since any date bound already excludes NULLs
by construction. End to end on the timeline's own call shape:

| machine | window | before | after |
|---|---|---|---|
| derecho | 30 d / day | 414 ms | **3.8 ms** |
| derecho | 180 d / day | 2170 ms | **14.3 ms** |
| casper | 30 d / day | 456 ms | **4.8 ms** |
| casper | 180 d / day | 17179 ms | **14.1 ms** |

Invisible on SQLite, so `test_probe_keeps_min_max_alone_for_the_index_initplan`
asserts the probe's select list stays pure.

**Coverage, not a watermark.** The obvious freshness test is "is the window at
or before `max(daily_summary.date)`", and it is not enough: the rollup lags
`jobs` at the top *and* need not reach back to the beginning of history (a
partial `--resummarize` leaves an earlier gap), and a single skipped day
mid-window would come back as a zero band while the scan path finds jobs — a
silent under-count, the worst failure mode available here. Because
`generate_daily_summary` writes a NO_JOBS marker for a day with no jobs, every
*processed* day has at least one row, so `COUNT(DISTINCT date)` against the
window's width is an exact check. Measured ~20 ms on casper_jobs — one
aggregate on an indexed column of a table three orders of magnitude smaller
than `jobs`, against 7390 ms saved. Anything short of full coverage falls back
for the whole window; there is deliberately no hybrid, because two code paths
contributing to one band vector is where a double-count would live.

### Indexes: measured, and declined

`SCHEMA.md` flagged the absence of `(account_id, end)` as a gap an
account-scoped series would benefit from. Measured, it is a weak win: PostgreSQL
already resolves the scoped shape with `BitmapAnd(ix_jobs_account_id,
ix_jobs_end)`.

| query, 30 d window | min |
|---|---|
| account-scoped, no owners | 119.4 ms |
| user-scoped, no owners | 202.2 ms |
| account-scoped, `owners_limit=10` | 257.2 ms |

In the account-scoped plan the BitmapAnd costs ~23 ms of 105 ms, while the
nested loop into `job_charges` is 638 k of 684 k buffers. Not worth an index
on a 21 M-row table; the join is the target, and the fast path avoids it
entirely.

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

Added in the self-review round:

- `TestTimeseriesSummaryRouting` — which path answers and why: every
  serviceable filter routes to the rollup, every aggregated-away one falls
  back, a window past the watermark falls back, and the fast path issues no
  statement against `jobs` at all. `test_every_filter_is_classified` walks
  the method's own signature, so a **new** filter defaults to the safe path
  instead of being silently ignored by the summary.
- `TestTimeseriesPathEquivalence` — the decisive contract, 33 parametrized
  shapes (3 periods × 11 filter/owner combinations) plus a derived window.
  Counts, band labels, window echo and owner rank order compared **exactly**;
  float metrics approximately, for the summation-order reason above.
- `TestTimeseriesBandCaps` — the two caps, and that the summary path serves a
  window the scan path refuses.
- `TestTimeseriesSummaryCoverageLimit` — pins the one legitimate divergence
  (below) rather than leaving it latent.
- Re-bounding guards: the series statement carries a bounded `job.end`
  predicate; a slice whose rows are *all* NULL-`end` still reports
  `total_count == jobs_count()`; a partial-bound empty window echoes the
  bound the caller supplied.

`job_history/tests/`: **739 passed**. The `fs_scans` failures on this branch
are pre-existing — verified by stashing these changes and re-running.

## A rollup bug the fast path exposed

`sync/summary.py` **inner**-joined `job_charges`, so a job with no charge row
was silently dropped from `daily_summary` — and therefore from
`daily_summary_report`, a charging surface. `trg_ensure_job_charge` makes that
impossible in production (0 of 21.0M rows on casper_jobs), but a rollup should
not depend on a trigger for its arithmetic. Now a LEFT join with `COALESCE`ed
sums, so such a job counts with zero hours exactly as every outer-joining
query already reports it. The path-equivalence test is what caught it.

Rows whose `user_id` / `account_id` / `queue_id` is NULL are still excluded,
and that one is **not** incidental: a NULL FK triple is already the NO_JOBS
marker, so an unattributable job cannot be stored without becoming
indistinguishable from one. That is the fast path's one documented coverage
limit, pinned by `TestTimeseriesSummaryCoverageLimit` — also 0 of 21.0M rows.

## Docs touched

- `CLAUDE.md` key-files: `jobs_timeseries` in the shared-filter family, the
  `_METRIC_KEYS` note, a warning on `PeriodGrouper`'s UTC formatting, the
  fast path, and `sync/summary.py`'s join.
- `job_history/SCHEMA.md` § *Composite Indexes* — **corrected twice**. It
  claimed `(queue_id, end)`, `(queue_id, user_id, end)`,
  `(queue_id, account_id, end)` and a planner trace through
  `ix_jobs_queue_end`; none of those indexes exist. The replacement then
  called the missing `(account_id, end)` a gap worth closing, which
  measurement did not support — now records the numbers and declines it.

## Not in scope

- `(account_id, end)` / `(queue_id, end)` composites — measured and
  **declined**, see § *Indexes*. The join, not the index, is the cost.
- Denormalising `cpu_hours`/`gpu_hours`/`qos_factor` onto `jobs` to kill the
  45 % join. Real, but a schema change against the `job_charges` 1:1 trigger
  design, with a migration in `bin/update_jobs_db.py`. Its own PR.
- A `daily_summary` fast path for `jobs_histogram` / `jobs_usage_by`.
  `jobs_usage_by` is the natural next candidate — same serviceable filter
  subset — but one new path per PR.
- `quarter` / `year` periods — a chart that wide wants month bands, and
  keeping the vocabularies independent avoids implying `PeriodGrouper`
  compatibility.
- `memory_hours` / `memory_charges` in the metric vector — omitted until a
  consumer needs them, consistent with the existing note in `jobs_usage_by`.
  Note the rollup already stores them, so the fast path could serve them free.
