# Per-machine histogram bins

> **Status: IMPLEMENTED** 2026-07-29, on branch `per_machine_histogram`.
> Scoped as a follow-up to the `jobs_timeseries` work (PR #102).
>
> **What shipped differs from § *Plan* below in one important way:** the
> per-machine tables are **derived by truncation** from the shared tables
> (`QueryConfig.MACHINE_HIST_CAPS` + `_truncate_bucket_table`), not
> hand-written `CASPER_*_HIST_BUCKETS` constants. Closure and contiguity then
> hold *by construction*, a cap only has to be right to within a band, and
> adding a machine costs three integers. See § *What shipped* at the end;
> § *Plan* is kept as the original reasoning.
>
> Memory was **dropped from scope on evidence**: Casper's largemem nodes put
> 9,666 jobs above 1000 GB, so both machines populate every `REQMEM` band and
> there is no dead axis there. Only `nodes`/`cpus`/`gpus` are capped.

## Context

`jobs_histogram`'s bucket tables are plugin-owned constants
(`QueryConfig.*_BUCKETS`, `job_history/queries/jobs.py:499-613`) sized for the
**largest machine the plugin serves**: `CPU_HIST_BUCKETS` runs past 32768,
`NODE_HIST_BUCKETS` past 2048, `GPU_HIST_BUCKETS` past 256.

Casper has a few hundred nodes. So on Casper the top of every
node/CPU/GPU/memory axis is bands nothing can ever land in — roughly a third
of the x-axis spent on structural zeros. SAM compensates at render time with
`_trim_empty_edge_bands` (`project_samuel/devel/src/webapp/jobs/routes.py:978`),
which drops leading and trailing all-zero bands before the chart and the drill
list are built.

The original question was: **should the consumer supply the bins?** The
research answer is no — for this driver the consumer has *less* information
than the plugin, and the fix needs no API change at all.

## Why not consumer-supplied bins

SAM cannot compute per-machine bins today, and giving it the ability is the
expensive part:

- **`Machine`** (`project_samuel/devel/src/sam/resources/machines.py:17`) has
  exactly one hardware column — `cpus_per_node`, nullable, hand-entered
  through the admin UI. **No `max_nodes`, no GPU capacity, anywhere in SAM.**
  The only `num_nodes` in the schema is `num_nodes_used` on *activity* rows —
  consumption, not capacity.
- **`webapp/jobs/` never imports `Machine`.** There, `machine` is a bare
  string (`_VALID_MACHINES`, `routes.py:92`); no `machine → Machine` resolver
  exists. Adding one puts a SAM DB round-trip on a path that today touches
  only the plugin.
- **The mapping isn't 1:1.** `_count_via_sam_summary`'s docstring
  (`webapp/jobs/service.py:303`) records that plugin `machine='derecho'` spans
  SAM's `Derecho` *and* `Derecho GPU` resource rows — so "the Machine row for
  derecho" is ambiguous, and that's the GPU capacity you'd most want.

Meanwhile **the plugin already receives the machine**
(`JobQueries(session, machine=machine)`) and already keeps per-machine
knowledge beside the bucket tables (`QueryConfig.MACHINE_QUEUES`,
`jobs.py:405`; `VALID_MACHINES`, `database/session.py:23`).

⇒ Right-sizing belongs where the machine identity and the tables already live.

## Plan

### 1. Machine-keyed tables — `job_history/queries/jobs.py`

Add overrides beside the existing tables (`jobs.py:499-613`) and select on
`self.machine` at the spec unpack in `jobs_histogram` (`jobs.py:2216`):

```python
#: dimension -> {machine: table}. A machine absent here keeps the default
#: table, so adding a machine is additive and never silently re-bins an
#: existing one.
_MACHINE_HISTOGRAM_BUCKETS = {
    'nodes': {'casper': CASPER_NODE_HIST_BUCKETS},
    'cpus':  {'casper': CASPER_CPU_HIST_BUCKETS},
    'gpus':  {'casper': CASPER_GPU_HIST_BUCKETS},
}
```

The edit inside `jobs_histogram` is **one line** after
`column, buckets, unit, min_param, max_param = spec` (`:2216`). The local
`buckets` is read in exactly two places — `_bucket_case` (`:2218`) and the
zero-fill/emit loop (`:2282`) — so nothing else in the method is touched.

Sizing input: Casper's real maxima. Take them from the data rather than
guessing —
`SELECT max(numnodes), max(numcpus), max(numgpus) FROM jobs` on
`casper_jobs` — then round up to the next natural band edge so the top band
stays open-ended and stable as the machine grows.

⚠️ **Do not mutate or replace the class attributes.** `DURATION_HIST_BUCKETS`
is not histogram-only: `QueryConfig.get_duration_buckets()` (`jobs.py:616`)
derives the legacy `job_durations` report's SQL conditions from it, and that
ships as two `jobhist resource` commands plus `plots/plot_usage_history.py`.
Per-call *selection* is safe; in-place replacement is not.

### 2. Promote the closure invariant to a runtime validator

`test_bucket_tables_are_closed_and_contiguous`
(`job_history/tests/test_jobs_search.py:1565`) already encodes exactly what a
table must satisfy:

- first band reaches the domain floor (`lo == 0`, or `None` for a signed
  dimension like `memory_wasted`)
- last band is open-ended (`hi is None`)
- no mid-table `hi is None` (the ladder would skip it)
- strict contiguity: `lo_next == hi + 1`

Extract it to `_validate_bucket_table(buckets)`, have the existing test call
it per dimension (keeping the pin), and run it over the new machine tables.

**This is the load-bearing step.** The failure mode is *silent*: an un-closed
table makes `_bucket_case`'s `<= hi` ladder claim rows whose advertised
`lo`/`hi` exclude them, so a bar counts rows the replayed `jobs_search`
filters don't return — while `total_count` stays correct. The
`floor_band_jobs` fixture (`:1438`) exists because that regression already
happened in production (404 derecho / 21 casper rows with `numcpus=0`).

Checks the current test lacks and a validator should add: non-empty (an empty
table gives `IndexError` at `_bucket_case`'s `buckets[-1][0]`, not a clean
error) and unique labels (duplicates make the fold merge bands and quietly
break `total_count == jobs_count(**filters)`).

### 3. Tests

- Parameterize `_assert_bands_round_trip` (`:1475`) over machines, so every
  band of every per-machine table replays to exactly its own `job_count`.
- Assert an **unlisted machine is byte-identical** to today — mirror
  `test_owners_by_defaults_to_user` (`:1792`), which uses exact equality for
  the same "same code path both times" reason.
- Validator accept/reject cases.
- Not affected: `TestFilterSignatureParity` — no signature change, so
  `HIST_ONLY` (`:1179`) is untouched.

### 4. SAM: documentation only, no code

Two docstrings assert the "sized for the largest machine" framing as
present-tense fact and go stale:

- `_trim_empty_edge_bands` (`project_samuel/devel/src/webapp/jobs/routes.py:993`)
- `test_trim_drops_trailing_empty_bands`
  (`project_samuel/devel/tests/unit/test_webapp_jobs.py:3977`)

## What this does NOT fix

**`_trim_empty_edge_bands` stays.** Three of its four jobs survive
per-machine bins — budget accordingly:

| behaviour | survives? | why |
|---|---|---|
| Leading trim | **yes** | Every job uses ≥1 node/CPU, so those `0` bands can never fill. The band exists because `_bucket_case` requires the table to reach the column's domain floor — a property of the ladder, not of machine size. (GPUs are the exception that keeps it honest: their `0` band holds CPU-only jobs and survives on merit.) |
| Trailing trim | **partly** | Per-machine tables remove the machine-sizing half. A project-scoped or queue-filtered pane still can't fill its machine's top bands — a *filter* artifact no static table can anticipate. |
| All-zero → `buckets: []` | **yes** | Load-bearing UI logic: `routes.py:1250` computes `has_bands`, and `jobs_histogram.html:54` branches on it to choose between "No jobs match these filters" and "All N jobs have no wait measurement". |
| Index alignment | **yes** | `#jh-bar-<i>`, `data-jh-bucket` and `bucket_drills` are three consumers of one index space. |

## Free either way — the self-describing envelope

No change needed in any of these, whatever the bins are. This is the payoff of
the envelope carrying `lo`/`hi`/`min_param`/`max_param`:

- `_bucket_drill_url` (`webapp/jobs/routes.py:952`) replays the envelope's own
  bounds.
- The `#jh-bar-<i>` ↔ `data-jh-bucket` contract
  (`webapp/static/js/svg-chart-links.js:78`) is **index**-keyed, never
  label-keyed — deliberately, per the comment there.
- `_jobs_histogram_cache_key` (`webapp/dashboards/charts.py:1318`) is
  content-addressed on rendered output, so differing tables key apart on their
  own.
- Cache fan-out: `machine` is already part of the aggregation cache key
  (`webapp/jobs/cache.py:122`), so a machine only ever sees one table —
  **zero fan-out cost**.

## If the `buckets=` kwarg is ever revisited

Not recommended now (no consumer knows better than the plugin), but the
scoping is done, so it isn't lost:

Plugin cost is genuinely small — `buckets=None` on the signature, one override
line, the same `_validate_bucket_table`, plus `"buckets"` added to `HIST_ONLY`
(`test_jobs_search.py:1179`), which otherwise fails immediately. ~5 lines
production, ~25 validator, ~3 tests. **Zero CLI blast radius**: `jobhist`
exposes no histogram command (README § "Dashboard aggregations (API-only)").

Three design decisions hide in it:

1. **The floor rule.** `first[1] in (0, None)` is right for today's tables but
   too permissive at runtime — it would let a caller declare an
   unbounded-below floor on `cpus`. Doing it properly needs a signed/floor
   flag, i.e. a **6th element on the `_HISTOGRAM_SPECS` tuple**, which breaks
   the positional unpacks at `jobs.py:2216` and `test_jobs_search.py:1572`.
2. **Duplicate labels** silently break `total_count == jobs_count(**filters)`.
3. **No band-count cap.** `jobs_timeseries` has `_MAX_TIMESERIES_BANDS = 400`
   because the CASE ladder is a per-scanned-row cost (measured: 7 → 180 bands
   = +54% on a fixed 4.28M-row window). `jobs_histogram` has no equivalent, so
   caller bins would be an uncapped knob.

⚠️ **SAM-side trap, verified by running it.**
`sam/caching/buckets.py:278` normalizes collections in cache keys with
`tuple(sorted(str(v) for v in value))` — **order-insensitive on purpose**,
because "every collection in these keys is a set of filters, where order
carries no meaning". A bin table is the opposite: an ordered vector whose
order determines bucket indices. Two tables with identical edges in different
order produce the **same cache key** and different envelopes. Dicts are worse:
`norm` falls to `str(dict)`, insertion-ordered and not key-sorted (unlike
`_content_hash`, `webapp/caching/chart.py:21`, which does sort).

Do **not** loosen `norm` — it is shared with `disk_scans` and
`sam.queries.usage_cache`, where order-insensitivity is correct. Pass a short
**`bins_id`** (name or content hash) instead of the table.

Also note: a *user-selectable* bins control multiplies the documented ~22
aggregation keys per filter combination (`webapp/jobs/cache.py:40`) and
invalidates `test_jobs_cache_sizes_fit_the_explorer_fan_out`
(`tests/unit/test_webapp_jobs_cache.py:182`). Per-machine bins do not.

## Is this worth it?

`_trim_empty_edge_bands` already hides most of the symptom from users. The
gain is a truer axis — and one less layer of "the data is fine, the display is
compensating" — not a visible bug fix. It does **not** let you delete the
trimming helper.

Do it if you want the axis honest at the source; defer it freely otherwise.
The validator extraction (§2) has standalone value and could ship alone.

## What shipped

All in `job_history/queries/jobs.py` unless noted. Note every line reference
in § *Plan* above had drifted ~60 lines by the time this was executed; the
claims held, the numbers did not.

1. **`_validate_bucket_table(buckets, name)`**, beside `_bucket_case`. The six
   rules — non-empty, floor reached, top open-ended, no mid-table `hi is
   None`, strict contiguity, unique labels. Run **at import** over every
   `_HISTOGRAM_SPECS` table and every derived one: the tables are constants,
   so it either always passes or always fails, and a query never pays for it.
   `test_bucket_tables_are_closed_and_contiguous` now calls it, keeping the
   pin explicit.

2. **`QueryConfig._truncate_bucket_table(buckets, cap)`** — keeps every band
   with `lo <= cap`, then re-opens the last survivor as `(f">{lo-1}", lo,
   None)`, matching the existing overflow convention. Bands below are
   untouched, so a label means the same thing on every machine. `cap` at or
   above the last band's `lo` is an identity; a `cap` landing in the floor
   band raises.

3. **`QueryConfig.MACHINE_HIST_CAPS = {'casper': {'nodes': 128, 'cpus': 1024,
   'gpus': 32}}`**, beside `MACHINE_QUEUES`. Sized from production: over
   21.1M casper jobs the observed maxima are 100 / 864 / 32, and no casper job
   has ever exceeded a cap.

4. **`_MACHINE_HISTOGRAM_BUCKETS`** — `{machine: {dimension: table}}`, derived
   and validated once at import.

5. **The selection**, at the `jobs_histogram` spec unpack:
   `buckets = _MACHINE_HISTOGRAM_BUCKETS.get(self.machine, {}).get(dimension,
   buckets)`. The double `.get` mirrors `get_cpu_queues`, so an unlisted
   machine, an uncapped dimension, and the CLI's `machine="all"` all fall
   through unchanged.

6. **`scripts/bench_jobs_aggregates.py`** — its gate A read
   `CPU_HIST_BUCKETS` directly while gate B called `jobs_histogram`, so under
   `--machine casper` the two gates would have timed different ladders. Routed
   through the same selector. (§ *Plan* missed this third reader.)

Resulting Casper axes, and the live counts that justify them:

| dim | bands | table | top band holds |
|---|---|---|---|
| `nodes` | 14 → **9** | `0 1 2 3-4 5-8 9-16 17-32 33-64 >64` | 5 jobs |
| `cpus` | 14 → **11** | `… 65-128 129-512 >512` | 28 jobs |
| `gpus` | 11 → **7** | `0 1 2 3-4 5-8 9-16 >16` | 52 jobs |

No band is traded for another dead one. Verified against live `casper_jobs`:
`total_count` exact and **every** band round-trips to its own `job_count`,
both over a 1.2M-job month and over full history.

37 new tests (`TestBucketTableInvariants`, plus per-machine cases in
`TestJobsHistogram`); `_assert_bands_round_trip` is parameterized over
machines, and `test_every_bucket_table_compiles` now covers the derived
tables (and the `MEMUSED_`/`MEMWASTED_` ones it had been missing). Suite
748 → **785** job_history, **1004** total.

## Verification

```bash
cd ~/codes/hpc-usage-queries/devel
PYTHONPATH=$PWD python -m pytest job_history/tests/ -q     # 785

# size the tables from the data
PYTHONPATH=$PWD python -c "
from sqlalchemy import func
from job_history import get_session, Job
s = get_session('casper')
print(s.query(func.max(Job.numnodes), func.max(Job.numcpus),
              func.max(Job.numgpus)).one())
"

# an unlisted machine must be unchanged
PYTHONPATH=$PWD python -c "
from job_history import JobQueries, get_session
q = JobQueries(get_session('derecho'), machine='derecho')
print(len(q.jobs_histogram('cpus')['buckets']))   # expect 14
"

# casper should top out near its real size
PYTHONPATH=$PWD python -c "
from job_history import JobQueries, get_session
q = JobQueries(get_session('casper'), machine='casper')
print([b['label'] for b in q.jobs_histogram('cpus')['buckets']])
"
```

> `PYTHONPATH=$PWD` is required: SAM's conda-env carries a pip-installed
> `job_history` snapshot that shadows this working tree when a script is run
> by absolute path.

Then in SAM (`docker compose up webdev --watch`, :5050): Job Sizes on Casper
vs Derecho — Casper's CPU/node axes should lose their structurally-empty top
bands *before* trimming, and a bar click must still open the matching row.
The index contract is bin-agnostic, but it is the thing most worth eyeballing.
