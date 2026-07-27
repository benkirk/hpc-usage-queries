# Job-history plugin: search drill-down surface for a SAM dashboard

## Context

PR #97 (merged to `staging` as `b044414`) added `queued`, `eligible_secs`, and
`run_count` to the `jobs` table and backfilled both production machines. Those
columns are *displayable* today — they're in `COLUMNS`, so `--display` and
`--verbose` reach them, and `sort_by` already resolves them — but nothing can
**filter** on them. Meanwhile a user has asked for job-name glob search in the
SAM dashboard.

SAM is about to grow a drillable job-history dashboard. This branch does the
plugin-side work only; the SAM side is explicitly out of scope.

**What SAM actually consumes** (verified against `project_samuel/devel`): the
Python API only — `get_engine`, `get_session`, `JobQueries.jobs_search`,
`jobs_count`, `list_qos_names`, `daily_summary_report`, plus a direct import of
`job_history.cli.search.columns.COLUMNS` at `webapp/jobs/routes.py:312`. It never
shells out to `jobhist` and never parses the `kind=` JSON envelope. So new
capability must land as `JobQueries` methods; CLI flags are for humans.

Intended outcome: `jobs_search`/`jobs_count` gain a job-name glob and
wait/resource range filters, plus a new `jobs_facets()` for live filter-dropdown
counts — all sharing one filter helper so a paginated UI's total can never
disagree with its rows.

**The plugin and SAM will be deployed together**, and SAM can pin any plugin SHA
via `HPC_USAGE_QUERIES_REF` for local dev. So this PR takes three deliberately
breaking cleanups (§7) instead of adding compatibility shims for warts we
control both ends of.

## Measured cost (local dev PG 18, derecho 13.0M rows; one month = 304k rows)

Everything below is warm, machine-wide, on the localhost dev container.

| operation | cost |
|---|---|
| `jobs_search(limit=50)` | walks `ix_jobs_end`, stops at 50 — does **not** scan |
| `jobs_count()` | index-only scan of the slice |
| name glob added to a scan | ~free (107 ms vs ~100 ms without) |
| facets, 1 dimension | 154 ms |
| facets, 3 dimensions **composite** | **154 ms** |
| facets, 5 dimensions composite | 201 ms |
| facets, 5 dimensions as 5 separate queries | 714 ms |
| facets, 5 dimensions via `GROUPING SETS` | 182 ms |
| `GROUP BY queue_id` vs `GROUP BY <hybrid property>` | **122 ms vs 1,223 ms (10×)** |

Two conclusions that drive the design:

1. **One composite `GROUP BY` over the integer FKs gives N facets for the price
   of ~1 scan**, on both backends. `GROUPING SETS` buys nothing over it and
   would add a dialect split — skip it.
2. **Never group on `Job.user`/`account`/`queue`/`qos`.** Those `@hybrid_property`
   expressions compile to a correlated scalar subquery emitted *twice* (select
   list and `GROUP BY`), re-evaluated per scanned row. 10× on PostgreSQL.

The only cost lever that matters by an order of magnitude is the **date window**:
unbounded facets over full history were measured at ~200 s. Document it, don't
enforce it (see §3).

## Data facts established while measuring

- `numgpus`, `numnodes`, `numcpus`, `name` are **NULL-free** on all 34M rows
  across both machines. NULL-strictness edges below are real in SQL but
  theoretical in this data — document and test them, don't contort the API.
- `Job.status` is the PBS **`Exit_status`** (`sync/pbs.py:250`), not a job state:
  192 distinct values on derecho, 135 on casper, **zero non-numeric**. Top values
  `'0'`, `'1'`, `'-29'`, `'271'`, `'255'`, `'143'`. Four docstrings claim
  *"e.g. 'F' for finished"* — a value that matches nothing, ever.
- `eligible_secs` is NULL for every derecho job ending before
  2025-01-07 17:47:50 UTC (`eligible_time_enable` was off). Casper is complete.

## 1. Glob helper — `job_history/queries/builders.py`

Append a new section. Deliberately **not** a `@compiles` `FunctionElement` like
the `_PeriodFunc`/`_QuarterFunc` classes above it: those rewrite the *operator*
per dialect, but a glob must rewrite the *bound value* (glob → POSIX regex on
PG). A bindparam's value isn't part of the compiled-SQL cache key, so a
compiler-side value rewrite would let two different globs share one cache entry.
Translating in Python at query-build time keeps each pattern its own correctly
cached bindparam.

- `glob_to_posix_regex(pattern)` — `*`→`.*`, `?`→`.`, escape `.\+()[]{}^$|`,
  anchor `^…$`. Near-duplicate of `fs_scans/core/query_builder.py:16`; the two
  packages have no cross-imports and different substrates (raw SQL there, ORM
  here), so keep them independent and say so in a comment.
- `glob_to_sql_like(pattern)` — `*`→`%`, `?`→`_`, and **escape literal `%`, `_`**
  with `LIKE_ESCAPE`. This is a deliberate divergence from the fs_scans copy,
  which doesn't escape: job names are full of underscores (`wrf_cycle_01`,
  `cesm_b1850`), so without it `-N 'cesm_b*' -i` silently also matches
  `cesmXb1850` — a wrong answer that looks right. fs_scans has the same latent
  bug against directory names; worth a follow-up there, not fixable from here.
- `LIKE_ESCAPE = "!"`, not `\`. SQLAlchemy's PG compiler doubles backslashes
  while `dialect._backslash_escapes` is True (until a live connection reports
  `standard_conforming_strings=on`), so an offline-compiled `ESCAPE '\'` renders
  as `ESCAPE '\\'` — a different string from what production emits, which makes
  the compile-assert tests both wrong and misleadingly green.
- `glob_match_clause(column, patterns, *, dialect, ignore_case=False)` — returns
  an OR'd expression, or `None` when there's nothing to filter.

**Three branches, not fs_scans' four.** The case-*insensitive* path needs no
dialect branch: `ColumnOperators.ilike()` already compiles to native `ILIKE` on
PG and `lower(col) LIKE lower(?)` on SQLite. Only the case-*sensitive* path must
branch, because SQLite `LIKE` is case-insensitive for ASCII:

| | emitted |
|---|---|
| `ignore_case=True`, either dialect | `col ILIKE :p ESCAPE '!'` / `lower(col) LIKE lower(:p) ESCAPE '!'` |
| case-sensitive, postgresql | `col ~ :p` with the anchored regex |
| case-sensitive, sqlite | `col GLOB :p`, pattern verbatim |

Dialect comes from `self.session.get_bind().dialect.name`, resolved lazily and
only when a pattern is present (the same source `fs_scans/queries/query_engine.py:532`
uses).

## 2. New filters — `job_history/queries/jobs.py`

`_apply_jobs_search_filters` (`:1247`) is the single insertion point, shared by
`jobs_search` (`:1063`) and `jobs_count` (`:1198`). Add to all three signatures:

```
name, ignore_case,
min_eligible_secs, max_eligible_secs,
min_nodes, max_nodes, min_cpus, max_cpus, min_gpus, max_gpus
```

and, per §7, **remove** `has_gpus` and **rename** `status` → `exit_status`
across the same three signatures.

Flat scalar kwargs, not a `{'nodes': (1, 8)}` dict: the published JSON `filters`
contract is flat, the Click options are flat, every existing filter on this
method is flat, and a keyword-only signature turns a typo into an instant
`TypeError` where a dict key needs hand-written validation.

Apply the six range bounds table-driven over
`(eligible_secs, numnodes, numcpus, numgpus)` rather than twelve near-identical
`if` blocks. **Every guard must be `is not None`, never truthiness** — `0` is
meaningful and falsy for `--max-wait-hours 0`, `--min-nodes 0`, `--max-gpus 0`.

Give `_apply_jobs_search_filters` **no defaults** on any parameter, so adding a
filter to `jobs_search` and forgetting `jobs_count` fails loudly instead of
silently returning a total that disagrees with the page.

### Semantics to get right

- **`name=[]` means "no filter"; `account=[]` means "no rows".** These invert,
  inside the same function. It's forced: Click's `multiple=True` hands `()` for
  an unsupplied `-N`, so if the empty sequence meant "no rows", a plain
  `jobhist search --user alice` would return nothing. Loud docstring + a test
  that pins the asymmetry against `account=[]`.
- `name` accepts `str` or `Sequence[str]`, OR'd. Check `isinstance(name, str)`
  first — same str-is-iterable trap `account` already handles at `:1255`.
- **Wait bounds exclude NULL `eligible_secs`**, via ordinary SQL three-valued
  logic. That's correct: a pre-2025 derecho job has *no wait measurement*, not a
  wait of zero, and `max_eligible_secs=3600` must not quietly claim otherwise.
  Note this is *not* the `job_waits_by_resource` trap — that one was intra-query
  (`AVG` skips NULLs, `COUNT(id)` doesn't). Here `jobs_count` runs the identical
  predicate through the same helper, so the count is the row count by
  construction.
- Range filters are NULL-strict (`numnodes IS NULL` genuinely means "unknown").
  With `has_gpus` removed (§7b) there is no longer a competing NULL-inclusive
  variant to reconcile, so this is uniform across all six bounds.
- SQLite `GLOB` honours `[abc]` character classes; the PG regex path treats `[`
  as a literal. Inherited from fs_scans. Document; stick to `*` and `?`.

## 3. `jobs_facets()` — new method on `JobQueries`

Placed after `jobs_count`. Signature mirrors the full filter set exactly (so it
can never drift from the rows it describes), plus:

```python
facets: Sequence[str] = DEFAULT_FACETS,   # ('queue', 'qos', 'exit_status')
self_exclude: bool = True,
limit: Optional[int] = None,
```

Returns `{dimension: [{'value': ..., 'count': int}, ...]}`, each list sorted
count-desc then value-asc (`None` last) so equal counts don't reshuffle between
calls and cause spurious UI diffs.

**Implementation: one statement.** Group by the integer FKs for every requested
dimension simultaneously; resolve display names *after* aggregation via one
small lookup-table read per dimension (once per distinct id, not once per
scanned row); fold in Python. A module-level `_FACET_SPECS` maps
dimension → `(group column, lookup model, name column)`.

**Self-exclusion costs no extra query.** Rather than one query per dimension
with that dimension's filter removed, drop the faceted dimensions' predicates
from the `WHERE` clause and put those dimensions in the `GROUP BY`; the
composite result contains every sub-combination and the fold reconstructs each
facet applying all filters *except* its own. Two real costs: the scan is no
longer narrowed by the excluded predicates, and self-excluding a *selective
indexed* dimension can flip the plan from an index seek to a slice scan
(measured 4.5× for a single-user filter). Hence `user`/`account` are opt-in —
for the plan flip, not for cardinality, which measured free.

**`account` is never self-excluded** (`_FACET_SCOPE_DIMS`). This is a security
property, not an optimization: SAM's `search_jobs`/`count_jobs` raise without a
project precisely because the account filter is the authorization boundary
(`webapp/jobs/service.py:119-120`). A naive self-exclude would emit counts for
projects the requester can't see.

`limit` truncates in the fold with **no "other" bucket** — it would need a
sentinel `value` colliding with either a real name or the `None` used for a NULL
FK. Rows then sum to ≤ `jobs_count`, an easy invariant to test.

Surface a NULL FK as `{'value': None, 'count': N}` rather than dropping it;
dropping makes the facet rows silently under-sum. Note `jobs` has no `NO_JOBS`
sentinel convention — that's `DailySummary` only.

Put the measured costs in the docstring, especially the ~200 s unbounded figure.
**Do not raise on an unbounded window**: that would make `jobs_facets` the only
method in the class with a mandatory window and break the drop-in filter-shape
contract. Document, and let SAM supply a default window.

## 4. CLI — `job_history/cli/cmds/jobhist.py` + `cli/search/commands.py`

All ten new filters get flags on `jobhist search`; facets stay API-only (SAM
doesn't read the CLI or the envelope, and `queue_statistics`/`usage_by_group`
already serve the human case).

- `-N/--name-pattern` (`multiple=True`) and `-i/--ignore-case` — mirrors
  `fs-scans query`.
- `--min-wait-hours`/`--max-wait-hours` in **hours** (what a person asks),
  converted to seconds at the boundary; publish the resolved *seconds* in the
  envelope so a consumer can replay `filters` straight into `jobs_search`.
- `--min-nodes`/`--max-nodes`/`--min-cpus`/`--max-cpus`/`--min-gpus`/`--max-gpus`.

Each new option must also be added to the `filters={...}` dict in
`SearchCommand.execute` — that dict *is* the `kind="search"` JSON contract
(`cli/search/builders.py:38`) and it emits `null` for unset filters, so omitting
one silently drops it from the published envelope. Normalize Click's `()` to
`None` so "unset" stays `null` rather than becoming `[]`.

`offset`/`sort_by`/`sort_dir` stay API-only as today. `--status` becomes
`--exit-status` (§7c).

Document the unindexed-scan reality in three places: the `-N`/`--min-*` help
text, the `search` command docstring (Click prints it above the options), and
the `jobs_search` docstring.

## 5. Ride-alongs

- **Fix the DAT/MD `None` crash.** `job_history/exporters.py:57-59` and `:145`
  format cells with no `None` guard — `f"{None:<9d}"` and `f"{None:<20}"` both
  raise `TypeError`. Rich guards already (`cli/core/output.py:88`); CSV is fine
  via `DictWriter`. Emit `""`. PR #97 made this reachable on derecho via
  `jobhist search -v --format dat`.
- **Delete `_TimeDiffHours`** (`queries/builders.py:79-105`). Dead since `b044414`
  replaced its only call site. The SAM-consumer check that was deferred came
  back clean: no `julianday`/`EXTRACT(EPOCH` counterpart anywhere in SAM's `src/`.
- The wrong *"e.g. 'F' for finished"* docstrings (`queries/jobs.py:999,1028,1111`,
  `cli/cmds/jobhist.py:211`) are subsumed by the §7c rename — fix the prose in
  the same pass, and note `'0'` is success.

## 6. Docs

- `job_history/SCHEMA.md` — `name` glob semantics and the `status` = `Exit_status`
  correction.
- `CLAUDE.md:134` — currently credits only `fs_scans/core/query_builder.py` with
  dialect-aware GLOB/regex; `job_history/queries/builders.py` now has it too.
- `CLAUDE.md` — record the new public `job_history.columns` export (§7a) in the
  key-files table, since it becomes part of the SAM-facing contract.

## 7. Breaking cleanups (coordinated deploy)

All three are cheap to land and each removes something actively misleading. The
full blast radius is 8 plugin sites and 4 SAM sites; verified by grep, listed
below so nothing is discovered late.

### 7a. Promote the column registry out of `cli/`

Move `job_history/cli/search/columns.py` → **`job_history/columns.py`** and
re-export `COLUMNS`, `DEFAULT_COLUMNS`, `VERBOSE_COLUMNS`, `project_row` from
`job_history/__init__.py` (which today exports only `database` + `JobQueries`
symbols).

This isn't cosmetic. The registry is query-layer metadata that happens to live
in the CLI package, which forces `queries/jobs.py` to import it *lazily inside
function bodies* at `:33` and `:1148` — the comment there says so outright:
*"Local import keeps the queries package importable without cli/."* Moving it
lets both become normal module-level imports and deletes the cycle.

Update, then delete the old module (no shim):
`queries/jobs.py:33,1085,1148`, `cli/search/__init__.py:3`,
`cli/search/builders.py:10`, `cli/search/commands.py:16`,
`tests/test_jobs_search.py:13`. SAM: the function-local import at
`webapp/jobs/routes.py:312`.

### 7b. Remove `has_gpus`

Superseded by `min_gpus`/`max_gpus`. Its one distinguishing behaviour —
`has_gpus=False` matching `numgpus IS NULL` — has never matched a row (0 NULLs
in 34M). Removing it deletes the `max_gpus=0` ≠ `has_gpus=False` asymmetry that
would otherwise need a docstring paragraph and a test to pin.

Plugin: `queries/jobs.py:1073,1112,1182,1208,1224,1248,1273-1275`; the four
`test_has_gpus_*` tests at `tests/test_jobs_search.py:437-461` and the assertions
at `:642-643` are rewritten against `min_gpus`/`max_gpus`.

SAM: `webapp/jobs/service.py:73,99,141,161,178,194`. Note `:194` — `has_gpus` is
one of the predicates that makes `count_jobs` abandon the fast
`comp_charge_summary` path and fall through to the plugin; that predicate must
switch to the new kwargs, not silently drop.

**Do not touch SAM's other `has_gpus`.** `system_status/queries/user_proj_queues.py`,
`webapp/dashboards/charts.py`, `dashboards/status/blueprint.py`, and two
`status/partials/*.html` templates use the same identifier for an unrelated
"does this scope have GPU activity" flag. Same name, different feature.

### 7c. Rename `status` → `exit_status`

`Job.status` is PBS `Exit_status` (`sync/pbs.py:250`) — all-numeric, 192/135
distinct values, zero non-numeric across both machines. The name invites exactly
the mistake the current docstrings make.

Rename the **user-facing surfaces only**: the `COLUMNS` key (`source` stays
`"job.status"`), the `jobs_search`/`jobs_count`/`jobs_facets` parameter, the
facet dimension name, and the CLI flag `--status` → `--exit-status`. Also rename
on the legacy `jobs_by_user`/`jobs_by_account` (`jobs.py:990,1020`) for
consistency — unused by SAM, cheap to keep in step.

**Deliberately not renamed: the DB column and the ORM attribute.** Keeping
`Job.status` mapped to column `status` means no prod migration, and leaves
`sync/pbs.py:250` and the `Job.__table__.columns` filtering in
`_bulk_insert_jobs` (`sync/base.py:648`) untouched — that path keys off column
names, so an attribute rename there is a real risk for no user-visible gain.
Add a one-line comment on the model saying the attribute mirrors the DB column
while the query API says `exit_status`.

Plugin: `queries/jobs.py:990,999,1020,1028,1072,1111,1182,1207,1224`,
`cli/cmds/jobhist.py:210,236`, `cli/search/commands.py:29,49`, the `COLUMNS` key.
SAM: `webapp/jobs/service.py:137`, `routes.py:68` (`_VERBOSE_EXTRAS`), `:172`
(request arg), `cli/accounting/commands.py:1788` (`JOB_COLUMNS`).

Because the `COLUMNS` key is also the **row-dict key** returned by `jobs_search`,
SAM's column tuples must change in the same deploy or its table renders a blank
column. That is the one place the coordination actually matters.

## Explicitly out of scope

- **SAM-side feature work.** The §7 renames require mechanical SAM edits in the
  same deploy (enumerated above), but nothing beyond that. Still SAM's job, not
  this PR's: adding `queued`/`eligible_secs`/`run_count` to `_VERBOSE_EXTRAS`
  (`webapp/jobs/routes.py:67-76`, which also feeds `_SORT_WHITELIST` via
  `_DEFAULT_COLS`), wiring the glob into a search box, calling `jobs_facets`,
  caching, and bucketing raw exit codes into human labels.
- **No index on `Job.name`** — decided after measuring. A plain B-tree can't
  serve `LIKE 'foo%'` at all under the `en_US.utf8` collation (would need
  `text_pattern_ops`), and even then only helps left-anchored patterns, never
  the `*substr*` case a search box generates. The thing that would help is a
  `pg_trgm` GIN index (extension available 1.6, not installed → a manual CNPG
  `CREATE EXTENSION`, as in PR #78) — PG-only, no benefit to SQLite. **Revisit
  trigger:** if the dashboard ever exposes an *unbounded-window* name search.
  Date-bounded, the glob is free.
- `GROUPING SETS`, an `include_hours` facet variant, a `daily_summary` facet
  fast path, caching (SAM's layer — and job history has no content-addressed
  freshness key like fs_scans' scan dates), and `ix_jobs_account_end` /
  `ix_jobs_queue_end` composites (separate PR; `ix_jobs_account_submit` is on
  `submit` while the date filter is on `end`, so an account+date query filters
  one predicate per row).

## Verification

1. `pytest job_history/tests/` — full suite green (baseline: 659 passed, 1 skipped).
2. **Glob translation units** in `test_query_builders.py`, which already has the
   `_sql(expr, dialect_cls)` compile helper and both dialect imports.
   Metacharacter escaping, and the `_`/`%` escaping that diverges from fs_scans.
3. **Dialect dispatch** by compiling offline against
   `sqlalchemy.dialects.postgresql.dialect()` and asserting emitted SQL + bind
   values — the suite runs on SQLite, so this is the only way to reach the PG
   regex/ILIKE branches.
4. **Filter behaviour** in `test_jobs_search.py`: new fixtures for names
   (including the `cesm_b1850` vs `cesmXb1850` underscore-leak decoy and a case
   variant) and for `eligible_secs` (including NULL). Assert `jobs_count` agrees
   with `len(jobs_search(...))` for every new filter; assert NULL `eligible_secs`
   is excluded by *both* bounds; assert `name=[]` is a no-op while `account=[]`
   is not.
5. **Signature-parity test** — `jobs_search`, `jobs_count`, and
   `_apply_jobs_search_filters` expose the same filter set, and the helper's
   params have no defaults.
6. **The one-scan guard for facets** — reuse the `before_cursor_execute` idiom
   already at `test_jobs_search.py:339-357`: request 4 facets, assert exactly one
   aggregate statement against `jobs`, and assert it contains `jobs.queue_id` but
   **not** `SELECT queues.queue_name`. That's the regression guard that matters
   most; it fails loudly if someone "simplifies" the FK back to the hybrid.
7. **Breaking-change sweep.** After §7, grep both repos for the retired names —
   `has_gpus` (excluding SAM's unrelated `system_status`/`charts`/`status`
   usages), `status=` as a search kwarg, and `cli.search.columns` — and confirm
   zero stale references. Import `job_history` in a fresh interpreter to prove
   the `queries`↔`cli` cycle is gone and the local imports at `jobs.py:33,1148`
   can be module-level.
8. **End-to-end against local dev PG** (13M/21M rows, `.env` on localhost):
   ```
   jobhist search -m derecho --start-date 2026-06-01 --end-date 2026-06-30 \
       -N 'cesm_*' -i --min-wait-hours 1 --min-nodes 2 --limit 20
   jobhist search -m derecho --start-date 2026-06-01 --end-date 2026-06-30 \
       -N '*run*' -v --format dat --output-dir /tmp   # exercises the None fix
   ```
   Confirm a facets call over the same window lands near the measured
   ~150-200 ms.
9. **SAM smoke test** by pinning `HPC_USAGE_QUERIES_REF` to this branch's SHA
   and loading the existing jobs drill-down drawer — it exercises `jobs_search`,
   `jobs_count`, `list_qos_names`, and the `COLUMNS` import in one page, so it
   catches all three §7 renames at once.
