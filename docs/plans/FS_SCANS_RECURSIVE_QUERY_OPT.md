# Plan: Optimize fs_scans recursive subtree-scan queries

**Status:** Design finalized (see §6.5) — implementation pending, to be done
on-machine where the import runs and `/glade/campaign` is mounted (so each rebuild
can be measured against live data). This PR is **doc-only**.
**Owner:** unassigned. Access-pattern questions (§8) resolved with the SAM owner —
see §6.5.
**Priority:** High UX impact, low operational risk (see §1).
**Prereq context:** none — this doc is self-contained and restartable from a cold session.

---

## 1. TL;DR

The fs_scans web UI (SAM, consuming this repo's `fs_scans` plugin) issues a
**recursive subtree walk** for any *scoped* directory/owner query. On a large
scope it takes **~85–95 seconds**. This is now the dominant cost on the
filesystem-scan feature; everything else (histogram tabs, scan_metadata
lookups) is sub-100 ms.

It is **latency, not capacity**: the read load runs on the CNPG **`-ro`
replica** (SAM points `FS_SCAN_PG_HOST` at `csg-postgres-ro.k8s.ucar.edu`), so
the primary is untouched and the cluster is healthy. Redis caching (weekly
content-addressed + 30-min filtered buckets, SAM-side) absorbs repeat loads.
So this is **important-but-not-urgent**: bad page-load latency on cold/uncached
scoped queries, but no stability risk. Tune for correctness + a big latency win,
not for firefighting.

**Goal:** bring a scoped owner-summary / large-directories query from ~85 s to
**< ~1 s** (the whole-collection fast path already achieves 0.45 s; we want the
*scoped* path to match).

---

## 2. The offending query

Two call sites generate the same `WITH RECURSIVE … descendants` shape:

| File | Function | What it powers |
|------|----------|----------------|
| `fs_scans/queries/query_engine.py:556-607` | owner-summary dynamic path | "User / group counts" tab, scoped |
| `fs_scans/core/query_builder.py:298-331` | `with_path_prefix_ids()` | "Large directories" / directory explorer, scoped |

The pattern (owner-summary variant):

```sql
WITH RECURSIVE
ancestors AS (SELECT dir_id FROM directories WHERE dir_id IN (:ids)),
descendants AS (
  SELECT dir_id FROM ancestors
  UNION ALL
  SELECT d.dir_id FROM directories d JOIN descendants p ON d.parent_id = p.dir_id
)
SELECT s.owner_uid, SUM(s.total_size_nr), SUM(s.file_count_nr), COUNT(*)
FROM directories d
JOIN directory_stats s USING (dir_id)
JOIN descendants USING (dir_id)
WHERE s.owner_uid IS NOT NULL AND s.owner_uid >= 0
GROUP BY s.owner_uid ORDER BY total_size DESC;
```

### Fast path vs slow path (the gap)
`query_engine.py:502` — `has_filters = any([min_depth, max_depth, path_prefixes])`.
- **No filters → fast path** (`query_engine.py:505-539`): reads the pre-computed
  `owner_summary` table. ~0.45 s. Used by the unscoped resource-mode landing.
- **Any filter → slow path** (`query_engine.py:541+`): the recursive CTE above.
  **SAM's project scoping ALWAYS passes `path_prefixes`** (to bound results to a
  project's directories), so project members *always* hit the slow path. The
  whole-collection fast path only helps the unscoped case.

This is the core gap: **there is no pre-computed path for *scoped* aggregation.**

---

## 3. Why it's slow — measured `EXPLAIN ANALYZE`

Reproduced on the live `campaign` DB, schema `cgd`, scope `dir_id=2` (project
"amp", 14.6 M recursive subdirs). **85.5 s total.** Plan highlights:

```
Sort (actual time=85455 ms) rows=26                         -- returns 26 owner rows
  CTE descendants
    Recursive Union (actual time=..27337 ms) rows=14,595,518 -- 27s just to enumerate
      Storage: Disk  Maximum Storage: 131072kB               -- CTE spills to disk
      Nested Loop -> Index Scan ix_directories_parent
        Index Searches: 14,595,518                           -- one probe PER descendant
  GroupAggregate
    Sort  Sort Method: external merge  Disk: 340680kB        -- GROUP BY spills 340MB
      Nested Loop rows=10,238,187
        CTE Scan on descendants rows=14,595,518
          Storage: Disk  Maximum Storage: 199544kB           -- CTE re-scanned from disk
        Index Only Scan directories_pkey  Index Searches: 14,595,518
        Index Scan directory_stats_pkey   Index Searches: 14,595,518
```

**Diagnosis:**
1. The recursive enumeration is a **per-row tree walk** — 14.6 M index probes on
   `ix_directories_parent` (the index exists and is used; the problem is the row
   *count*, not a missing index).
2. Recursive CTEs are **optimization fences** — `descendants` is materialized to
   disk and scanned again (199 MB), so the walk is effectively paid ~twice.
3. The final `GROUP BY owner_uid` over 10.2 M rows spills an external merge sort
   (340 MB). `work_mem=64MB` is exceeded no matter what — but spill is *not* the
   dominant cost; the 14.6 M-row walk is. **Raising `work_mem` will not fix this.**
4. We scan ~15 M directories to produce **26 owner rows**. The work is
   fundamentally disproportionate to the answer.

---

## 4. Standalone reproducer (no UI / no peer needed)

The data is live; drive the query directly. Pick a scope, run `EXPLAIN ANALYZE`.

```bash
NS=pg-testing; POD=csg-postgres-2   # primary (idle); or csg-postgres-1 (replica)

# (a) find big scopes in a collection schema (depth-4 dirs = project roots):
kubectl -n $NS exec $POD -c postgres -- psql -U postgres -d campaign -tAXqc \
 "SET search_path=cgd,public;
  SELECT d.dir_id, d.name, s.dir_count_r
  FROM directories d JOIN directory_stats s USING(dir_id)
  WHERE d.depth=4 ORDER BY s.dir_count_r DESC LIMIT 6;"
# cgd: dir_id=5 (tss, 34M), dir_id=2 (amp, 14.6M), dir_id=3 (cas, 7.5M) ...

# (b) baseline the slow query (substitute the scope dir_id):
kubectl -n $NS exec $POD -c postgres -- psql -U postgres -d campaign -Xqc \
 "SET search_path=cgd,public;
  EXPLAIN (ANALYZE, BUFFERS)
  WITH RECURSIVE
  ancestors AS (SELECT dir_id FROM directories WHERE dir_id IN (2)),
  descendants AS (SELECT dir_id FROM ancestors UNION ALL
                  SELECT d.dir_id FROM directories d JOIN descendants p ON d.parent_id=p.dir_id)
  SELECT s.owner_uid, SUM(s.total_size_nr), SUM(s.file_count_nr), COUNT(*)
  FROM directories d JOIN directory_stats s USING(dir_id) JOIN descendants USING(dir_id)
  WHERE s.owner_uid IS NOT NULL AND s.owner_uid >= 0
  GROUP BY s.owner_uid ORDER BY 2 DESC;"
```

Smaller scopes (e.g. `dir_id=6` ppc, 168 K) run in ~1 s — good for fast
iteration; use a 14 M+ scope to confirm the win at scale. To measure via the
cumulative ranking instead: `SELECT pg_stat_statements_reset();` (as superuser
on the relevant instance), drive load, then read `pg_stat_statements` — see
`scripts/cirrus_healthcheck.sh` §9 (now replica-aware).

There is also a **local** path: the SQLite generation pipeline produces per-collection
`.db` files; the same query runs there via `fs_scans/core/query_builder.py`. A
unit-level reproducer can build a synthetic deep tree in an in-memory SQLite DB
(see `fs_scans/tests/`) — preferred for a regression test that runs in CI.

---

## 5. Schema facts (grounding for any fix)

`fs_scans/core/models.py`:
- **`directories`**: `dir_id` PK, `parent_id` FK (self), `name` (component only,
  not full path), `depth`. Indexes: `directories_pkey`, `ix_directories_parent`
  (on `parent_id`). **No materialized full-path / lineage column.**
- **`directory_stats`**: PK `dir_id`. Has **non-recursive** (`*_nr`) and
  **pre-computed recursive** (`*_r`: `file_count_r`, `total_size_r`,
  `dir_count_r`, `max_atime_r`) columns, plus `owner_uid`, `owner_gid`. The `_r`
  columns already aggregate *each directory's own subtree* — so a directory's own
  recursive totals need no walk; the walk is only to **enumerate / group
  descendants by owner**.
- **`owner_summary`**: whole-collection per-owner precompute (the fast path).
  Built in the importer's aggregation pass.
- Histogram fast-path tables `access_histogram`, `size_histogram` per schema.

**Constraints any fix MUST respect:**
1. **Dual backend** — SQLite (default; the `fs_scans/PBS/` generation pipeline is
   SQLite-only) **and** PostgreSQL/CNPG. Any new table/column must build in the
   SQLite importer AND survive `fs-scans consolidate` (SQLite→PG `COPY` +
   schema swap, `fs_scans/consolidate/consolidator.py`). No PG-only types
   (e.g. avoid `ltree`) unless mirrored on SQLite.
2. **Result parity** — existing callers expect identical rows/ordering. Note the
   deterministic tiebreaker `ORDER BY <sort>, d.dir_id ASC` in
   `query_builder.py:401`. Preserve it.
3. **Import-time cost** — the importer is already 3-pass (discovery → stats →
   recursive aggregation). A closure table / lineage build is a 4th pass; weigh
   build time + storage against the query win.

---

## 6. Options (with trade-offs)

**A. Closure table** (recommended starting point).
A table `dir_closure(ancestor_id, descendant_id)` with index on `ancestor_id`.
The recursive walk becomes one indexed join:
```sql
SELECT s.owner_uid, SUM(...), COUNT(*)
FROM dir_closure c
JOIN directory_stats s ON s.dir_id = c.descendant_id
WHERE c.ancestor_id IN (:ids) AND s.owner_uid >= 0
GROUP BY s.owner_uid;
```
- ✅ Works at **any** scope depth; portable (plain table + index) across SQLite/PG.
- ✅ Removes the recursion fence + the double-materialization.
- ⚠️ Storage is O(Σ depth) — for 57 M dirs at avg depth ~10 that's ~500 M+ rows.
  Mitigate by **bounding closure depth** (only store ancestors up to the depth
  SAM actually scopes at — e.g. depth ≤ 4–6 "project root" levels), which caps
  size and still covers the real access pattern. Confirm the depth distribution
  of SAM's `path_prefixes` with the peer (§8).
- ⚠️ Build cost at import + consolidation.

**B. Denormalized scope id** (cheapest, if scoping is always at a fixed level).
Add `scope_root_id` to `directory_stats` = the dir_id of each row's depth-N
"project root" ancestor. Then `WHERE scope_root_id = :id GROUP BY owner_uid` is a
single indexed scan, no recursion.
- ✅ Minimal storage (one column), trivial query.
- ❌ Only works if SAM always scopes at that fixed level. Resource-mode
  drill-down / sub-project paths would still need the walk. **Verify access
  patterns first** (§8) — likely too rigid alone, but a cheap complement to A.

**C. Per-scope owner-summary precompute** (extends the existing fast path).
Generalize `owner_summary` to `owner_summary_by_scope(scope_id, owner_uid, …)`
for a bounded set of scope roots (the registered filesets / depth-4 dirs).
- ✅ Reuses the proven fast-path mechanism; O(1) query.
- ⚠️ Only covers pre-chosen scope roots; arbitrary drill-down falls back.
- Pairs naturally with the closure table (A handles the long tail, C the hot set).

**D. Query-shape fixes (incremental, no schema change).**
- Push `LIMIT`/top-N into the directory-listing path so we don't enumerate the
  whole subtree to return 50 rows (helps "large directories", not owner-summary
  which must see all rows to aggregate).
- `MATERIALIZED`/`NOT MATERIALIZED` CTE hints (PG-only; no SQLite analog).
- These shave time but **do not remove** the 14 M-row walk. Stopgap at best.

**Recommendation (original):** **A (depth-bounded closure table)**, optionally
complemented by **C**. **Superseded — see §6.5**, which adopts a leaner realization
of A's goal (denormalized ancestor-at-depth *columns* instead of a closure table:
same walk-elimination for both call sites, O(N) storage instead of O(N × depth),
no new table).

---

## 6.5 Chosen design — denormalized ancestor-at-depth columns

**Access pattern resolved (§8), with the SAM owner:**
- Scoping happens at **arbitrary depth**; drill-down below a fileset root must stay
  *reasonably* fast (faster is nice, not a hard <1s).
- **Both** call sites (§2) must be optimized.
- Very deep directories are **real but rare**; the web UI only needs a few levels
  below each `.db`'s root. Deeper scopes may fall back to the slow path.

This rules out C-alone (only fixes owner/group summary, not the directory explorer)
and B-alone (single fixed depth). It points at A's goal — eliminate the tree walk
for both call sites at any bounded depth — but a closure table is heavier than
needed.

### The mechanism
Add `SCOPE_MAX_DEPTH` integer columns `anc_d1 … anc_dN` to `directory_stats`, where
`anc_d{k}(row)` = the `dir_id` of that row's ancestor at depth `k`:
- row depth `> k` → ancestor on its path at depth `k`
- row depth `== k` → its own `dir_id` (a directory is its own depth-k ancestor)
- row depth `< k` → `NULL`

A scoped query for ancestor `X` at depth `k` becomes `WHERE anc_d{k} = X` — a single
indexed equality replacing the `WITH RECURSIVE … descendants` walk.

**Parity.** The current `descendants` CTE for `X` yields exactly
`{X} ∪ descendants(X)`; `{d : anc_d{k}(d) = X}` is the identical set (X via its
self-row, deeper descendants via their depth-k ancestor). So results, aggregates,
and the `ORDER BY …, d.dir_id ASC` tiebreaker are byte-identical. Multi-prefix →
`OR` of per-depth predicates; SAM's `_collapse_prefixes` already removes nesting,
so no double counting. A scope deeper than `SCOPE_MAX_DEPTH` → recursive-CTE
fallback (small subtree, already ~1s). Probe column presence defensively, mirroring
the `owner_summary` `COUNT(*)` fast-path check (`query_engine.py:507-512`).

### Measured cost (local `.db` set in `fs_scans/data/`)
Anchored on `cesm.db` (4.87M rows): `directory_stats` ≈ **57 B/row**; one composite
index ≈ **14.5 B/row**. Largest collection `cgd.db` = **57.4M dirs**, max depth 35,
17 GB; bulk of dirs at **depth 11–15** (depth 12 = 33M); collection root ≈ depth 3.

| Item | cgd estimate | Note |
|---|---|---|
| `anc_d*` columns (≈6 ints) | **+~1.6 GB** (~+10% of `.db`) | ~every row gets the cols |
| **each** composite scope index | **~0.8 GB** | the lever: 3×6 = 18 ≈ +15 GB (≈ doubles cgd) |
| column-population pass | **~minutes** | set-based `UPDATE…FROM` per depth, pass2b cost class |

### Refinement — split work across backends
The latency-critical consumer is the **PG/CNPG web UI**, not the local SQLite CLI:
- **SQLite import**: populate `anc_d*` columns only (+~10% `.db`, one extra
  set-based pass). Build **no** heavy scope indexes → highly-optimized pipeline
  essentially untouched.
- **Consolidation → PG**: build covering scope indexes **PG-side only**, post-COPY,
  via the existing deferred-index + raised `maintenance_work_mem` path. On PG use
  `(anc_d{k}, owner_uid) INCLUDE (total_size_nr, file_count_nr)` (and the `owner_gid`
  / `total_size_r` analogues) for index-only aggregates with no GROUP BY sort spill.
- Columns ride the existing `directory_stats` COPY automatically (no `_LOAD_ORDER`
  or encoder change); the local CLI keeps working via the recursive-CTE fallback.

Index only the selective depth band (a few levels below each root); skip the root
depth itself (non-selective — that scope is already the whole-collection fast path).
`SCOPE_MAX_DEPTH` and the indexed band are tuning knobs to finalize on-machine
against the real depth histogram.

### Touch points
- `fs_scans/core/models.py` — `SCOPE_MAX_DEPTH` + `anc_d1..N` columns on `DirectoryStats`.
- `fs_scans/importers/pass2c.py` (new) wired into `run_import()` after
  `pass2b_aggregate_recursive_stats()`; top-down by depth (ascending), the
  `UPDATE … FROM` mirror of pass2b. Parent's columns inherited; own depth column
  set to `dir_id`.
- `fs_scans/importers/add_table_indexing.py` — dialect-aware: PG covering/composite
  scope indexes; SQLite none (or minimal).
- `fs_scans/queries/query_engine.py` — owner/group summary slow path: resolve each
  prefix to `(dir_id, depth)`; if all depths ≤ bound and columns present, use
  `anc_d{k}` predicate, else recursive CTE.
- `fs_scans/core/query_builder.py` — `with_path_prefix_ids()` lineage branch;
  preserve the `dir_id` tiebreaker.
- `fs_scans/queries/facade.py` — resolve prefix depths once, keep the decision
  central.
- `fs_scans/consolidate/consolidator.py` — verify only (columns auto-COPY; new
  indexes flow through `add_directory_stats_indexing`).
- `fs_scans/tests/test_recursive_scope_opt.py` (new) — synthetic deep multi-owner
  tree; assert lineage-path == recursive-path (rows + ordering) for owner/group
  summary and directory listing, single/multi prefix, and a >bound fallback case.

---

## 7. Where the build lives

> Superseded by §6.5 "Touch points" for the chosen column design. The closure-table
> notes below are retained for reference; the same layers are involved.

- **Importer:** add a pass in `fs_scans/importers/importer.py` (alongside the
  recursive aggregation that already fills `*_r` and `owner_summary`). Build the
  closure / scope columns from the `parent_id` tree in one traversal.
- **Models/migration:** new table/columns in `fs_scans/core/models.py`; ensure
  `bin/update_jobs_db.py`-style idempotent creation if applied to existing DBs
  (note: that script is job_history's — fs_scans may need its own migration hook;
  check how `fs_scans` creates schema).
- **Consolidation:** `fs_scans/consolidate/consolidator.py` must `COPY` the new
  table into the `<collection>_staging` schema and include it in the atomic
  schema swap. Add indexes post-COPY (deferred-index pattern) + `ANALYZE`.
- **Query layer:** branch `query_engine.py` owner-summary and
  `query_builder.with_path_prefix_ids()` to use the closure join when the table
  is present, else fall back to the recursive CTE (graceful degradation, mirrors
  the `owner_summary` `count > 0` probe at `query_engine.py:508`).
- **Facade:** `fs_scans/queries/facade.py` is the single source of truth — keep
  the fast/slow decision there if possible.

---

## 8. Coordinate with the peer / SAM before building

**Resolved** (with the SAM owner) — see §6.5: scoping is at arbitrary depth,
drill-down must stay *reasonably* fast, both call sites in scope, deep dirs rare
(only a few levels below each `.db` root matter). This selected the §6.5 design.

Original framing retained for context — the fix shape depends on **how SAM scopes**:
- At what tree depth do `path_prefixes` resolve? (Registered fileset roots only,
  or arbitrary drill-down paths?) This sets the closure depth bound (§6A) and
  whether B/C are viable.
- Is the resource-mode file-browser drill-down (descend below a fileset, see
  SAM PR #322) expected to stay fast, or is it acceptably rare/cached?
- SAM caches results (Redis, scan-date-keyed) — confirm the cold-load latency
  target so we size the win correctly.

SAM repos/PRs for reference: `sam-queries#321` (plugin wire-up), `#322`
(web UI + explorer). The whole-collection fast path is `hpc-usage-queries#77`.

---

## 9. Validation plan

1. **Before:** run §4 reproducer on 2–3 scopes (small/medium/large, e.g. cgd
   dir_id 6 / 3 / 5) on the primary; record `EXPLAIN ANALYZE` total ms.
2. **After:** same scopes, same command; target **< ~1 s** on the 14 M scope and
   no disk spill in the plan.
3. **Parity:** assert the optimized query returns identical owner rows (and
   identical ordering incl. the `dir_id` tiebreaker) as the recursive version —
   add a test in `fs_scans/tests/` over a synthetic SQLite tree.
4. **Both backends:** run `pytest fs_scans/tests/` (SQLite) and verify against a
   PG schema (the `campaign` DB) after a test consolidation.
5. **End-to-end:** reset `pg_stat_statements` on the replica, have the UI driven,
   confirm the recursive shape no longer tops the ranking (see
   `scripts/cirrus_healthcheck.sh` §9 per-instance pg_stat_statements).

## 10. Non-goals
- CNPG/helm tuning — already done (`#78`, `#79`); not the lever here.
- The histogram tabs (`access_histogram`/`size_histogram`) — already fast.
- Raising `work_mem` — measured irrelevant to the dominant cost (§3).
