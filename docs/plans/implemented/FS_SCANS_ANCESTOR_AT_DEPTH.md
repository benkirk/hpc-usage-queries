# Spec: Denormalized ancestor-at-depth columns for fs_scans scoped queries

**Status:** Design spec — implementation pending, to be done on-machine where the
import runs and `/glade/campaign` is mounted (so each rebuild can be measured
against live data). This document is **doc-only** and **self-contained**: a fresh
session can execute §7 without any other file.
**Relationship:** This is the detailed, restartable realization of the option
chosen in `FS_SCANS_RECURSIVE_QUERY_OPT.md` §6.5. That doc surveys all options
(A closure table, B fixed-depth scope id, C per-scope precompute, D query-shape);
this one specifies the winner. Client measurements: `FS_SCANS_SLOW_CALLS.md`.
**Goal:** scoped owner-summary / large-directories / histogram queries from
**~60–95 s → < ~1 s** (the whole-collection fast path already does 0.45 s; we want
the *scoped* path to match).

---

## 1. TL;DR

The fs_scans web UI (SAM, consuming this repo's `fs_scans` plugin) answers any
*scoped* directory/owner query with a **recursive subtree walk** over
`directories.parent_id`. The client's capture (`FS_SCANS_SLOW_CALLS.md`) shows one
query shape — `WITH RECURSIVE … descendants` — dominates **every** slow call
(`owner_summary`, `group_summary`, `list_directories`, `access_history`,
`file_size_histogram`); cost scales with **subtree size, not result size** (≈15 M
directories walked to return 26 owner rows).

The fix: for every `directory_stats` row, **precompute the `dir_id` of its ancestor
at each fixed depth** in columns `anc_d1 … anc_dN`. A scoped query for an ancestor
`X` resolved at depth `k` then becomes `WHERE anc_d{k} = X` — a single indexed
equality that replaces the entire recursive walk. O(N) storage (one int per
indexed depth per row), no new table, portable across the SQLite and PostgreSQL
backends.

---

## 2. The mechanism (high level)

Add `SCOPE_MAX_DEPTH` integer columns `anc_d1 … anc_dN` to `directory_stats`. For a
row at depth `d`, the value of `anc_d{k}` is:

| Row depth vs `k` | `anc_d{k}` value | Meaning |
|---|---|---|
| `d > k`  | the `dir_id` of the ancestor on its path at depth `k` | a descendant of that ancestor |
| `d == k` | its own `dir_id` | a directory is its own depth-k ancestor |
| `d < k`  | `NULL` | shallower than depth k — not in any depth-k subtree |

A scoped query for ancestor `X`, where `X` lives at depth `k`, becomes
`WHERE anc_d{k} = X`. Multiple prefixes → an `OR` of per-depth predicates.

Illustrative rewrite of the owner-summary slow path
(`query_engine.py:557-609`):

```sql
-- BEFORE — recursive walk (the offending shape):
WITH RECURSIVE
ancestors AS (SELECT dir_id FROM directories WHERE dir_id IN (:a0)),
descendants AS (
  SELECT dir_id FROM ancestors
  UNION ALL
  SELECT d.dir_id FROM directories d JOIN descendants p ON d.parent_id = p.dir_id)
SELECT s.owner_uid, SUM(s.total_size_nr), SUM(s.file_count_nr), COUNT(*)
FROM directories d JOIN directory_stats s USING (dir_id)
JOIN descendants USING (dir_id)
WHERE s.owner_uid >= 0
GROUP BY s.owner_uid ORDER BY 2 DESC;

-- AFTER — single indexed equality (X at depth 6; second prefix at depth 4):
SELECT s.owner_uid, SUM(s.total_size_nr), SUM(s.file_count_nr), COUNT(*)
FROM directory_stats s
WHERE (s.anc_d6 = :a0 OR s.anc_d4 = :a1)
  AND s.owner_uid >= 0
GROUP BY s.owner_uid ORDER BY 2 DESC;
```

The `directories` join disappears entirely for the aggregate cases — everything the
query needs (`owner_uid`, the `_nr` sums, the `anc_d{k}` predicate) lives on
`directory_stats`.

---

## 3. Parity — why results are byte-identical

The recursive CTE for `X` produces exactly `{X} ∪ descendants(X)`. The set
`{ d : anc_d{k}(d) = X }` is the **same set**:

- `X` itself is included via its self-row (`anc_d{k}(X) = X` because `depth(X)=k`).
- every descendant of `X` is included via its depth-k ancestor slot.
- every row shallower than `k` is excluded (its `anc_d{k}` is `NULL`).
- no unrelated row at depth `k` matches (its `anc_d{k}` is its own `dir_id`, `≠ X`).

So the row set, all aggregates, and the deterministic tiebreaker
`ORDER BY <sort>, d.dir_id ASC` (`query_builder.py:401`) are preserved exactly.

**Multi-prefix safety.** The facade's `_collapse_prefixes()`
(`facade.py:80-92`) already drops any prefix nested under another before the query
runs, so the `OR` of per-depth predicates cannot match a directory twice → no
double counting.

**Fallbacks (must remain correct, not necessarily fast).**
- A scope resolved **deeper than `SCOPE_MAX_DEPTH`** → fall back to the recursive
  CTE. Such subtrees are small and already run in ~1 s.
- Columns **absent** (older `.db`, or SQLite where we chose not to populate) → fall
  back to the recursive CTE. Probe column presence defensively, mirroring the
  `owner_summary` `COUNT(*)` fast-path check at `query_engine.py:511-518`.

---

## 4. Expected performance benefit

The measured slow plan (`EXPLAIN ANALYZE`, cgd scope `amp`, 14.6 M subdirs, 85.5 s):

- **27 s** just to enumerate `descendants` — 14.6 M index probes on
  `ix_directories_parent`, one per descendant.
- The recursive CTE is an **optimization fence** — materialized to disk (199 MB)
  and re-scanned, so the walk is paid ~twice.
- The final `GROUP BY owner_uid` over 10.2 M rows spills a 340 MB external merge.

The rewrite removes all three: a single indexed equality scan over the precomputed
column. On PostgreSQL a **covering** index — `(anc_d{k}, owner_uid) INCLUDE
(total_size_nr, file_count_nr)` (plus the `owner_gid` / `total_size_r` analogues) —
yields an **index-only aggregate**: no heap fetch, no `directories` join, no sort
spill. Target **< ~1 s**, matching the existing whole-collection fast path.

---

## 5. Storage and build cost (grounded in `fs_scans/data/`)

Largest collection `cgd.db` = **57.4 M** directories, max depth 35, 16 GB; bulk of
rows at **depth 11–15** (depth 12 alone = 33.1 M); collection root ≈ depth 3.
Anchored on `cesm.db` (4.87 M rows): `directory_stats` ≈ 57 B/row; one composite
index ≈ 14.5 B/row.

| Item | cgd estimate | Note |
|---|---|---|
| `anc_d*` columns (~6 ints) | **+~1.6 GB** (~+10 % of `.db`) | ~every row gets the cols |
| **each** PG covering scope index | **~0.8 GB** | the lever; build PG-side only |
| column-population pass | **~minutes** | set-based `UPDATE…FROM` per depth, pass2b cost class |

`dir_id` maxes around 57 M, well inside a signed 4-byte `Integer`, so `anc_d*` are
`Integer` (4 B) columns — **not** `BigInteger` (the stat columns are `BigInteger`
only because they hold unsigned UIDs/sizes; ancestor ids never need that range).

---

## 6. Backend split (the key refinement)

The latency-critical consumer is the **PG/CNPG web UI**, not the local SQLite CLI.
Split the work so the heavily-optimized SQLite generation pipeline stays cheap:

- **SQLite import:** populate `anc_d*` columns only (+~10 % `.db`, one extra
  set-based pass). Build **no** heavy scope indexes. The local CLI keeps working via
  the recursive-CTE fallback — the columns are dormant there.
- **Consolidation → PG:** build the covering scope indexes **PG-side only**,
  post-COPY, through the existing deferred-index + raised `maintenance_work_mem`
  path (`consolidator.py:260-272`). The columns ride the existing `directory_stats`
  COPY automatically — the consolidator enumerates columns from
  `model.__table__.columns` at runtime (`consolidator.py:100,166`), so **no
  manifest / `_LOAD_ORDER` change is needed**.

---

## 7. Implementation plan — layer by layer

A fresh session executes top to bottom. Verified anchor points are in parentheses.

| Layer | File | Change |
|---|---|---|
| **Schema** | `fs_scans/core/models.py` (after `owner_gid`, ~line 86) | Add `SCOPE_MAX_DEPTH` constant and `anc_d1 … anc_dN = Column(Integer, nullable=True)`. No `ForeignKey` — the consolidator drops FKs for bulk COPY anyway, and the column is a denormalized lookup, not a referential constraint. |
| **Populate** | `fs_scans/importers/pass2c.py` (**new**) wired into `run_import()` after `pass2b_aggregate_recursive_stats()` (`importer.py:~133`) and before `pass3` (`importer.py:~139`) | **Top-down, depth ascending** — the mirror of pass2b. For each depth `D` from low to high: one `UPDATE … FROM` joins each row at depth `D` to its parent's `directory_stats` row to **inherit** `anc_d1 … anc_dN`; then, if `D ≤ SCOPE_MAX_DEPTH`, override `anc_d{D} = dir_id`. Because the parent (depth `D-1`) is already populated, inherited slots `anc_d1..anc_d{D-1}` are correct, `anc_d{D}` is set to self, and `anc_d{D+1..N}` remain `NULL`. Reuse pass2b's idioms: per-depth `session.commit()`, the rich progress bar, generic `text()` SQL (SQLite 3.33+ / PG `UPDATE…FROM`). Skip depths at/above the collection root (no useful scope there). |
| **Index** | `fs_scans/importers/add_table_indexing.py` | Make `add_directory_stats_indexing` dialect-aware. **PG:** covering `(anc_d{k}, owner_uid) INCLUDE (total_size_nr, file_count_nr)`, the `owner_gid` analogue, and `(anc_d{k}, total_size_r)` for `list_directories` — **only over the selective depth band** (a few levels below the root). **SQLite:** none, or minimal. Skip the root depth itself (non-selective — that scope is already the whole-collection fast path). |
| **Resolve** | `fs_scans/queries/query_engine.py` `resolve_path_to_id()` (132-185) + `fs_scans/queries/facade.py` | Extend the resolver to return `(dir_id, depth)` — the existing N-way join already lands on the target directory; add `d{N}.depth` to its SELECT. Resolve each prefix's depth **once** in the facade (`_resolve_scope`, 185-234) and thread the fast/slow decision down — keep the decision central. |
| **Owner / group** | `fs_scans/queries/query_engine.py` owner-summary slow path (557-609) and group-summary (~917-996) | When `path_prefixes` are present, **all** resolved depths `≤ SCOPE_MAX_DEPTH`, and columns present: build the `WHERE (anc_d{k}=:a0 OR …)` predicate against `directory_stats` (drop the `directories` join and CTE). Otherwise keep the recursive CTE. |
| **Listing** | `fs_scans/core/query_builder.py` `with_path_prefix_ids()` (298-331) | Add a lineage branch that emits the `anc_d{k}` predicate instead of appending the `descendants` CTE. **Preserve the `dir_id` tiebreaker** at line 401. |
| **Histograms** | `fs_scans/queries/access_history.py` (~277), `fs_scans/queries/file_size.py` (~113) | Same resolve-then-predicate swap — both call `resolve_path_to_id` and build the same CTE; in scope per the client report (calls #1–3, #9–10). |
| **Consolidate** | `fs_scans/consolidate/consolidator.py` | **Verify only.** Columns auto-COPY (runtime column enumeration); the new PG indexes flow through `add_directory_stats_indexing` in the deferred-index phase. Confirm `NULL` anc values on shallow rows COPY correctly (`\N`). |
| **Tests** | `fs_scans/tests/test_recursive_scope_opt.py` (**new**) | Build a synthetic deep, multi-owner tree (mirror the `populated_session` fixture in `test_fs_scans.py:43-80`). Assert the lineage-predicate path returns **identical rows and ordering** to the recursive-CTE path for owner-summary, group-summary, and `list_directories`; cover single-prefix, multi-prefix, and a `> SCOPE_MAX_DEPTH` fallback case. |

---

## 8. Knobs to finalize on-machine

- **`SCOPE_MAX_DEPTH`** and the **indexed depth band** are tuning knobs. Set them
  against the real depth histogram: on cgd the collection root is ≈ depth 3 and the
  mass sits at depth 11–15, so index a selective band a few levels below each `.db`
  root and skip the root depth itself. Deeper-than-bound scopes accept the slow-path
  fallback (rare per the SAM access pattern — only a few levels below each root
  matter to the UI).

---

## 9. Validation plan

1. **Before:** run the `EXPLAIN ANALYZE` reproducer from
   `FS_SCANS_RECURSIVE_QUERY_OPT.md` §4 on small/medium/large cgd scopes
   (`dir_id` 6 / 3 / 5 / 2); record total ms.
2. **After:** same scopes, same command via the rewritten path; target **< ~1 s**
   on the 14 M scope and **no disk spill** in the plan.
3. **Parity:** the new test (§7) asserts identical owner/group/listing rows and
   ordering (incl. the `dir_id` tiebreaker) vs the recursive version.
4. **Both backends:** `pytest fs_scans/tests/` (SQLite) and a test consolidation
   into a PG schema, then re-run the parity query there.
5. **End-to-end:** reset `pg_stat_statements` on the `-ro` replica, have the UI
   driven, confirm the `WITH RECURSIVE descendants` shape no longer tops the
   ranking (`scripts/cirrus_healthcheck.sh` §9, per-instance, replica-aware).

---

## 10. Non-goals

- CNPG / helm tuning — already done (`#78`, `#79`); not the lever here.
- Histogram fast-path tables (`access_histogram` / `size_histogram`) — already fast
  at whole-collection granularity; this spec restores that speed for *scoped*
  histograms via the same column predicate.
- Raising `work_mem` — measured irrelevant to the dominant 14 M-row walk.
- A closure table (option A) — superseded; same walk-elimination at O(N×depth)
  storage and a new table, vs O(N) columns here.
