# fs_scans slow call targets — for peer (hpc-usage-queries) optimization

**Captured:** 2026-06-21, live nwc1 pod against `csg-postgres-ro` (campaign DB),
build `sha-83ae75a`. Serial in-pod harness through the real SAM service layer
(`webapp/disk_scans/service._scoped` → `FsScanQueries.<method>`), one cold call
each (no cache), instrumented with SQLAlchemy cursor events to split wall vs DB
time and capture the single slowest SQL statement per call.

**Goal:** hand the peer exact `FsScanQueries` call targets + arguments + the
dominating SQL, so the on-the-fly path can be optimized.

---

## TL;DR — one query shape dominates everything

Every slow call spends ~**all** its DB time in **one** statement: a
**`WITH RECURSIVE descendants` CTE** that seeds from the scope's ancestor
`dir_id`s and walks the whole subtree via `directories.parent_id`, then joins
`directory_stats`. It is **identical** across `access_history`,
`file_size_histogram`, `list_directories`, `owner_summary`, `group_summary` —
they differ only in the trailing SELECT/aggregation.

```sql
WITH RECURSIVE ancestors AS (
    SELECT dir_id FROM directories WHERE dir_id IN (%(ancestor_id_0)s, %(ancestor_id_1)s)
), descendants AS (
    SELECT dir_id FROM ancestors
    UNION ALL
    SELECT d.dir_id FROM directories d JOIN descendants p ON d.parent_id = p.dir_id
)
SELECT ... FROM directories d JOIN directory_stats s USING (dir_id)
         JOIN descendants USING (dir_id) WHERE ...
```

Cost **scales with subtree size, not result size**: 10-prefix NRAL0002 (≈ whole
RAL+ncar, ~745M files) = **60–63s**; single-prefix `/ral/hap` = **12s**; a
whole-collection fast-path scope = **0.0s** (different, precomputed query — see
below). Post-walk filters (`owner_uid`, `accessed_before`, `leaves_only`) do
**not** materially reduce it (the walk runs first; the filter trims after).

---

## Slow calls, ranked (exact targets)

All on resource **Campaign_Store**. `FsScanQueries(filesystems=<collections>)`
then the method below. Times are wall / DB(s) / #queries.

| # | Call | scope | wall | db | nq | rows |
|---|---|---|---|---|---|---|
| 1 | `access_history(path_prefixes=P, owner_uid=None)` | NRAL0002 | 76.3 | 63.6 | 12 | 222 |
| 2 | `access_history(path_prefixes=P, owner_uid=7752)` | NRAL0002 | 76.2 | 62.8 | 12 | 86 |
| 3 | `file_size_histogram(path_prefixes=P, owner_uid=None)` | NRAL0002 | 71.1 | 63.7 | 12 | 253 |
| 4 | `list_directories(path_prefixes=P, sort_by='size', limit=50)` | NRAL0002 | 61.4 | 61.4 | 6 | 50 |
| 5 | `owner_summary(path_prefixes=P, limit=50)` | NRAL0002 | 61.0 | 61.0 | 5 | 50 |
| 6 | `group_summary(path_prefixes=P, limit=50)` | NRAL0002 | 60.7 | 60.7 | 5 | 27 |
| 7 | `list_directories(path_prefixes=P, leaves_only=True, limit=50)` | NRAL0002 | 59.7 | 59.7 | 6 | 50 |
| 8 | `list_directories(path_prefixes=P, accessed_before='2024-01-01', limit=50)` | NRAL0002 | 40.3 | 40.3 | 6 | 50 |
| 9 | `access_history(path_prefixes=['/gpfs/csfs1/ral/hap'])` | P48500028 | 17.5 | 12.8 | 5 | 121 |
| 10 | `file_size_histogram(path_prefixes=['/gpfs/csfs1/ral/hap'])` | P48500028 | 15.4 | 12.2 | 5 | 152 |

**`P` (NRAL0002 path_prefixes, exact)** — `filesystems=['ncar','ral']`:
```
['/gpfs/csfs1/ncar/USGS_Water', '/gpfs/csfs1/ncar/fedata',
 '/gpfs/csfs1/ral', '/gpfs/csfs1/ral/aap', '/gpfs/csfs1/ral/hap',
 '/gpfs/csfs1/ral/jntp', '/gpfs/csfs1/ral/nral0003', '/gpfs/csfs1/ral/nsap',
 '/gpfs/csfs1/ral/risc', '/gpfs/csfs1/ral/wsap']
```
The seed is only **2** ancestor `dir_id`s (`ancestor_id_0/1`), so the facade
already collapses these 10 prefixes to 2 subtree roots before the walk — i.e.
the cost is two big recursive descendant walks, not ten.

Note `P` contains the collection root `/gpfs/csfs1/ral` **redundantly alongside
its own children** (`/ral/aap`, `/ral/hap`, …). Since `/gpfs/csfs1/ral` subsumes
all `/gpfs/csfs1/ral/*` siblings, this scope is effectively "(almost) the whole
`ral` collection" — a candidate to route to the whole-collection fast path (see
Leads). `P48500028` (`/ral/hap` alone) shows the per-subtree cost in isolation.

**The `wall − db` gap** on the histograms (#1 76.3 vs db 63.6; #3 71.1 vs 63.7)
is ~8–13s of **Python** (per-user bucket assembly / username resolution); the
list/summary calls are ~100% DB. Worth a glance but secondary to the 60s SQL.

---

## Fast-path contrast (what "good" looks like — already precomputed)

| Call | scope | wall | db | slowest SQL |
|---|---|---|---|---|
| `access_history(path_prefixes=mmm6)` | NMMM0003 | **0.0** | 0.0 | `SELECT bucket_index, owner_uid, file_count, total_size FROM access_histogram ORDER BY ...` |
| `list_directories(path_prefixes=None, sort_by='size', limit=50)` | resource-wide | **0.1** | 0.1 | `SELECT ... s.total_size_r ... ORDER BY s.total_size_r DESC LIMIT %(limit)s` |

Two things the fast path does that the slow path doesn't:
1. **`list_directories(path_prefixes=None)`** reads precomputed **recursive
   rollup columns** `directory_stats.total_size_r / file_count_r / dir_count_r /
   max_atime_r` directly — **no recursion**. The `_r` (recursive) columns already
   hold the subtree totals. The slow path instead re-derives subtree totals by
   walking `descendants` and summing the **`_nr`** (non-recursive, this-dir-only)
   columns on the fly.
2. **`access_history` fast path** reads a precomputed **`access_histogram`** table
   (per `bucket_index, owner_uid`) — no recursion, no per-file work. The slow path
   recomputes the access-time histogram from the descendant walk.

---

## Leads for the peer (hypotheses, not prescriptions — you have the context)

1. **`list_directories` / `owner_summary` / `group_summary` on a directory scope
   could read the precomputed `directory_stats._r` rollup of the scope root(s)
   instead of walking `descendants` and summing `_nr`.** The `_r` columns already
   exist and the resource-wide fast path already uses `total_size_r`. For
   `owner_summary`/`group_summary` this needs per-owner/per-group rollups at the
   scope root (today only the totals look precomputed); for `list_directories` the
   subtree listing still needs descendant rows, but ordering/sizing could come
   from `_r` and the walk could be **depth-bounded** (the UI shows 50 rows).

2. **`access_history` / `file_size_histogram` for sub-paths have no precomputed
   table** — the `access_histogram` fast-path table appears to exist only at
   whole-collection granularity. A precomputed histogram keyed by
   `(scope_root_dir_id, bucket)` would turn these 60s calls into the 0.0s NMMM
   case. (Distribution can't collapse to a single `_r` scalar, so it needs its own
   rollup table, unlike lead 1.)

3. **The recursive `parent_id` walk itself is the hot loop.** If `directories`
   carried a materialized path / `ltree`, a subtree query becomes one indexed
   range scan (`WHERE path <@ '/gpfs/csfs1/ral'`) instead of `WITH RECURSIVE`.
   This helps the cases that genuinely need descendant rows (list_directories,
   leaves-only explorer) where a precomputed scalar can't substitute.

4. **Redundant-root collapse → fast path.** NRAL0002's prefix list includes the
   `ral` collection root alongside its children. If the scope-resolver recognized
   "root present ⇒ drop the children ⇒ whole-collection fast path for `ral`", this
   specific (common, lab-parent) shape would drop from ~60s toward the NMMM 0.0s
   case without any new precompute. Worth checking whether the existing
   per-collection fast-path detection is defeated by the mixed root+children list.

5. **Filters are applied post-walk.** `accessed_before` (40s) was only modestly
   cheaper than unfiltered (60s) and `owner_uid` not at all — the filter doesn't
   prune the recursive walk. If a filter could push into the walk / precompute,
   the per-user drill (#2) and access-time explorer (#8) would benefit.

---

## How to reproduce / re-baseline

Serial in-pod harness (no auth, clean timing): see
`docs/plans/K8S_HAMMER_HANDOFF.md` §2 for the SQLAlchemy-event timing pattern;
this capture extended it to wrap each `FsScanQueries.<method>` and record the
slowest statement. Scopes: NRAL0002 (slow, mixed root+children), P48500028
(`/ral/hap`, single subtree), NMMM0003 + resource-wide (fast-path baselines).
Re-run after a plugin change to confirm the 60s calls dropped.
