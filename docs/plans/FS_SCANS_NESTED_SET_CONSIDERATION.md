# Considered alternative: nested-set (lft/rgt) tree model for fs_scans scoped queries

**Status:** Considered, **held** (not implemented). The shipped design is the
ancestor-at-level (`anc_d{k}`) approach — see
[`implemented/FS_SCANS_ANCESTOR_AT_DEPTH.md`](implemented/FS_SCANS_ANCESTOR_AT_DEPTH.md)
(PR #82). This doc records *why* nested-set was evaluated and deferred, so the option
(and its real costs) don't have to be re-derived later.

**Recommendation: HOLD.** A feasible, performant alternative — but it addresses a
limitation the shipped design has not been shown to hit in practice. Revisit only under
the triggers listed below.

## 1. What it is / what it would buy us

A modified-preorder-traversal ("nested set") encoding stores two integers per row,
`lft`/`rgt`. Any subtree is then `WHERE lft BETWEEN X.lft AND X.rgt` — a single indexed
range, at **any depth**, with no recursion. Versus the shipped `anc_d{k}` design it would:

- **Remove the depth cap entirely** → drop `SCOPE_MAX_DEPTH` and the indexed-band tuning
  (`SCOPE_INDEX_MIN_DEPTH`/`SCOPE_INDEX_MAX_DEPTH`).
- Use **2 `Integer` columns** (`lft`/`rgt`; both bounded by `2·N` — for cgd `2·57.4M =
  114.7M`, well inside signed int32) vs **9** `anc_d*` ints → less storage
  (~0.5 GB vs ~2 GB on cgd).
- Express a scope as one range predicate instead of an `OR` of per-level equalities.

## 2. Feasibility / performance on the largest collection (cgd, 57.4 M dirs)

The natural worry is that the import is parallel/threaded and has no global ordering, so a
nested-set numbering would force a sequential depth-first traversal. **It does not** — there
is a clean, set-based build:

- **Preorder for free from a path sort.** `fs_scans/importers/pass1.py` already materializes
  the full `path → id` map in memory and sorts before assigning `dir_id` (today by *depth*,
  so parents precede children for the `parent_id` lookup). A **path-component sort is also
  parent-before-child AND is preorder**, so `dir_id` becomes the preorder rank = `lft`. This
  is a sort-key change, single-threaded like today's depth sort — *not* a DFS.
- **`rgt` is analytic.** `rgt = lft + 2·subtree_size − 1`, and `subtree_size` is `dir_count_r`,
  already computed bottom-up in `pass2b`. One extra set-based `UPDATE` (≈ the `pass2c` cost,
  ~2 min on cgd) — no recursion.
- The parallel `pass2a` / histogram passes key on `dir_id`/path and are order-independent →
  unaffected.
- fs_scans does **full weekly rebuilds** (no incremental inserts), so nested-set's classic
  fatal flaw — O(N) `lft`/`rgt` shifts on every mutation — is **moot** here.

### The one real correctness wrinkle
Lexicographic preorder is only correct if the path separator sorts before every name
character. Filenames containing bytes `< '/'` (0x2F: space, `!`, `"`, `#`, …) would break a
naive string sort, so the build must sort by **split path components** (or a separator-aware
key). Solvable, but it is the detail to get right.

## 3. Why HOLD (cost vs. benefit against the shipped design)

- **It addresses a non-problem today.** cgd's real depth histogram shows fat subtrees only
  through relative level 7, with a cliff at level 8 (max subtree ~45k, none >100k). The
  shipped band `[2,7]` covers every fat scope; deeper scopes are thin and the recursive
  fallback already returns in ~1 s. The depth cap is **not a practical limitation.**
- **It's a rewrite of code just validated + merged** (populate + query + indexing layers), for
  marginal gain → churn on a working, fast system (1.55 s on cgd's 34 M-row worst case).
- **Grouping is marginally worse.** `anc`'s `(anc_d{k}, owner_uid) INCLUDE(...)` is an
  equality-keyed `GroupAggregate` (no sort). Nested-set's `(lft, owner_uid, …)` is a range
  scan + `HashAggregate` — comparable order, slightly less tidy; would need re-verification at
  ~2 s on the cgd worst case.
- **Cheap to defer.** The build above is low-risk to add later (even alongside `anc_d{k}`), so
  holding does not paint us into a corner.

## 4. Revisit IF
- The depth distribution shifts so fat subtrees appear beyond a reasonable band (the band can
  no longer cover them and the recursive fallback gets slow), **or**
- the band / `SCOPE_MAX_DEPTH` tuning becomes a real maintenance burden, **or**
- a consumer genuinely needs fast *arbitrary-depth* scopes.

In any of those, the path-preorder + analytic-`rgt` build is the recommended route.

## 5. If pursued (sketch, not for now)
1. `core/models.py`: `lft`/`rgt` `Integer` columns on `directory_stats`.
2. `importers/pass1.py`: sort by split-path components (preorder) so `dir_id` = `lft`.
3. New set-based pass after `pass2b`: `rgt = lft + 2·dir_count_r − 1`.
4. `importers/add_table_indexing.py`: covering `(lft, owner_uid, total_size_nr, file_count_nr)`
   (+ group/listing analogues) on PostgreSQL; plain `(lft)` on SQLite.
5. Query layer: replace the `anc_d{k}` predicate with `lft BETWEEN :a AND :b`; keep the
   recursive-CTE fallback only for pre-feature `.db` files.
6. Parity tests mirroring `tests/test_recursive_scope_opt.py`; validate on cgd (build + EXPLAIN).
