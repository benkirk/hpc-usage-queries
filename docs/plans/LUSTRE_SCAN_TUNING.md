# Lustre metadata-scan performance: tuning notes & evaluated alternatives

**Status:** Investigated 2026-06-23 on Derecho / `desc1`. **Conclusion: the existing
`bin/lustre_scan_depth.sh` design is near-optimal for user-space `lfs find`.** The only
worthwhile code change is raising the hardcoded worker count `-P 8 → 32`. Everything
fancier we tried (MDT-index parallelism, a single recursive `lfs find`, subtree-recursive
splitting) measured *slower*. This doc records the data and the *why* so the options — and
their real costs — don't have to be re-derived later.

This concerns only the **scan-file generation** step (`bin/lustre_scan_depth.sh`), i.e. how
we turn a Lustre directory tree into a `.lfs-scan` text file. The downstream
`fs-scans import` pipeline is unaffected.

---

## 1. TL;DR — what to do

In priority order:

1. **Rely on multi-node fan-out (already in place).** Each client *node* has its own pool of
   metadata RPC slots (see §3). Aggregate throughput scales with the number of client nodes,
   not with workers on one node. `fs_scans/PBS/destor/submit_all.sh` already splits input dirs
   across PBS jobs — **this is the real scaling path and it already exists.**
2. **Bump `-P 8 → 32`** in `bin/lustre_scan_depth.sh` to match the single-client RPC ceiling
   (4 MDTs × 8 in-flight = 32). One line, free, ~15–45 % faster per invocation depending on
   shared-MDS load. Surplus workers simply sleep, so there is no downside.
3. **(Privileged, optional) Raise the ceiling itself** via `max_rpcs_in_flight` (§3, §6),
   then raise `-P` to `≈ 4 × max_rpcs_in_flight`. Lifts single-client throughput but loads the
   **shared** MDS — test for collateral impact and coordinate with the filesystem admins.

**Do not** adopt: MDT-index (`lfs find -m`) parallelism, a single serial recursive `lfs find`,
or subtree-recursive splitting — all measured losers (§5). **Keep** the `--printf` format fully
general-purpose, including `p=%LP`; it costs nothing measurable (§5.4).

---

## 2. The only constraints the scan file must satisfy

Understanding what the importer *actually* requires is what frees up (or rules out) generation
strategies. The import pipeline (`fs_scans/importers/`) is **order-independent**:

- Pass 1a parses with `imap_unordered`; Pass 1b **re-sorts all directory paths by depth in
  memory** (`fs_scans/importers/pass1.py:122`) before insert, so the "parent row exists before
  child row" property is *manufactured by the importer*, not required of the file.
- Parent/child is derived from the path string (`rpartition('/')`), never from encounter order.

So the scan file's only real contract is:

1. **Completeness** — every directory appears as an explicit `type=d` line, and every file's
   `dirname` matches some directory line.
2. **Exactly-once per entry** — Pass 2a *sums* size/count per parent without dedup, so a
   duplicated line double-counts. Every path must be emitted exactly once.

Ordering, depth-first traversal, and per-directory grouping are all irrelevant. Any strategy
that produces a complete, duplicate-free union is valid — which is why we were free to try
reordered/split/parallel generators. (The parser, `fs_scans/parsers/lustre.py`, consumes only
`%LF`, `s`, `b`→allocated, `u`, `g`, `type`, `a`, `%p`; it ignores `%LP`, `perm`, `m`, `c`.)

---

## 3. Root cause of the performance ceiling

**The scan is ~100 % MDS-RPC-latency-bound, not client-CPU-bound.** During a run on a 256-logical-core
compute node: load average ≈ 6, CPU **97 % idle**, and most `lfs find` processes in state `S`
(sleeping, blocked on RPC replies). The client has enormous headroom; the limiter is metadata
round-trips to the shared MDS.

**The single-client concurrency wall is `max_rpcs_in_flight`:**

```
$ lctl get_param mdc.desc1-*.max_rpcs_in_flight
mdc.desc1-MDT0000-...max_rpcs_in_flight=8
mdc.desc1-MDT0001-...max_rpcs_in_flight=8
mdc.desc1-MDT0002-...max_rpcs_in_flight=8
mdc.desc1-MDT0003-...max_rpcs_in_flight=8
```

`desc1` has **4 MDTs**, each capped at **8** in-flight metadata RPCs per client → **32** concurrent
RPCs maximum from one client. This *exactly* explains the measured P-scaling knee (§4): beyond
`-P 32`, extra workers cannot get more RPCs onto the wire and simply queue.

Two ways past the wall:
- **More client nodes** — each node has its own independent 8/MDT × 4 = 32-slot pool. Aggregate
  concurrency = `32 × N_nodes`. (Already exploited by `submit_all.sh`.)
- **Raise `max_rpcs_in_flight`** per client (privileged; §6) — lifts the per-node ceiling but adds
  load to the shared MDS.

Because the cap is *per MDT*, work must be spread across all 4 MDTs to use all 32 slots. The
per-directory `lfs find` calls naturally hit whichever MDT holds each dir/file, and `desc1` is
globally balanced (MDT inode use within ~±13 %; `gdex`, `p`, and `CMIP6` are DNE2 striped dirs
with `lmv_stripe_count: 4`), so this happens for free.

---

## 4. Benchmark data

Two size-matched but structurally opposite real subtrees, on a dedicated Derecho compute node:

| Target | Entries | Shape |
|---|---|---|
| `/lustre/desc1/p` | 389,812 | shallow (depth-4 = 326 dirs), ~34 files/dir, large `.nc` files |
| `…/CMIP6/CMIP/EC-Earth-Consortium` | 677,556 | deep (depth-5 = 6,007 dirs), ~25 files/dir, small files |

**All approaches produced identical line counts** (`p`=389,812; EC-Earth=677,556) → every variant
is correct (complete + exactly-once). Timings:

| Approach | `p` P=32 | `p` P=64 | `p` P=128 | EC P=32 | EC P=64 | EC P=128 |
|---|---|---|---|---|---|---|
| **current** (per-dir `find`+xargs) | **64.3** | **63.1** | **62.5** | **140.1** | **139.0** | **138.7** |
| subtree-recursive (depth-4 split) | 118.2 | 121.5 | 118.2 | 181.6 | 179.8 | 178.9 |

Single-client P-scaling knee for the **current** approach (`/lustre/desc1/p`):

```
P=4    110.3s
P=8     73.6s   ← current hardcoded default
P=16    63.6s
P=32    62.6s   ← matches the 32-RPC ceiling
P=64    62.8s
P=256   61.5s
```

Other one-off measurements (`/lustre/desc1/p`, ~390k entries):
- Single **serial** recursive `lfs find`: ~130 s. EC-Earth serial: 484 s.
- **MDT-index** 4-way parallel (`lfs find -m 0..3` concurrent): 156 s (cold) — *slower* than serial.

> **Caveat on absolute numbers.** The MDS is shared across every client on `desc1`, so absolute
> seconds carry noise from other tenants' metadata load. Trust the **back-to-back deltas and curve
> shapes**, not cross-run absolutes. (E.g. an earlier warm/login-node run showed `p` P=32 ≈ 26 s;
> the compute-node cold runs above are ~62 s for the same work — same shape, different background load.)

---

## 5. Approaches evaluated

### 5.1 Current: POSIX `find` enumerates dirs → per-dir `lfs find` (xargs -P) — **KEEP**

```
find $path -depth -type d -readable -print0 \
  | xargs -0 -n 1 -P 8 bash -c 'lfs_cmd "$@"' _
# lfs_cmd: lfs find <dir> --maxdepth 0 --type d   (the dir's own line)
#          lfs find <dir> --maxdepth 1 ! --type d (its non-dir children)
```

**Winner.** Fine-grained, *per-directory* work units keep the maximum number of independent,
short readdir/stat RPCs cycling through the 32-slot in-flight pipe — exactly what a
latency-bound MDS rewards. The per-directory process spawns ("fork storm") are **not** the
bottleneck (the node is 97 % idle); the RPC pipe is. Load-balances naturally because units are
tiny and numerous, and xargs hands the next unit to any free worker.

### 5.2 MDT-index parallelism (`lfs find -m 0..3`) — **REJECTED**

Idea: run one `lfs find --mdt-index i` per MDT concurrently; the union is a correct disjoint
partition (verified: `sum_i count(-m i) == total`, diff=0, even across striped dirs). **But it
ran slower** (156 s vs ~130 s serial). `lfs find -m` filters *output*, not *traversal*: each of
the 4 workers still readdirs the **entire** tree (children of any dir are scattered across all
MDTs, so no worker can skip descending). That is **4× the readdir RPC volume**, which swamps the
benefit of spreading per-inode stats across MDTs. Balance is also imperfect (~2.3× skew even on a
striped dir). The original motivation — a dirs-then-files two-pass for ordering — is moot anyway
because the importer is order-independent (§2).

### 5.3 Single recursive `lfs find` over the whole subtree — **REJECTED**

One `lfs find <root> --printf …` emits every entry once in one process — clean and minimal, but
**single-threaded**. At ~130 s (`p`) / 484 s (EC-Earth) it is ~2× / ~3× slower than the parallel
current approach. With a 32-slot RPC pipe available, a serial walk that keeps ~1 RPC in flight
leaves almost all the concurrency unused.

### 5.4 Subtree-recursive (split at depth N, recursive `lfs find` per unit) — **REJECTED**

Enumerate dirs at a split depth, run one *recursive* `lfs find` per subtree W-way parallel
(handling the shallow remainder per-dir). Intended to eliminate the double-readdir and the
per-dir spawns. **~2× slower on both regimes** (§4). Two reasons: (a) each worker serializes a
**depth-first descent** of its subtree — readdir a dir, *then* descend, each step a round-trip,
keeping far fewer RPCs in flight than many independent per-dir finds; (b) coarse units (hundreds,
not tens of thousands) load-balance poorly — the largest subtree's serial walk becomes the long pole.

### 5.5 `--printf` field trimming (drop `p=%LP`) — **NO MEASURABLE EFFECT**

`%LP` (OST pool) is the one field that could plausibly force a layout fetch, and the parser
ignores it. A/B (with vs without, alternating runs): 128.0/132.6 s vs 133.3/136.1 s — the
difference is buried in monotonic background drift. Keep the format general-purpose.

---

## 6. The privileged lever: `max_rpcs_in_flight`

Production scans run with elevated privilege, so the per-client ceiling can be lifted:

```bash
# inspect (read-only, works as any user):
lctl get_param mdc.desc1-*.max_rpcs_in_flight        # → 8 each (×4 MDTs = 32)

# raise (privileged; per-MDT). Max is ~256. Example doubling to 16/MDT = 64 total:
lctl set_param mdc.desc1-*.max_rpcs_in_flight=16
```

Then set the scan's `-P ≈ 4 × max_rpcs_in_flight` (e.g. 16/MDT → `-P 64`).

Then set the scan's `-P ≈ 4 × max_rpcs_in_flight` — **but see the measured result below before bothering.**

**Trade-off / cautions:**
- This raises load on the **shared** MDS; it can degrade interactive metadata performance for
  every other user of `desc1`. Coordinate with the filesystem admins; prefer running during
  low-utilization windows.
- It is a **client tunable**, not persistent — it resets on remount. A privileged scan job would
  set it at job start (and ideally restore it at exit).

### 6.1 Measured result: 8 → 64 per MDT (2026-06-25)

An admin set `max_rpcs_in_flight=64` per MDT (256 total in-flight, an 8× bump) on a client; we
re-ran the identical P-sweep on both targets, idle nodes both times:

| -P | `p` rpc=8 | `p` rpc=64 | EC rpc=8 | EC rpc=64 |
|---|---|---|---|---|
| 8 | 82.6 | 72.1 | 172.7 | 153.1 |
| 16 | 64.1 | 57.3 | 140.1 | 123.9 |
| 32 | 62.9 | 56.5 | 139.9 | 122.6 |
| 128 | 62.2 | 54.5 | 139.5 | 122.9 |
| 256 | 61.7 | 53.7 | 139.1 | 122.9 |
| 512 | — | 53.9 | — | 123.1 |

- **A uniform ~10–13 % speedup at every `-P`** (including P=8) — the larger per-MDT budget lets each
  worker pipeline more RPCs (statahead), shifting the whole curve down.
- **The knee did not move** — still ~P=16, dead-flat to P=512 despite the 8× budget. We are no longer
  client-RPC-limited, yet extra workers still do nothing.
- **Root cause of the residual ceiling = shared-MDS service rate, confirmed directly.** The serial
  outer `find` is trivial (0.6 s / 1.6 s, ruled out as the limiter). A live snapshot during a P=64 run
  showed the node **97–98 % idle**, load ~2–3 on 256 cores, and **~20 of 22 `lfs` workers in state `S`**
  (sleeping, blocked on RPC replies) — aggregate CPU under one core. The client cannot extract more
  metadata throughput from the shared MDS regardless of `-P` or RPC budget.

**Takeaway:** the bump is a real but **modest, optional ~10–13 %** win. It does *not* change the optimal
`-P` (keep 32) and does *not* unlock higher single-client scaling. Single-client tuning is now exhausted;
genuine throughput gains come only from **more client nodes** (§1 lever #1), which `submit_all.sh` already
provides. Adopt the bump only if the ~10 % is worth the privileged setup and the shared-MDS load — note
this workload self-limits to ~20 effective concurrent ops, so the collateral impact is bounded in practice.

Spreading work across multiple **nodes** (lever #1) is generally the safer scaling path because
each node contributes an independent RPC pool without any single client hammering one MDT harder.

---

## 7. Non-issues (investigated, dismissed)

- **Symlinks not followed.** `CMIP6/CMIP/NCAR` (and similar) are symlinks into GPFS, which is
  covered by a *separate* scan. `find`/`lfs find` not following them is correct, not a gap.
- **`lfs find --printf` EPERM on other users' data.** Observed as `benkirk` on `ejn`-owned mirror
  subtrees (the format triggers an upfront attribute/FID fetch denied by ownership + the
  `nouser_fid2path` mount option). This is a *benchmarking artifact only* — production scans run
  privileged and can read everything. Note the current script's `2>/dev/null` would silently drop
  any genuinely unreadable subtree, which is fine under privilege but worth remembering if the scan
  is ever run unprivileged.

---

## 8. Reproducing / extending

Environment facts (`desc1`, 2026-06-23): `lfs 2.15.7.1_cray`; 4 MDTs, globally balanced;
`max_rpcs_in_flight=8`/MDT; mount options include `nouser_fid2path`. `lctl` at `/usr/sbin/lctl`.
Note `nproc` may report 1 if `OMP_NUM_THREADS=1` — check `nproc --all` / `taskset -cp $$` for the
true core/affinity count.

Benchmark recipe used throughout:
- Pick size-matched subtrees of **different shape** (shallow/big-files vs deep/small-files);
  `lfs find <dir> | wc -l` sizes a candidate, `find <dir> -mindepth d -maxdepth d -type d | wc -l`
  profiles its depth for choosing a split.
- Time with `date +%s.%N`; **verify correctness** by confirming `grep -vc '^#'` line counts match
  across every variant (and the serial baseline).
- Run on a **dedicated compute node** to remove client-side contention; remember the MDS is still
  shared, so compare back-to-back deltas, not absolutes.

See also the cross-session summary in the agent memory note *lustre-scan-perf-findings*.
