"""Query interface for common HPC job history queries.

This module provides a Python API for common queries against the job history
database. It wraps SQLAlchemy queries with a convenient interface for:
- Finding jobs by user, account, or queue
- Generating usage summaries and statistics
- Filtering by date ranges and status
"""

import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple, Sequence, Union
from zoneinfo import ZoneInfo

from sqlalchemy import func, and_, or_, false, distinct
from sqlalchemy.orm import Session

from ..database import Job, DailySummary, JobCharge, JobQoS
from ..database.models import User, Account, Queue
from ..database.config import JobHistoryConfig
from ..columns import COLUMNS, DEFAULT_COLUMNS, project_row
from .builders import glob_match_clause

logger = logging.getLogger(__name__)


from sqlalchemy import case


#: Facet dimension -> (GROUP BY column, lookup model or None, name column).
#:
#: Grouping goes through the integer FK on purpose. The text-valued
#: ``Job.user`` / ``account`` / ``queue`` / ``qos`` hybrids compile to a
#: correlated scalar subquery (see LookupMixin in database/models.py) that
#: SQLAlchemy emits *twice* — select list and GROUP BY — and PostgreSQL
#: re-evaluates per scanned row with no memoization. Measured 10x slower on a
#: 304k-row slice, and it degrades the group key from int4 to collation-aware
#: text. The FK columns are indexed; names are resolved after aggregation.
_FACET_SPECS: Dict[str, Tuple[Any, Any, Any]] = {
    'queue':       (Job.queue_id,   Queue,   Queue.queue_name),
    'qos':         (Job.qos_id,     JobQoS,  JobQoS.name),
    'exit_status': (Job.status,     None,    None),
    'user':        (Job.user_id,    User,    User.username),
    'account':     (Job.account_id, Account, Account.account_name),
}

#: Cheap dimensions a filter bar needs. ``user`` / ``account`` are opt-in — not
#: because grouping them is expensive (cardinality measured free) but because
#: self-excluding a *selective* filter can flip the query plan.
DEFAULT_FACETS = ('queue', 'qos', 'exit_status')

#: Dimensions whose own filter is NEVER dropped under ``self_exclude``.
#: ``account`` is a security scope in every real caller — SAM pins it for
#: authorization — so self-excluding it would emit counts for projects the
#: requester cannot see. Faceting ``account`` therefore means "which projcodes
#: within my scope", which is what a project-tree drill-down wants.
_FACET_SCOPE_DIMS = frozenset({'account'})

#: Above this many distinct ids, read the whole lookup table (a few thousand
#: rows) instead of building an IN-list — cheaper, and it keeps the statement
#: cache from filling with one plan per distinct id-count.
_LOOKUP_FETCH_ALL_THRESHOLD = 500


def _facet_rows(counts: Dict[Any, int], limit: Optional[int]) -> List[Dict[str, Any]]:
    """Sort a ``{value: count}`` map into the facet row contract.

    Count desc, then value asc so the order is stable across calls with equal
    counts. ``None`` (NULL FK) sorts last within its count group.
    """
    ordered = sorted(
        counts.items(), key=lambda kv: (-kv[1], kv[0] is None, str(kv[0]))
    )
    if limit is not None:
        ordered = ordered[:limit]
    return [{"value": value, "count": count} for value, count in ordered]


#: 1 GB = 1024^3 bytes. Mirrors ``sync/charging.BYTES_PER_GB`` — redefined
#: here because importing the sync package would pull the PBS/Slurm parsers
#: into the queries import path.
_GIB = 1024 ** 3

#: CASE label emitted for rows whose histogram column is NULL. Routed to the
#: response's top-level ``null_count`` by the fold, never surfaced as a
#: bucket label.
_NULL_BUCKET = "__null__"


def _bucket_case(field, buckets):
    """CASE labelling *field* into ``(label, lo, hi)`` buckets; NULL first.

    The ascending ``field <= hi`` ladder (rather than per-band BETWEENs)
    means every non-NULL value lands in exactly one bucket, with no gaps for
    a value to fall through the way ``_build_range_case``'s band arithmetic
    allows. The ``lo`` values are reporting metadata (and drill-down filter
    bounds), not SQL.

    That makes the ladder *total* but not automatically *faithful*, and the
    bucket table owns the difference. Only ``hi`` is tested, so a value
    below the first band's ``lo`` is still claimed by that band's arm, and a
    value above the last band's ``hi`` falls to ``else_`` — in both cases
    labelled with a band whose advertised ``lo``/``hi`` would exclude it,
    breaking the histogram's bar↔``jobs_search`` round-trip (a bar counting
    rows the replayed filters don't return). So every table must be
    *closed*: the first band reaches the column's domain floor (``lo=0``, or
    ``lo=None`` for a signed dimension like ``memory_wasted``) and the last
    band is open-ended (``hi=None``). Pinned by
    ``test_bucket_tables_are_closed_and_contiguous``.
    """
    whens = [(field.is_(None), _NULL_BUCKET)]
    whens += [(field <= hi, label) for label, _lo, hi in buckets if hi is not None]
    return case(*whens, else_=buckets[-1][0]).label("bucket_label")


#: Period granularities accepted by :meth:`JobQueries.jobs_timeseries`.
#: Deliberately NOT ``PeriodGrouper``'s vocabulary — see
#: :func:`_period_bands` for why that helper cannot be reused here.
_TIMESERIES_PERIODS = ("day", "week", "month")

#: Hard cap on the period ladder's width, for the ``jobs`` **scan** path.
#: :func:`_period_case` emits one WHEN arm per band and PostgreSQL evaluates
#: the ladder per scanned row, so band count is a real cost knob — but it does
#: not bite where the first cut of this method assumed. Measured on PG 18 /
#: casper_jobs (21.0M jobs), periods **interleaved** inside one loop and the
#: window held fixed so ladder width is the only variable:
#:
#:   180d window (4.3M rows):    7 bands 6735ms | 26 bands 6848ms (1.02x)
#:                             180 bands 7390ms (1.10x)
#:   730d window (16.6M rows):  25 bands 8909ms | 105 bands 9505ms (1.07x)
#:                             730 bands 14699ms (1.65x)
#:
#: So ~10 % at 180 bands and ~65 % at 730 — the O(bands/2) argument is sound,
#: it just does not show until the ladder is several hundred arms wide. 400
#: sits below that knee. Callers pick a coarser *period* rather than raising
#: it. (An earlier "+54 % at 180 bands" figure was a measurement artifact:
#: the periods were timed sequentially, so cache warming rode along with band
#: count. A PostgreSQL ``date_trunc`` fast path was also tried and measured at
#: parity with the ladder, 0.86-1.11x — it is not the escape hatch.)
_MAX_TIMESERIES_BANDS = 400

#: The same cap for the ``daily_summary`` fast path, which has no ladder at
#: all — it groups the pre-binned ``date`` column, so band count costs nothing
#: (730 daily bands: 21 ms, against 14699 ms for the scan path). This cap is
#: purely a JSON-payload backstop; ~1200 is over three years of daily bands.
_MAX_SUMMARY_BANDS = 1200

#: Filters :class:`~job_history.database.models.DailySummary` can express.
#: Its unique key is ``(date, user_id, account_id, queue_id)``, so those
#: dimensions — and only those — survive the rollup. Anything else is a
#: per-job attribute that was aggregated away and forces the ``jobs`` scan.
#: ``qos`` is the subtle one: a qos *filter* cannot be served, but the
#: qos-weighted *metrics* can, because ``sync.summary`` folds ``qos_factor``
#: into the stored ``*_charges`` with the same ``hours x factor`` formula
#: :func:`_charge_expr` uses.
_SUMMARY_SERVICEABLE_FILTERS = frozenset({
    "start", "end", "user", "account", "queue", "ignore_case",
})


def _site_midnight_utc(day: date) -> datetime:
    """Naive-UTC instant at which site-local *day* begins.

    The same conversion :meth:`JobQueries._apply_date_filter` applies to its
    ``start``/``end`` bounds, factored out so the period bands and the window
    filter cannot drift apart. DST-exact because ``zoneinfo`` does the
    arithmetic — a spring-forward day is 23h wide and a fall-back day 25h,
    which no fixed-offset shift can reproduce.
    """
    site_tz = ZoneInfo(JobHistoryConfig.SITE_TIMEZONE)
    return (
        datetime.combine(day, time.min)
        .replace(tzinfo=site_tz)
        .astimezone(timezone.utc)
        .replace(tzinfo=None)
    )


def _period_first_days(start: date, end: date, period: str) -> List[date]:
    """Site-local first-day of every *period* band touching ``[start, end]``."""
    if period == "day":
        return [start + timedelta(days=i) for i in range((end - start).days + 1)]
    if period == "week":
        # Snap back to Monday so bands are whole ISO weeks: two windows over
        # overlapping spans then share band boundaries and stay comparable.
        out, cur = [], start - timedelta(days=start.weekday())
        while cur <= end:
            out.append(cur)
            cur += timedelta(days=7)
        return out
    out, cur = [], start.replace(day=1)
    while cur <= end:
        out.append(cur)
        # Jump into the next month without calendar arithmetic edge cases.
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
    return out


def _period_bands(start: date, end: date, period: str) -> List[Dict[str, Any]]:
    """Zero-filled band table for ``[start, end]`` at *period* granularity.

    Returns ``[{label, start, end, hi_utc}, ...]`` in chronological order,
    where ``start``/``end`` are **site-local** inclusive calendar dates (the
    band's replay bounds — ``jobs_search(start=…, end=…)`` returns exactly
    that band's jobs) and ``hi_utc`` is the naive-UTC **exclusive** upper
    bound used to build the SQL ladder.

    Bands are clipped to the window: a week band snapped back to Monday
    reports the window's start, not the Monday, so replaying it cannot pull
    in jobs from before the window and break the band↔``jobs_search``
    round-trip.

    **Why not** :class:`~job_history.queries.builders.PeriodGrouper`: it
    formats the raw column (``to_char(job.end, 'YYYY-MM-DD')`` /
    ``strftime``) with no timezone conversion, so it buckets by **UTC**
    calendar day while :meth:`JobQueries._apply_date_filter` bounds the
    window by **site-local** days. Mixing the two offsets every band from
    its own window, makes the first and last bands partial, and disagrees
    with ``DailySummary`` (which bins site-local). Generating the boundaries
    in Python keeps one convention, works identically on SQLite and
    PostgreSQL, and gets ``week`` for free — a granularity ``PeriodGrouper``
    does not support at all.
    """
    if start > end:
        # Inverted window covers nothing. Guarded here rather than in the
        # caller because only ``day`` falls out empty on its own: ``week``
        # and ``month`` snap their first day BACKWARDS, which can land at or
        # before *end* and emit a single band whose clipped start > end.
        return []
    bands = []
    firsts = _period_first_days(start, end, period)
    for i, first in enumerate(firsts):
        nxt = firsts[i + 1] if i + 1 < len(firsts) else None
        last = (nxt - timedelta(days=1)) if nxt is not None else end
        if period == "day":
            label = first.isoformat()
        elif period == "week":
            iso = first.isocalendar()
            label = f"{iso[0]}-W{iso[1]:02d}"
        else:
            label = first.strftime("%Y-%m")
        bands.append({
            "label": label,
            # Clipped to the window — see the round-trip note above.
            "start": max(first, start),
            "end": min(last, end),
            "hi_utc": _site_midnight_utc(min(last, end) + timedelta(days=1)),
        })
    return bands


def _period_case(field, bands: List[Dict[str, Any]]):
    """CASE labelling *field* into contiguous half-open period *bands*.

    Mirrors :func:`_bucket_case`'s totality argument with the half-open
    ``<`` comparison :meth:`JobQueries._apply_date_filter` uses, rather than
    ``<= hi``: timestamps carry sub-second precision, so an inclusive upper
    bound would need an epsilon and would not tile the axis exactly.

    Totality holds because the caller has already filtered the query to
    ``[bands[0] window start, bands[-1].hi_utc)``: every non-NULL row is at
    or past the first band's floor, and anything beyond the penultimate
    band's bound belongs to the last band, which is the ``else_`` arm.
    """
    whens = [(field.is_(None), _NULL_BUCKET)]
    whens += [(field < b["hi_utc"], b["label"]) for b in bands[:-1]]
    return case(*whens, else_=bands[-1]["label"]).label("period_label")


def _charge_expr(hours_col):
    """SUM-able charge expression: raw hours x the job's QoS factor.

    The one formula for "charges" in this package, mirroring
    ``columns._compute_charge`` (the per-row path) and ``sync.summary``'s
    daily rollup, so all three surfaces cannot drift.

    ``qos_factor`` is ``NOT NULL`` on ``job_charges``, so the COALESCE is
    defensive only — it covers the outer-join miss (a job with no charge
    row), where ``hours_col`` is NULL too and the product drops out of the
    SUM regardless.
    """
    return hours_col * func.coalesce(JobCharge.qos_factor, 1.0)


#: The metric vector every aggregate carries, in fold order (count first).
#: ``*_charges`` reuse the plugin's established key names (``columns.py``,
#: ``daily_summary_report``, ``DailySummary``) so a consumer keeps one map.
_METRIC_KEYS = ("job_count", "cpu_hours", "gpu_hours",
                "cpu_charges", "gpu_charges")


def _metric_agg_cols() -> List[Any]:
    """The five aggregate select columns, in ``_METRIC_KEYS`` order.

    Charges add no join and no scan: ``qos_factor`` rides the same
    ``job_charges`` row already read for the hour sums.
    """
    return [
        func.count(Job.id),
        func.sum(JobCharge.cpu_hours),
        func.sum(JobCharge.gpu_hours),
        func.sum(_charge_expr(JobCharge.cpu_hours)),
        func.sum(_charge_expr(JobCharge.gpu_hours)),
    ]


def _zero_metrics() -> List[float]:
    """A fresh mutable accumulator matching ``_METRIC_KEYS``."""
    return [0, 0.0, 0.0, 0.0, 0.0]


def _accumulate(acc: List[float], count, cpu, gpu, cpu_chg, gpu_chg) -> None:
    """Fold one aggregate row into *acc*. SUM over an all-NULL charge group
    is NULL, hence the ``or 0.0``.

    ``count`` gets the same treatment: it is ``COUNT()`` on the ``jobs`` scan
    path, which is never NULL, but ``SUM(job_count)`` on the ``daily_summary``
    fast path, which can be.
    """
    acc[0] += int(count or 0)
    acc[1] += float(cpu or 0.0)
    acc[2] += float(gpu or 0.0)
    acc[3] += float(cpu_chg or 0.0)
    acc[4] += float(gpu_chg or 0.0)


def _metrics_dict(acc: Sequence) -> Dict[str, Any]:
    """``_METRIC_KEYS`` -> value, with ``job_count`` as an int."""
    return {
        "job_count":   int(acc[0]),
        "cpu_hours":   float(acc[1]),
        "gpu_hours":   float(acc[2]),
        "cpu_charges": float(acc[3]),
        "gpu_charges": float(acc[4]),
    }


#: Ranking metrics accepted by ``jobs_usage_by(sort_by=...)``,
#: ``jobs_histogram(owners_sort_by=...)`` and
#: ``jobs_timeseries(owners_sort_by=...)``. ``hours`` is the historical
#: default: combined ``cpu_hours + gpu_hours``; ``charges`` is its
#: QoS-weighted counterpart, combined ``cpu_charges + gpu_charges``.
_USAGE_SORT_KEYS = ("hours", "cpu_hours", "gpu_hours", "job_count",
                    "charges", "cpu_charges", "gpu_charges")


def _check_usage_sort_key(sort_by: str, param: str) -> None:
    """Validate a ``_USAGE_SORT_KEYS`` selection, naming the offending kwarg."""
    if sort_by not in _USAGE_SORT_KEYS:
        valid = ", ".join(_USAGE_SORT_KEYS)
        raise ValueError(f"Unknown {param}: {sort_by!r}. Valid keys: {valid}")


def _usage_rank(sort_by: str, metrics: Dict[str, Any]) -> float:
    """The ranking metric's value, so every call site agrees on ``hours``.

    Shared by ``jobs_usage_by``'s row ranking and the per-owner ranking in
    ``jobs_histogram`` / ``jobs_timeseries`` — the same vocabulary must mean
    the same thing everywhere, or a dashboard's pie and its stacked bars
    disagree about who the top consumers are.

    Args:
        sort_by: a ``_USAGE_SORT_KEYS`` member.
        metrics: a ``_METRIC_KEYS``-shaped mapping.
    """
    if sort_by == "hours":
        return metrics["cpu_hours"] + metrics["gpu_hours"]
    if sort_by == "charges":
        return metrics["cpu_charges"] + metrics["gpu_charges"]
    return metrics[sort_by]


def _sort_expression(sort_by: str):
    """Map a ``COLUMNS`` key to a SQLAlchemy expression suitable for ORDER BY.

    Direct ``job.<attr>`` / ``charge.<attr>`` keys resolve to the matching
    ``Job`` / ``JobCharge`` column. Computed ``*_charges`` keys sort on the
    underlying ``hours × COALESCE(qos_factor, 1)`` product, matching the
    formula in :func:`project_row`.

    Lookup-backed keys (``user``/``account``/``queue``/``qos``) must NOT
    resolve here: ``getattr(Job, 'user')`` returns the text hybrid, whose
    SQL side is a correlated scalar subquery re-evaluated per scanned row
    (measured 10x slower — see :data:`_FACET_SPECS`).
    :meth:`JobQueries.jobs_search` routes them through
    :data:`_LOOKUP_DIMS` instead.
    """
    spec = COLUMNS[sort_by]
    kind, attr = spec["source"].split(".", 1)
    if kind == "job":
        return getattr(Job, attr)
    if kind == "charge":
        return getattr(JobCharge, attr)
    hours_attr = {
        "cpu_charges":    "cpu_hours",
        "gpu_charges":    "gpu_hours",
        "memory_charges": "memory_hours",
    }.get(attr)
    if hours_attr is None:
        raise ValueError(f"Cannot sort by computed column {sort_by!r}")
    return getattr(JobCharge, hours_attr) * func.coalesce(JobCharge.qos_factor, 1.0)


#: The four query dimensions whose ``COLUMNS`` source is a text hybrid over
#: a lookup FK: ``(lookup model, Job FK column, name column)``.
#:
#: Every one of those hybrids compiles to a correlated scalar subquery
#: re-evaluated per scanned row, so neither ORDER BY nor WHERE may touch
#: them. ``jobs_search`` joins the lookup once and orders by its name column;
#: ``_apply_jobs_search_filters`` resolves the name to an id once and filters
#: the integer FK. Measured on derecho over a one-month window:
#: ``jobs_count(user=…)`` 406 ms → 16 ms, ``jobs_count(account=[3])``
#: 274 ms → 48 ms; ``EXPLAIN`` swaps ``Seq Scan … Filter: ((SubPlan 1) = …)``
#: for a bitmap index scan on ``ix_jobs_user_account``.
_LOOKUP_DIMS = {
    "user":    (User,    Job.user_id,    User.username),
    "account": (Account, Job.account_id, Account.account_name),
    "queue":   (Queue,   Job.queue_id,   Queue.queue_name),
    "qos":     (JobQoS,  Job.qos_id,     JobQoS.name),
}


def _account_to_facility(account):
    """Map an NCAR project/account code to its facility bucket.

    Buckets: UNIV, WNA, CSL, CISL, NCAR (default).
    Returns 'NCAR' for None / empty / unmatched.
    """
    if not account:
        return "NCAR"
    if account.startswith("U") or account.startswith("P35"):
        return "UNIV"
    if account.startswith("W"):
        return "WNA"
    if account.startswith("C") or account.startswith("P933"):
        return "CSL"
    if account.startswith("S"):
        return "CISL"
    return "NCAR"


class QueryConfig:
    """Centralized configuration for query patterns and constants."""

    # Machine-specific queue definitions
    MACHINE_QUEUES = {
        'derecho': {
            'cpu': ['cpu', 'cpudev'],
            'gpu': ['gpu', 'gpudev', 'pgpu']
        },
        'casper': {
            'cpu': ['htc', 'gdex', 'largemem', 'vis', 'rda'],
            'gpu': ['nvgpu', 'gpgpu', 'a100', 'h100', 'l40', 'amdgpu']
        }
    }

    # Legacy attributes for backward compatibility (default to Derecho)
    CPU_QUEUES = MACHINE_QUEUES['derecho']['cpu']
    GPU_QUEUES = MACHINE_QUEUES['derecho']['gpu']

    @staticmethod
    def get_cpu_queues(machine: str) -> list:
        """Get CPU queue names for a specific machine.

        Args:
            machine: Machine name ('casper' or 'derecho')

        Returns:
            List of CPU queue names for the machine
        """
        return QueryConfig.MACHINE_QUEUES.get(machine.lower(), {}).get('cpu', QueryConfig.CPU_QUEUES)

    @staticmethod
    def get_gpu_queues(machine: str) -> list:
        """Get GPU queue names for a specific machine.

        Args:
            machine: Machine name ('casper' or 'derecho')

        Returns:
            List of GPU queue names for the machine
        """
        return QueryConfig.MACHINE_QUEUES.get(machine.lower(), {}).get('gpu', QueryConfig.GPU_QUEUES)

    @staticmethod
    def _make_ranges(boundaries: List[int]) -> List[Tuple[int, int]]:
        """Generate range tuples from boundary list.

        The first two boundaries create single-value ranges (e.g., (4,4), (8,8)),
        then subsequent boundaries fill gaps (e.g., (9,16), (17,32)).

        Args:
            boundaries: List of boundary values (e.g., [4, 8, 16, 32, 64])

        Returns:
            List of (low, high) tuples

        Examples:
            >>> QueryConfig._make_ranges([4, 8, 16, 32])
            [(4, 4), (8, 8), (9, 16), (17, 32)]
            >>> QueryConfig._make_ranges([1, 2, 4, 8])
            [(1, 1), (2, 2), (3, 4), (5, 8)]
        """
        if not boundaries:
            return []

        ranges = []

        # First two boundaries are singleton ranges
        for i in range(min(2, len(boundaries))):
            ranges.append((boundaries[i], boundaries[i]))

        # Remaining boundaries fill gaps
        if len(boundaries) > 2:
            prev = boundaries[1] + 1  # Start after second boundary
            for bound in boundaries[2:]:
                ranges.append((prev, bound))
                prev = bound + 1

        return ranges

    # GPU resource ranges: 4, 8, 9-16, 17-32, 33-64, 65-96, 97-128, 129-256, 257-320
    GPU_RANGES = _make_ranges([4, 8, 16, 32, 64, 96, 128, 256, 320])
    GPU_OVERFLOW = ">320"

    # Node resource ranges: 1, 2, 3-4, 5-8, ..., 1025-2048
    NODE_RANGES = _make_ranges([1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048])
    NODE_OVERFLOW = ">2048"

    # Core resource ranges: 1, 2, 3-4, 5-8, ..., 97-128
    CORE_RANGES = _make_ranges([1, 2, 4, 8, 16, 32, 48, 64, 96, 128])
    CORE_OVERFLOW = ">128"

    # Memory resource ranges (GB)
    MEMORY_RANGES = [
        (1, 10), (11, 50), (51, 100), (101, 500), (501, 1000)
    ]
    MEMORY_OVERFLOW = ">1000"

    # ------------------------------------------------------------------
    # Histogram bucket tables — (label, lo, hi) triples, bounds inclusive,
    # hi=None open-ended. The same triple shape as fs_scans' SIZE_BUCKETS,
    # which SAM already band-drills against. Consumed by
    # JobQueries.jobs_histogram via _HISTOGRAM_SPECS; lo/hi round-trip
    # into the matching min_*/max_* jobs_search filters.
    # ------------------------------------------------------------------

    @staticmethod
    def _ranges_to_buckets(ranges, overflow_label, overflow_lo):
        """Derive (label, lo, hi) triples from legacy (lo, hi) range pairs.

        Labels reproduce ``_build_range_case`` exactly ('1', '2', '3-4', …)
        so the histogram API and the legacy resource reports agree on
        vocabulary.
        """
        buckets = [
            (f"{lo}-{hi}" if lo != hi else str(lo), lo, hi)
            for lo, hi in ranges
        ]
        buckets.append((overflow_label, overflow_lo, None))
        return buckets

    # Queue-wait distribution buckets (seconds). eligible_secs masses near
    # zero with a tail out to days, so log-ish spacing gives resolution at
    # both ends. A NEW table: job_waits_by_resource buckets by *size* and
    # averages the wait — a distribution needs wait-valued bands.
    WAIT_BUCKETS = [
        ("<1m",    0,      59),
        ("1-5m",   60,     299),
        ("5-15m",  300,    899),
        ("15-30m", 900,    1799),
        ("30-60m", 1800,   3599),
        ("1-2h",   3600,   7199),
        ("2-4h",   7200,   14399),
        ("4-8h",   14400,  28799),
        ("8-12h",  28800,  43199),
        ("12-24h", 43200,  86399),
        ("1-2d",   86400,  172799),
        (">2d",    172800, None),
    ]

    # Elapsed-runtime distribution buckets (seconds). Labels and integer
    # band edges are identical to the historical get_duration_buckets()
    # dict, which is now DERIVED from this table — single source of truth.
    DURATION_HIST_BUCKETS = [
        ("<30s",    0,     29),
        ("30s-30m", 30,    1799),
        ("30-60m",  1800,  3599),
        ("1-5h",    3600,  17999),
        ("5-12h",   18000, 43199),
        ("12-18h",  43200, 64799),
        (">18h",    64800, None),
    ]

    # NODE_RANGES starts at 1, but a bucket table must reach the column's
    # domain floor or _bucket_case's `<= hi` ladder folds sub-floor values
    # into the first band while its advertised lo/hi excludes them (see the
    # leading-band note there). Hence the explicit zero band, matching
    # GPU_HIST_BUCKETS. No numnodes=0 rows exist today; numcpus=0 does.
    NODE_HIST_BUCKETS = (
        [("0", 0, 0)] + _ranges_to_buckets(NODE_RANGES, NODE_OVERFLOW, 2049)
    )

    # Deliberately NOT derived from CORE_RANGES: its >128 overflow would
    # swallow every multi-node derecho job (128 cpus is one derecho node).
    # Within-node resolution matches CORE_RANGES, then ×4 steps keep the
    # tail wide enough for 2000+-node jobs without a dozen more bars.
    # The zero band is load-bearing, not symmetry: 404 derecho / 21 casper
    # production rows carry numcpus=0, and without it they were counted in
    # the "1" bar while min_cpus=1/max_cpus=1 excluded them.
    CPU_HIST_BUCKETS = [
        ("0", 0, 0),
        ("1", 1, 1), ("2", 2, 2), ("3-4", 3, 4), ("5-8", 5, 8),
        ("9-16", 9, 16), ("17-32", 17, 32), ("33-64", 33, 64),
        ("65-128", 65, 128), ("129-512", 129, 512),
        ("513-2048", 513, 2048), ("2049-8192", 2049, 8192),
        ("8193-32768", 8193, 32768), (">32768", 32769, None),
    ]

    # Deliberately NOT derived from GPU_RANGES (starts at 4 — derecho-shaped):
    # explicit 0/1/2 buckets keep casper's small GPU jobs and the CPU/GPU
    # split visible. min_gpus=1 composes to drop the zero bucket.
    GPU_HIST_BUCKETS = [
        ("0", 0, 0), ("1", 1, 1), ("2", 2, 2), ("3-4", 3, 4),
        ("5-8", 5, 8), ("9-16", 9, 16), ("17-32", 17, 32),
        ("33-64", 33, 64), ("65-128", 65, 128), ("129-256", 129, 256),
        (">256", 257, None),
    ]

    # Requested-memory bands: raw bytes at GiB boundaries so returned
    # bounds round-trip exactly into min_reqmem/max_reqmem (no division in
    # SQL, no unit drift). Gains the <1GB band that the legacy
    # MEMORY_RANGES CASE folds into its ">1000" overflow.
    REQMEM_HIST_BUCKETS = [
        ("<1GB",       0,                _GIB - 1),
        ("1-10GB",     _GIB,             10 * _GIB),
        ("10-50GB",    10 * _GIB + 1,    50 * _GIB),
        ("50-100GB",   50 * _GIB + 1,    100 * _GIB),
        ("100-500GB",  100 * _GIB + 1,   500 * _GIB),
        ("500-1000GB", 500 * _GIB + 1,   1000 * _GIB),
        (">1000GB",    1000 * _GIB + 1,  None),
    ]

    # Used-memory bands: the same GiB boundaries as REQMEM_HIST_BUCKETS —
    # requested vs used on a shared x-axis is the point of the pair.
    MEMUSED_HIST_BUCKETS = list(REQMEM_HIST_BUCKETS)

    # Requested-minus-used bands. The leading "over request" band catches
    # negative deltas (the job used MORE memory than it requested —
    # ~0.3-0.4% of production rows on both machines); lo=None keeps it open
    # from below, so it round-trips as max_memory_wasted=-1 with no min
    # bound. _bucket_case's ascending `<= hi` ladder handles the negative
    # band with no special casing.
    MEMWASTED_HIST_BUCKETS = [("over request", None, -1)] + REQMEM_HIST_BUCKETS

    # Duration buckets (in seconds)
    @staticmethod
    def get_duration_buckets():
        """Get duration bucket definitions, derived from DURATION_HIST_BUCKETS.

        The label → SQL-condition dict shape predates the histogram table;
        deriving it keeps the labels and band edges in one place. The
        derivation reproduces the original conditions row-for-row: no lower
        bound on the first band, no upper bound on the last, and
        ``Job.elapsed`` is INTEGER seconds so ``<= 29`` ≡ the original
        ``< 30``.

        Returns as a static method to avoid issues with Job reference at import time.
        """
        buckets = {}
        for label, lo, hi in QueryConfig.DURATION_HIST_BUCKETS:
            conds = []
            if lo > 0:
                conds.append(Job.elapsed >= lo)
            if hi is not None:
                conds.append(Job.elapsed <= hi)
            buckets[label] = and_(*conds) if len(conds) > 1 else conds[0]
        return buckets

    @staticmethod
    def get_memory_per_rank_buckets():
        """Get memory-per-rank bucket definitions (mixed MB/GB units).

        Returns buckets for memory per rank histogram using Job.memory
        (actual memory used) divided by (mpiprocs * ompthreads * numnodes).

        Returns as a static method to avoid issues with Job reference at import time.
        """
        from ..sync.charging import BYTES_PER_GB
        BYTES_PER_MB = 1024 * 1024

        return {
            "<128MB": Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (128 * BYTES_PER_MB),
            "128MB-512MB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (128 * BYTES_PER_MB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (512 * BYTES_PER_MB)
            ),
            "512MB-1GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (512 * BYTES_PER_MB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < BYTES_PER_GB
            ),
            "1-2GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= BYTES_PER_GB,
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (2 * BYTES_PER_GB)
            ),
            "2-4GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (2 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (4 * BYTES_PER_GB)
            ),
            "4-8GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (4 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (8 * BYTES_PER_GB)
            ),
            "8-16GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (8 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (16 * BYTES_PER_GB)
            ),
            "16-32GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (16 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (32 * BYTES_PER_GB)
            ),
            "32-64GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (32 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (64 * BYTES_PER_GB)
            ),
            "64-128GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (64 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (128 * BYTES_PER_GB)
            ),
            "128-256GB": and_(
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (128 * BYTES_PER_GB),
                Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) < (256 * BYTES_PER_GB)
            ),
            ">256GB": Job.memory / (Job.mpiprocs * Job.ompthreads * Job.numnodes) >= (256 * BYTES_PER_GB),
        }


#: Requested-minus-used memory delta, in bytes. SQL NULL propagation makes
#: it NULL-strict for free: NULL in either column yields a NULL delta, which
#: range bounds exclude and the histogram routes to ``null_count``. Negative
#: values are legal and meaningful (the job used more than it requested), so
#: nothing here clamps at zero. The Label supplies the ``column`` name in
#: the histogram envelope; in WHERE/CASE contexts it compiles to the bare
#: expression (no AS clause).
_MEMORY_WASTED = (Job.reqmem - Job.memory).label('memory_wasted')

#: jobs_histogram dimension → (column, bucket table, unit, min/max kwargs).
#:
#: Dimension names are deliberately the SAM dashboard's tab/pill vocabulary,
#: not column names. ``min_param``/``max_param`` make each response
#: self-describing: a consumer turns a clicked bar into
#: ``jobs_search(**{min_param: lo, max_param: hi})`` without hardcoding the
#: dimension→filter map. ``memory`` buckets REQUESTED memory (``reqmem``) —
#: the dashboard ask is "resource needs" — while ``memory_used`` buckets the
#: memory actually consumed (``Job.memory``) and ``memory_wasted`` their
#: difference; per-rank used-memory physics stays with job_memory_per_rank.
_HISTOGRAM_SPECS: Dict[str, Tuple[Any, List[tuple], str, str, str]] = {
    'wait':     (Job.eligible_secs, QueryConfig.WAIT_BUCKETS,
                 'seconds', 'min_eligible_secs', 'max_eligible_secs'),
    'nodes':    (Job.numnodes,      QueryConfig.NODE_HIST_BUCKETS,
                 'count',   'min_nodes',         'max_nodes'),
    'cpus':     (Job.numcpus,       QueryConfig.CPU_HIST_BUCKETS,
                 'count',   'min_cpus',          'max_cpus'),
    'gpus':     (Job.numgpus,       QueryConfig.GPU_HIST_BUCKETS,
                 'count',   'min_gpus',          'max_gpus'),
    'memory':   (Job.reqmem,        QueryConfig.REQMEM_HIST_BUCKETS,
                 'bytes',   'min_reqmem',        'max_reqmem'),
    'duration': (Job.elapsed,       QueryConfig.DURATION_HIST_BUCKETS,
                 'seconds', 'min_elapsed',       'max_elapsed'),
    'memory_used':   (Job.memory,     QueryConfig.MEMUSED_HIST_BUCKETS,
                      'bytes', 'min_memory_used',   'max_memory_used'),
    'memory_wasted': (_MEMORY_WASTED, QueryConfig.MEMWASTED_HIST_BUCKETS,
                      'bytes', 'min_memory_wasted', 'max_memory_wasted'),
}


class JobQueries:
    """High-level query interface for job history data.

    This class provides convenient methods for common queries without
    requiring direct knowledge of the underlying SQLAlchemy models.

    Example:
        >>> from job_history import get_session, JobQueries
        >>> session = get_session("derecho")
        >>> queries = JobQueries(session)
        >>> jobs = queries.jobs_by_user("jdoe", start=date(2024, 1, 1))
    """

    def __init__(self, session: Session, machine: str = 'derecho'):
        """Initialize query interface.

        Args:
            session: SQLAlchemy session for database access
            machine: Machine name ('casper' or 'derecho') for queue filtering
        """
        self.session = session
        self.machine = machine.lower()

    def _apply_date_filter(self, query, start: Optional[date], end: Optional[date]):
        """Apply consistent date filtering to a query.

        ``start`` and ``end`` are interpreted as **site-local calendar days**
        (per ``JOB_HISTORY_SITE_TIMEZONE``, default ``America/Denver``), not
        UTC. The filter on ``Job.end`` is converted to the equivalent naive
        UTC bounds — the same convention :mod:`sync.summary` uses to bin
        jobs into ``DailySummary`` rows, so ``jobs_search`` and the daily
        rollup always agree on which calendar day a job belongs to.

        Without this conversion a job that ended at e.g. ``02:00 UTC`` on
        2026-05-18 (=``20:00 MDT`` on 2026-05-17) would be counted in the
        MDT-17 daily summary but excluded from a ``start=end=2026-05-17``
        ``jobs_search`` filter — a silent under-count for every evening
        job at western sites.

        ``Job.end`` is stored as a naive UTC timestamp, so we strip
        ``tzinfo`` after the conversion to keep psycopg2 from re-applying
        the session's local TZ to the bind parameter.

        The end boundary uses a half-open interval against the start of
        the next site-local day (``< end+1day``) rather than ``<= 23:59:59.999``
        to avoid sub-microsecond precision games and to match :mod:`sync.summary`.

        Args:
            query: SQLAlchemy query object.
            start: Optional first site-local day to include.
            end:   Optional last  site-local day to include.

        Returns:
            Filtered query.
        """
        site_tz = ZoneInfo(JobHistoryConfig.SITE_TIMEZONE)
        if start is not None:
            start_utc = (
                datetime.combine(start, time.min)
                .replace(tzinfo=site_tz)
                .astimezone(timezone.utc)
                .replace(tzinfo=None)
            )
            query = query.filter(Job.end >= start_utc)
        if end is not None:
            end_exclusive_utc = (
                datetime.combine(end + timedelta(days=1), time.min)
                .replace(tzinfo=site_tz)
                .astimezone(timezone.utc)
                .replace(tzinfo=None)
            )
            query = query.filter(Job.end < end_exclusive_utc)
        return query

    def _build_range_case(self, ranges: List[tuple], overflow_label: str, field):
        """Build a CASE statement for resource range bucketing.

        Args:
            ranges: List of (low, high) tuples defining ranges
            overflow_label: Label for values exceeding max range
            field: SQLAlchemy column to apply ranges to (e.g., Job.numgpus)

        Returns:
            SQLAlchemy case statement with label "range_label"
        """
        return case(
            *[
                (and_(field >= low, field <= high),
                 f"{low}-{high}" if low != high else str(low))
                for low, high in ranges
            ],
            else_=overflow_label
        ).label("range_label")

    def _build_range_ordering(self, ranges: List[tuple], overflow_label: str, range_column):
        """Build ordering expression for range-based results.

        Ensures ranges appear in natural order (1, 2, 3-4, 5-8, ..., >max).

        Args:
            ranges: List of (low, high) tuples
            overflow_label: Label for overflow range
            range_column: Column containing range labels

        Returns:
            SQLAlchemy case ordering expression
        """
        order_cases = {
            f"{low}-{high}" if low != high else str(low): i
            for i, (low, high) in enumerate(ranges)
        }
        order_cases[overflow_label] = len(ranges)
        return case(order_cases, value=range_column)

    def usage_by_group(
        self,
        resource_type: str,
        group_by: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get resource usage statistics grouped by user or account.

        Generic factory method replacing pie chart queries.

        Args:
            resource_type: 'cpu' | 'gpu' | 'all' - type of resources to query
            group_by: 'user' | 'account' - field to group by
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            List of dicts with keys: 'label', 'usage_hours', 'job_count'

        Examples:
            >>> # CPU usage by user (replaces pie_user_cpu)
            >>> queries.usage_by_group('cpu', 'user', start_date, end_date)
            >>> # GPU usage by account (replaces pie_group_gpu)
            >>> queries.usage_by_group('gpu', 'account', start_date, end_date)
        """
        # Determine queues and hours field (machine-specific)
        if resource_type == 'cpu':
            queues = QueryConfig.get_cpu_queues(self.machine)
            hours_field = JobCharge.cpu_hours
        elif resource_type == 'gpu':
            queues = QueryConfig.get_gpu_queues(self.machine)
            hours_field = JobCharge.gpu_hours
        else:  # 'all'
            queues = QueryConfig.get_cpu_queues(self.machine) + QueryConfig.get_gpu_queues(self.machine)
            # For 'all', sum both cpu_hours and gpu_hours
            hours_field = func.coalesce(JobCharge.cpu_hours, 0) + func.coalesce(JobCharge.gpu_hours, 0)

        # Determine group field
        group_field = Job.user if group_by == 'user' else Job.account

        # Build query
        query = self.session.query(
            group_field.label("label"),
            func.sum(hours_field).label("usage_hours"),
            func.count(Job.id).label("job_count")
        ).join(JobCharge, Job.id == JobCharge.job_id).filter(Job.queue.in_(queues))

        query = self._apply_date_filter(query, start, end)
        results = query.group_by(group_field).order_by(func.sum(hours_field).desc()).all()

        return [
            {
                "label": row[0],
                "usage_hours": row[1] or 0.0,
                "job_count": row[2] or 0,
            }
            for row in results
        ]

    def usage_by_facility(
        self,
        resource_type: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Resource usage aggregated into facility buckets.

        Post-aggregates `usage_by_group(resource_type, 'account', ...)` by
        mapping each account through `_account_to_facility()` and summing
        per-bucket. Returns rows sorted DESC by usage_hours.
        """
        rows = self.usage_by_group(resource_type, "account", start, end)
        buckets: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            fac = _account_to_facility(row["label"])
            b = buckets.setdefault(fac, {"usage_hours": 0.0, "job_count": 0})
            b["usage_hours"] += row["usage_hours"] or 0.0
            b["job_count"] += row["job_count"] or 0
        return sorted(
            [{"label": k, **v} for k, v in buckets.items()],
            key=lambda r: r["usage_hours"],
            reverse=True,
        )

    def job_waits_by_resource(
        self,
        resource_type: str,
        range_type: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get job wait statistics grouped by resource count ranges.

        Generic factory method replacing wait time queries.

        Args:
            resource_type: 'cpu' | 'gpu' | 'all' - type of resources to filter
            range_type: 'gpu' | 'node' | 'core' | 'memory' - resource to group by
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            List of dicts with keys: 'range_label', 'avg_wait_hours', 'job_count'

        Examples:
            >>> # GPU job waits (replaces gpu_job_waits_by_gpu_ranges)
            >>> queries.job_waits_by_resource('gpu', 'gpu', start_date, end_date)
            >>> # CPU job waits by node (replaces cpu_job_waits_by_node_ranges)
            >>> queries.job_waits_by_resource('cpu', 'node', start_date, end_date)
        """
        # Determine queues (machine-specific)
        if resource_type == 'cpu':
            queues = QueryConfig.get_cpu_queues(self.machine)
        elif resource_type == 'gpu':
            queues = QueryConfig.get_gpu_queues(self.machine)
        else:  # 'all'
            queues = QueryConfig.get_cpu_queues(self.machine) + QueryConfig.get_gpu_queues(self.machine)

        # Determine ranges and field
        if range_type == 'gpu':
            ranges = QueryConfig.GPU_RANGES
            overflow = QueryConfig.GPU_OVERFLOW
            field = Job.numgpus
        elif range_type == 'node':
            ranges = QueryConfig.NODE_RANGES
            overflow = QueryConfig.NODE_OVERFLOW
            field = Job.numnodes
        elif range_type == 'core':
            ranges = QueryConfig.CORE_RANGES
            overflow = QueryConfig.CORE_OVERFLOW
            field = Job.numcpus
        else:  # 'memory'
            ranges = QueryConfig.MEMORY_RANGES
            overflow = QueryConfig.MEMORY_OVERFLOW
            # Convert bytes to GB for memory ranges
            field = Job.reqmem / (1024**3)

        # Build range case and wait time calculation.
        #
        # Wait time is PBS's own eligible_time accrual, not start - eligible.
        # start - eligible (i.e. start - etime) is effectively submit -> start:
        # qtime == etime on 77,152 of 77,154 sampled derecho E records, so it
        # counts time the job spent held, dependency-blocked, or deferred by
        # `qsub -a` -- none of which is the site making the user wait.
        # eligible_secs counts only time blocked on resource scarcity.
        range_case = self._build_range_case(ranges, overflow, field)
        wait_time_hours = Job.eligible_secs / 3600.0

        # Build subquery.  The IS NOT NULL filter is required, not redundant:
        # func.avg already skips NULLs, but func.count(id) below would not, so
        # without it #-Jobs would count jobs excluded from the average.  Jobs
        # predating `eligible_time_enable` (derecho before 2025-01-07
        # 17:47:50 UTC) are
        # dropped rather than silently mixed with a different wait definition.
        subquery = self.session.query(
            Job.id,
            range_case,
            wait_time_hours.label("wait_hours")
        ).filter(Job.queue.in_(queues), Job.eligible_secs.isnot(None))

        subquery = self._apply_date_filter(subquery, start, end)
        subquery = subquery.subquery()

        # Aggregate by range
        query = self.session.query(
            subquery.c.range_label,
            func.avg(subquery.c.wait_hours).label("avg_wait_hours"),
            func.count(subquery.c.id).label("job_count")
        ).group_by(subquery.c.range_label)

        # Apply custom ordering
        order_expr = self._build_range_ordering(ranges, overflow, subquery.c.range_label)
        results = query.order_by(order_expr).all()

        return [
            {
                "range_label": row[0],
                "avg_wait_hours": row[1] or 0.0,
                "job_count": row[2],
            }
            for row in results
        ]

    def job_sizes_by_resource(
        self,
        resource_type: str,
        range_type: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get job size statistics grouped by resource count ranges.

        Generic factory method replacing job size queries.

        Args:
            resource_type: 'cpu' | 'gpu' | 'all' - type of resources to filter
            range_type: 'gpu' | 'node' | 'core' | 'memory' - resource to group by
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            List of dicts with keys: 'range_label', 'job_count', 'user_count', 'hours'

        Examples:
            >>> # GPU job sizes (replaces gpu_job_sizes_by_gpu_ranges)
            >>> queries.job_sizes_by_resource('gpu', 'gpu', start_date, end_date)
            >>> # CPU job sizes by node (replaces cpu_job_sizes_by_node_ranges)
            >>> queries.job_sizes_by_resource('cpu', 'node', start_date, end_date)
        """
        # Determine queues and hours field (machine-specific)
        if resource_type == 'cpu':
            queues = QueryConfig.get_cpu_queues(self.machine)
            hours_field = JobCharge.cpu_hours
        elif resource_type == 'gpu':
            queues = QueryConfig.get_gpu_queues(self.machine)
            hours_field = JobCharge.gpu_hours
        else:  # 'all'
            queues = QueryConfig.get_cpu_queues(self.machine) + QueryConfig.get_gpu_queues(self.machine)
            hours_field = func.coalesce(JobCharge.cpu_hours, 0) + func.coalesce(JobCharge.gpu_hours, 0)

        # Determine ranges and field
        if range_type == 'gpu':
            ranges = QueryConfig.GPU_RANGES
            overflow = QueryConfig.GPU_OVERFLOW
            field = Job.numgpus
        elif range_type == 'node':
            ranges = QueryConfig.NODE_RANGES
            overflow = QueryConfig.NODE_OVERFLOW
            field = Job.numnodes
        elif range_type == 'core':
            ranges = QueryConfig.CORE_RANGES
            overflow = QueryConfig.CORE_OVERFLOW
            field = Job.numcpus
        else:  # 'memory'
            ranges = QueryConfig.MEMORY_RANGES
            overflow = QueryConfig.MEMORY_OVERFLOW
            field = Job.reqmem / (1024**3)

        # Build range case
        range_case = self._build_range_case(ranges, overflow, field)

        # Build subquery — use user_id (real FK column) not the hybrid property,
        # since hybrid properties expand to correlated scalar subqueries which
        # SQLite cannot reference by column name inside a derived table alias.
        subquery = self.session.query(
            Job.id,
            Job.user_id,
            hours_field.label("hours_field"),
            range_case
        ).join(JobCharge, Job.id == JobCharge.job_id).filter(Job.queue.in_(queues))

        subquery = self._apply_date_filter(subquery, start, end)
        subquery = subquery.subquery()

        # Aggregate by range
        query = self.session.query(
            subquery.c.range_label,
            func.count(subquery.c.id).label("job_count"),
            func.count(func.distinct(subquery.c.user_id)).label("user_count"),
            func.sum(subquery.c.hours_field).label("hours")
        ).group_by(subquery.c.range_label)

        # Apply custom ordering
        order_expr = self._build_range_ordering(ranges, overflow, subquery.c.range_label)
        results = query.order_by(order_expr).all()

        return [
            {
                "range_label": row[0],
                "job_count": row[1],
                "user_count": row[2],
                "hours": row[3] or 0.0,
            }
            for row in results
        ]

    def job_durations(
        self,
        resource_type: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get job duration statistics by period.

        Generic factory method replacing duration queries.

        Args:
            resource_type: 'cpu' | 'gpu' | 'all' - type of resources to filter
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day' or 'month')

        Returns:
            List of dicts with keys: 'date', '<30s', '30s-30m', '30-60m', '1-5h', '5-12h', '12-18h', '>18h'

        Examples:
            >>> # GPU job durations by day
            >>> queries.job_durations('gpu', start_date, end_date)
            >>> # CPU job durations by month
            >>> queries.job_durations('cpu', start_date, end_date, period='month')
        """
        from .builders import ResourceTypeResolver, PeriodGrouper

        # Resolve resource type to queues and hours field (machine-specific)
        queues, hours_field = ResourceTypeResolver.resolve(resource_type, self.machine, JobCharge)

        # Get duration buckets
        duration_buckets = QueryConfig.get_duration_buckets()

        # Get period grouping function
        period_func = PeriodGrouper.get_period_func(period, Job.end)

        # Build query
        query = self.session.query(
            period_func.label("job_date"),
            *[
                func.sum(case((bucket, hours_field), else_=0)).label(label)
                for label, bucket in duration_buckets.items()
            ]
        ).join(JobCharge, Job.id == JobCharge.job_id).filter(Job.queue.in_(queues))

        query = self._apply_date_filter(query, start, end)
        results = query.group_by("job_date").order_by("job_date").all()

        return [
            {
                "date": row[0],
                **{label: row[i+1] or 0.0 for i, label in enumerate(duration_buckets.keys())}
            }
            for row in results
        ]

    def job_memory_per_rank(
        self,
        resource_type: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get job memory-per-rank histogram by period.

        Calculates memory per rank as memory_bytes / (mpiprocs * ompthreads * numnodes).
        Filters out jobs where mpiprocs, ompthreads, numnodes, or memory is 0 or NULL.

        Args:
            resource_type: 'cpu' | 'gpu' - type of resources to filter
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day' or 'month')

        Returns:
            List of dicts with date + histogram bucket columns

        Examples:
            >>> # CPU job memory-per-rank by day
            >>> queries.job_memory_per_rank('cpu', start_date, end_date)
            >>> # GPU job memory-per-rank by month
            >>> queries.job_memory_per_rank('gpu', start_date, end_date, period='month')
        """
        from .builders import ResourceTypeResolver, PeriodGrouper

        # Resolve resource type to queues and hours field (machine-specific)
        queues, hours_field = ResourceTypeResolver.resolve(resource_type, self.machine, JobCharge)

        # Get memory-per-rank buckets
        memory_buckets = QueryConfig.get_memory_per_rank_buckets()

        # Get period grouping function
        period_func = PeriodGrouper.get_period_func(period, Job.end)

        # Build query with CASE statements for each bucket
        query = self.session.query(
            period_func.label("job_date"),
            *[
                func.sum(case((bucket, hours_field), else_=0)).label(label)
                for label, bucket in memory_buckets.items()
            ]
        ).join(JobCharge, Job.id == JobCharge.job_id).filter(
            Job.queue.in_(queues),
            Job.mpiprocs.isnot(None),   # Filter NULL
            Job.mpiprocs > 0,           # Filter zero (prevents division by zero)
            Job.ompthreads.isnot(None), # Filter NULL
            Job.ompthreads > 0,         # Filter zero (prevents division by zero)
            Job.numnodes.isnot(None),   # Filter NULL
            Job.numnodes > 0,           # Filter zero (prevents division by zero)
            Job.memory.isnot(None)      # Filter NULL memory
        )

        query = self._apply_date_filter(query, start, end)
        results = query.group_by("job_date").order_by("job_date").all()

        return [
            {
                "date": row[0],
                **{label: row[i+1] or 0.0 for i, label in enumerate(memory_buckets.keys())}
            }
            for row in results
        ]

    def memory_job_waits(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get job wait statistics grouped by memory requirement ranges.

        Convenience method wrapping job_waits_by_resource with memory range type.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            List of dicts with keys: 'range_label', 'avg_wait_hours', 'job_count'

        Examples:
            >>> queries.memory_job_waits(start_date, end_date)
        """
        return self.job_waits_by_resource(
            resource_type='all',
            range_type='memory',
            start=start,
            end=end
        )

    def memory_job_sizes(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get job size statistics grouped by memory requirement ranges.

        Convenience method wrapping job_sizes_by_resource with memory range type.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            List of dicts with keys: 'range_label', 'job_count', 'user_count', 'hours'

        Examples:
            >>> queries.memory_job_sizes(start_date, end_date)
        """
        return self.job_sizes_by_resource(
            resource_type='all',
            range_type='memory',
            start=start,
            end=end
        )

    def _build_date_filter(self, start: Optional[date], end: Optional[date]) -> List:
        """Build date filter conditions for usage_history queries.

        Args:
            start: Optional start date (inclusive)
            end: Optional end date (inclusive)

        Returns:
            List of SQLAlchemy filter conditions
        """
        date_filter = []
        if start:
            date_filter.append(Job.end >= datetime.combine(start, datetime.min.time()))
        if end:
            date_filter.append(Job.end <= datetime.combine(end, datetime.max.time()))
        return date_filter

    def _usage_history_total_users(self, period_col, date_filter: List):
        """Build subquery for total unique users per period.

        Args:
            period_col: SQLAlchemy expression for period grouping
            date_filter: List of filter conditions

        Returns:
            SQLAlchemy subquery for total users
        """
        return self.session.query(
            period_col.label("period"),
            func.count(func.distinct(Job.user)).label("total_users")
        ).filter(*date_filter).group_by("period").subquery()

    def _usage_history_total_projects(self, period_col, date_filter: List):
        """Build subquery for total unique projects per period.

        Args:
            period_col: SQLAlchemy expression for period grouping
            date_filter: List of filter conditions

        Returns:
            SQLAlchemy subquery for total projects
        """
        return self.session.query(
            period_col.label("period"),
            func.count(func.distinct(Job.account)).label("total_projects")
        ).filter(*date_filter).group_by("period").subquery()

    def _usage_history_resource_stats(
        self, resource_type: str, period_col, date_filter: List
    ):
        """Build subquery for CPU or GPU stats per period.

        Args:
            resource_type: 'cpu' or 'gpu'
            period_col: SQLAlchemy expression for period grouping
            date_filter: List of filter conditions

        Returns:
            SQLAlchemy subquery for resource stats (users, projects, jobs, hours)
        """
        from .builders import ResourceTypeResolver

        queues, hours_field = ResourceTypeResolver.resolve(
            resource_type, self.machine, JobCharge
        )

        prefix = resource_type.lower()

        return self.session.query(
            period_col.label("period"),
            func.count(func.distinct(Job.user)).label(f"{prefix}_users"),
            func.count(func.distinct(Job.account)).label(f"{prefix}_projects"),
            func.count(Job.id).label(f"{prefix}_jobs"),
            func.sum(hours_field).label(f"{prefix}_hours")
        ).join(
            JobCharge, Job.id == JobCharge.job_id
        ).filter(
            Job.queue.in_(queues), *date_filter
        ).group_by("period").subquery()

    def _join_usage_history_results(self, users_sq, projects_sq, cpu_sq, gpu_sq):
        """Join subqueries and format usage history results.

        Args:
            users_sq: Total users subquery
            projects_sq: Total projects subquery
            cpu_sq: CPU stats subquery
            gpu_sq: GPU stats subquery

        Returns:
            List of formatted result dictionaries with usage history data
        """
        query = self.session.query(
            users_sq.c.period,
            users_sq.c.total_users,
            projects_sq.c.total_projects,
            cpu_sq.c.cpu_users,
            cpu_sq.c.cpu_projects,
            cpu_sq.c.cpu_jobs,
            cpu_sq.c.cpu_hours,
            gpu_sq.c.gpu_users,
            gpu_sq.c.gpu_projects,
            gpu_sq.c.gpu_jobs,
            gpu_sq.c.gpu_hours
        ).join(
            projects_sq, users_sq.c.period == projects_sq.c.period
        ).outerjoin(
            cpu_sq, users_sq.c.period == cpu_sq.c.period
        ).outerjoin(
            gpu_sq, users_sq.c.period == gpu_sq.c.period
        ).order_by(users_sq.c.period)

        results = query.all()

        return [
            {
                "Date": row[0],
                "#-Users": row[1] or 0,
                "#-Proj": row[2] or 0,
                "#-CPU-Users": row[3] or 0,
                "#-CPU-Proj": row[4] or 0,
                "#-CPU-Jobs": row[5] or 0,
                "#-CPU-Hrs": row[6] or 0.0,
                "#-GPU-Users": row[7] or 0,
                "#-GPU-Proj": row[8] or 0,
                "#-GPU-Jobs": row[9] or 0,
                "#-GPU-Hrs": row[10] or 0.0,
            }
            for row in results
        ]

    def usage_history(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get usage history by time period.

        This method coordinates 4 subqueries to gather comprehensive usage
        statistics per period (day, month, quarter, year):
        1. Total unique users across all queues
        2. Total unique projects across all queues
        3. CPU queue statistics (users, projects, jobs, hours)
        4. GPU queue statistics (users, projects, jobs, hours)

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day', 'month', 'quarter', 'year')

        Returns:
            List of dicts with usage history statistics for each period.
            Each dict contains: Date, #-Users, #-Proj, #-CPU-Users, #-CPU-Proj,
            #-CPU-Jobs, #-CPU-Hrs, #-GPU-Users, #-GPU-Proj, #-GPU-Jobs, #-GPU-Hrs
        """
        from .builders import PeriodGrouper

        # Get period grouping expression
        period_col = PeriodGrouper.get_period_func(period, Job.end)

        # Build date filter once
        date_filter = self._build_date_filter(start, end)

        # Build 4 subqueries
        total_users_sq = self._usage_history_total_users(period_col, date_filter)
        total_projects_sq = self._usage_history_total_projects(period_col, date_filter)
        cpu_stats_sq = self._usage_history_resource_stats('cpu', period_col, date_filter)
        gpu_stats_sq = self._usage_history_resource_stats('gpu', period_col, date_filter)

        # Join and format results
        return self._join_usage_history_results(
            total_users_sq, total_projects_sq, cpu_stats_sq, gpu_stats_sq
        )

    def jobs_by_user(
        self,
        user: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
        exit_status: Optional[str] = None,
        queue: Optional[str] = None,
    ) -> List[Job]:
        """Get all jobs for a user, optionally filtered by date range and other criteria.

        Args:
            user: Username to query
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            exit_status: Optional PBS ``Exit_status`` filter ('0' == success)
            queue: Optional queue name filter

        Returns:
            List of Job objects matching the criteria
        """
        query = self.session.query(Job).filter(Job.user == user)

        query = self._apply_date_filter(query, start, end)
        if exit_status:
            query = query.filter(Job.status == exit_status)
        if queue:
            query = query.filter(Job.queue == queue)

        return query.order_by(Job.end.desc()).all()

    def jobs_by_account(
        self,
        account: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
        exit_status: Optional[str] = None,
    ) -> List[Job]:
        """Get all jobs for an account, optionally filtered by date range.

        Args:
            account: Account name to query
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            exit_status: Optional PBS ``Exit_status`` filter ('0' == success)

        Returns:
            List of Job objects matching the criteria
        """
        query = self.session.query(Job).filter(Job.account == account)

        query = self._apply_date_filter(query, start, end)
        if exit_status:
            query = query.filter(Job.status == exit_status)

        return query.order_by(Job.end.desc()).all()

    def jobs_by_queue(
        self,
        queue: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> List[Job]:
        """Get all jobs for a queue, optionally filtered by date range.

        Args:
            queue: Queue name to query
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time

        Returns:
            List of Job objects matching the criteria
        """
        query = self.session.query(Job).filter(Job.queue == queue)

        query = self._apply_date_filter(query, start, end)

        return query.order_by(Job.end.desc()).all()

    def jobs_search(
        self,
        *,
        start: Optional[date] = None,
        end: Optional[date] = None,
        user: Optional[str] = None,
        account: Optional[Union[str, Sequence[str]]] = None,
        queue: Optional[str] = None,
        qos: Optional[str] = None,
        exit_status: Optional[str] = None,
        job_id: Optional[str] = None,
        name: Optional[Union[str, Sequence[str]]] = None,
        ignore_case: bool = False,
        min_eligible_secs: Optional[int] = None,
        max_eligible_secs: Optional[int] = None,
        min_nodes: Optional[int] = None,
        max_nodes: Optional[int] = None,
        min_cpus: Optional[int] = None,
        max_cpus: Optional[int] = None,
        min_gpus: Optional[int] = None,
        max_gpus: Optional[int] = None,
        min_elapsed: Optional[int] = None,
        max_elapsed: Optional[int] = None,
        min_reqmem: Optional[int] = None,
        max_reqmem: Optional[int] = None,
        min_memory_used: Optional[int] = None,
        max_memory_used: Optional[int] = None,
        min_memory_wasted: Optional[int] = None,
        max_memory_wasted: Optional[int] = None,
        columns: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        sort_by: Optional[str] = None,
        sort_dir: str = 'desc',
    ) -> List[Dict[str, Any]]:
        """Search individual job records — unified, dict-row contract.

        Returns a ``list[dict]`` with one entry per matching job; the dict
        keys are controlled by the ``columns`` parameter and default to
        :data:`job_history.columns.DEFAULT_COLUMNS`. Filters compose via AND.

        This is the job-level companion to :meth:`daily_summary_report`,
        which returns per-day *aggregated* rows. The two row shapes share
        a handful of keys (``user``, ``account``, ``queue``, ``cpu_hours``,
        ``gpu_hours``) but are otherwise different — ``jobs_search`` adds
        per-job identifiers and resource fields (``job_id``, ``start``,
        ``end``, ``elapsed``, ``numnodes``, ``numcpus``, ``numgpus``, …)
        and ``daily_summary_report`` adds aggregate fields (``date``,
        ``job_count``, ``memory_hours``, ``*_charges``).

        Args:
            start: Optional start date (inclusive) — filters on ``Job.end``
            end: Optional end date (inclusive) — filters on ``Job.end``
            user: Optional username filter (text; resolved via FK hybrid)
            account: Optional project/account filter. Accepts either a
                single projcode (``"NCAR0002"``) or a sequence of
                projcodes (``["NCAR0002", "NCAR0002_a", "NCAR0002_b"]``)
                — a sequence applies ``Job.account IN (...)`` so callers
                can pull every projcode in a project tree in one query.
                Resolved via FK hybrid.
            queue: Optional queue filter (text; resolved via FK hybrid)
            qos: Optional QoS / priority-class filter (text; resolved via FK
                hybrid against the ``job_qos`` lookup — e.g. ``"premium"``,
                ``"regular"``, ``"economy"``, ``"special"``, ``"uncharged"``).
            exit_status: Optional PBS ``Exit_status`` filter. This is an
                exit *code*, not a job state — ``'0'`` is success, non-zero
                is a failure or signal (e.g. ``'271'``, ``'143'``). Stored
                as text in ``Job.status``.
            job_id: Optional job-id filter, classified by input shape:

                * Has a ``.`` (e.g. ``"6049117[28].desched1"``) → exact match
                  on ``Job.job_id``.
                * No ``.`` — either bare digits (``"6049117"``) or a partial
                  array form (``"6049117[28]"``, ``"6049117[]"``) → two
                  boundary-anchored ``LIKE`` clauses
                  (``<input>.%`` OR ``<input>[%``) so e.g. ``"6049117"``
                  matches the scalar ``6049117.host`` AND every array
                  element ``6049117[N].host`` AND the array parent
                  ``6049117[].host``, but does **not** bleed into the
                  unrelated ``60491170.host``.

                ``Job.short_id`` is intentionally not consulted: pbsparse
                leaves it ``NULL`` on every array-job row, so a
                ``short_id``-based path would miss the dominant
                "match all elements of my array job" use case.
            name: Optional job-name glob filter on ``Job.name``. Accepts a
                single pattern (``"wrf_*"``) or a sequence of patterns
                (``["wrf_*", "*.restart"]``) OR'd together. Shell-glob
                syntax: ``*`` matches any run of characters, ``?`` exactly
                one. Case-sensitive unless ``ignore_case``.

                Unlike ``account``, an **empty sequence means "no filter"**,
                not "no rows" — Click's ``multiple=True`` hands us ``()``
                when ``-N`` is simply not supplied, so the empty case has to
                be the identity or a plain ``jobhist search --user alice``
                would return nothing.

                Backend note: on SQLite this compiles to ``GLOB`` (which
                additionally honours ``[abc]`` character classes); on
                PostgreSQL to an anchored POSIX regex (``~``) in which ``[``
                and ``]`` are literals. Stick to ``*`` and ``?`` for
                identical results on both. Jobs with a NULL ``Job.name``
                never match any pattern.
            ignore_case: Case-insensitive matching for ``name`` only.
                Default False, matching the ``fs-scans query -N/-i`` pair.
            min_eligible_secs: Lower bound (inclusive) on
                ``Job.eligible_secs`` — PBS ``eligible_time``, i.e. wall time
                blocked purely by resource scarcity.
            max_eligible_secs: Upper bound (inclusive) on the same column.

                **Both bounds exclude jobs with a NULL ``eligible_secs``.**
                That is plain SQL three-valued logic and it is the behaviour
                we want: ``eligible_time_enable`` was off on derecho until
                2025-01-07 17:47:50 UTC, so every job ending before then has
                *no wait measurement at all* — not a wait of zero. Folding
                those into a ``max_eligible_secs`` result would claim "these
                jobs waited less than N" about jobs whose wait is unknown.
                :meth:`job_waits_by_resource` makes the same exclusion
                explicit because ``AVG`` and ``COUNT`` disagreed about NULLs;
                here the comparison operator does it, and because
                :meth:`jobs_count` shares this exact predicate the count can
                never drift from the row set.
            min_nodes / max_nodes: Inclusive bounds on ``Job.numnodes``.
            min_cpus / max_cpus: Inclusive bounds on ``Job.numcpus``.
            min_gpus / max_gpus: Inclusive bounds on ``Job.numgpus``.
                ``min_gpus=1`` selects GPU jobs; ``max_gpus=0`` selects
                CPU-only jobs.
            min_elapsed / max_elapsed: Inclusive bounds on ``Job.elapsed``
                (walltime actually used), in **seconds**. Distinct from
                ``Job.walltime``, the walltime *requested*.
            min_reqmem / max_reqmem: Inclusive bounds on ``Job.reqmem``
                (memory *requested* at submit), in **bytes**. Requested,
                not used — used memory is ``Job.memory``, filtered by
                ``min_/max_memory_used`` below.
            min_memory_used / max_memory_used: Inclusive bounds on
                ``Job.memory`` (peak memory actually *used*, PBS
                ``resources_used.mem``), in **bytes**.
            min_memory_wasted / max_memory_wasted: Inclusive bounds on the
                computed ``Job.reqmem - Job.memory`` delta (requested minus
                used), in **bytes**. The delta may legitimately be
                **negative** — the job used more than it requested — so
                ``max_memory_wasted=-1`` selects exactly those over-request
                jobs, and neither bound clamps at zero. A NULL in *either*
                column makes the delta NULL, so those rows drop out under
                either bound (standard NULL-strict rules).

                All fourteen range bounds are NULL-strict: a NULL column
                value fails both comparisons and the row drops out.

                Performance: ``name``, ``eligible_secs``, ``numnodes``,
                ``numcpus``, ``numgpus``, ``elapsed``, ``reqmem`` and
                ``memory`` are **unindexed**. Each of these filters is
                evaluated as a scan of whatever slice ``start``/``end``
                leave behind (via ``ix_jobs_end``). Production derecho
                holds ~15.7M rows and casper ~26.7M, so always pass a
                bounded date window alongside them.
            columns: Optional sequence of column keys to project.
                When None, returns DEFAULT_COLUMNS. Unknown keys raise ValueError.
            limit: Optional max number of rows to return. Applied as a SQL
                ``LIMIT`` (server-side) so the truncated rows are never
                materialized. Must be a positive integer.
            offset: Non-negative SQL ``OFFSET`` for paging. Default 0.
            sort_by: Optional column-key (from ``COLUMNS``) to sort by.
                ``None`` keeps the historical default (``Job.end DESC``).
                Computed ``*_charges`` keys sort on ``hours × qos_factor``.
            sort_dir: ``'asc'`` or ``'desc'``. Ignored when ``sort_by`` is None.

        Returns:
            List of dicts ordered by ``sort_by`` (default ``Job.end DESC``).
            Each dict contains exactly the requested column keys, with values
            pulled from ``Job`` columns or the outer-joined ``JobCharge`` row.
        """
        cols = tuple(columns) if columns is not None else DEFAULT_COLUMNS
        unknown = [c for c in cols if c not in COLUMNS]
        if unknown:
            valid = ", ".join(sorted(COLUMNS))
            raise ValueError(
                f"Unknown column(s): {', '.join(unknown)}. Valid columns: {valid}"
            )

        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise ValueError(f"limit must be a positive integer, got {limit!r}")
        if not isinstance(offset, int) or offset < 0:
            raise ValueError(f"offset must be a non-negative integer, got {offset!r}")
        if sort_by is not None and sort_by not in COLUMNS:
            valid = ", ".join(sorted(COLUMNS))
            raise ValueError(
                f"Unknown sort_by: {sort_by!r}. Valid keys: {valid}"
            )
        if sort_by is not None and sort_dir not in ('asc', 'desc'):
            raise ValueError(
                f"sort_dir must be 'asc' or 'desc', got {sort_dir!r}"
            )

        query = (
            self.session.query(Job, JobCharge)
            .outerjoin(JobCharge, Job.id == JobCharge.job_id)
        )
        query = self._apply_jobs_search_filters(
            query, start=start, end=end, user=user, account=account,
            queue=queue, qos=qos, exit_status=exit_status, job_id=job_id,
            name=name, ignore_case=ignore_case,
            min_eligible_secs=min_eligible_secs,
            max_eligible_secs=max_eligible_secs,
            min_nodes=min_nodes, max_nodes=max_nodes,
            min_cpus=min_cpus, max_cpus=max_cpus,
            min_gpus=min_gpus, max_gpus=max_gpus,
            min_elapsed=min_elapsed, max_elapsed=max_elapsed,
            min_reqmem=min_reqmem, max_reqmem=max_reqmem,
            min_memory_used=min_memory_used,
            max_memory_used=max_memory_used,
            min_memory_wasted=min_memory_wasted,
            max_memory_wasted=max_memory_wasted,
        )

        if sort_by is None:
            query = query.order_by(Job.end.desc())
        else:
            lookup = _LOOKUP_DIMS.get(sort_by)
            if lookup is not None:
                # OUTER join (the FKs are nullable — an inner join would
                # drop rows and break jobs_search ↔ jobs_count agreement)
                # and no added select entity: the (Job, JobCharge) tuple
                # unpacked below must keep its arity.
                model, fk_col, expr = lookup
                query = query.outerjoin(model, fk_col == model.id)
            else:
                expr = _sort_expression(sort_by)
            query = query.order_by(expr.desc() if sort_dir == 'desc' else expr.asc())

        if limit is not None:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)
        return [project_row(job, charge, cols) for job, charge in query.all()]

    def jobs_count(
        self,
        *,
        start: Optional[date] = None,
        end: Optional[date] = None,
        user: Optional[str] = None,
        account: Optional[Union[str, Sequence[str]]] = None,
        queue: Optional[str] = None,
        qos: Optional[str] = None,
        exit_status: Optional[str] = None,
        job_id: Optional[str] = None,
        name: Optional[Union[str, Sequence[str]]] = None,
        ignore_case: bool = False,
        min_eligible_secs: Optional[int] = None,
        max_eligible_secs: Optional[int] = None,
        min_nodes: Optional[int] = None,
        max_nodes: Optional[int] = None,
        min_cpus: Optional[int] = None,
        max_cpus: Optional[int] = None,
        min_gpus: Optional[int] = None,
        max_gpus: Optional[int] = None,
        min_elapsed: Optional[int] = None,
        max_elapsed: Optional[int] = None,
        min_reqmem: Optional[int] = None,
        max_reqmem: Optional[int] = None,
        min_memory_used: Optional[int] = None,
        max_memory_used: Optional[int] = None,
        min_memory_wasted: Optional[int] = None,
        max_memory_wasted: Optional[int] = None,
    ) -> int:
        """Count rows that ``jobs_search`` would return under the same filters.

        Companion to :meth:`jobs_search` for paginated UIs: callers fetch
        one page via ``jobs_search(limit=…, offset=…)`` and the total via
        this method. Filter shape mirrors ``jobs_search`` exactly; ``columns``,
        ``limit``, ``offset``, and sort args do not apply. ``account``
        accepts a single projcode or a sequence (see :meth:`jobs_search`);
        ``job_id`` matches the shape-classifier described in
        :meth:`jobs_search`.

        The NULL-exclusion built into ``min_/max_eligible_secs`` applies
        identically here — this method and ``jobs_search`` push the *same*
        predicates through :meth:`_apply_jobs_search_filters`, so there is no
        ``AVG``-skips-NULL / ``COUNT``-doesn't split like the one
        :meth:`job_waits_by_resource` had to guard against. The count is
        always exactly the row count.
        """
        query = self.session.query(func.count(Job.id))
        query = self._apply_jobs_search_filters(
            query, start=start, end=end, user=user, account=account,
            queue=queue, qos=qos, exit_status=exit_status, job_id=job_id,
            name=name, ignore_case=ignore_case,
            min_eligible_secs=min_eligible_secs,
            max_eligible_secs=max_eligible_secs,
            min_nodes=min_nodes, max_nodes=max_nodes,
            min_cpus=min_cpus, max_cpus=max_cpus,
            min_gpus=min_gpus, max_gpus=max_gpus,
            min_elapsed=min_elapsed, max_elapsed=max_elapsed,
            min_reqmem=min_reqmem, max_reqmem=max_reqmem,
            min_memory_used=min_memory_used,
            max_memory_used=max_memory_used,
            min_memory_wasted=min_memory_wasted,
            max_memory_wasted=max_memory_wasted,
        )
        return int(query.scalar() or 0)

    def jobs_facets(
        self,
        *,
        start: Optional[date] = None,
        end: Optional[date] = None,
        user: Optional[str] = None,
        account: Optional[Union[str, Sequence[str]]] = None,
        queue: Optional[str] = None,
        qos: Optional[str] = None,
        exit_status: Optional[str] = None,
        job_id: Optional[str] = None,
        name: Optional[Union[str, Sequence[str]]] = None,
        ignore_case: bool = False,
        min_eligible_secs: Optional[int] = None,
        max_eligible_secs: Optional[int] = None,
        min_nodes: Optional[int] = None,
        max_nodes: Optional[int] = None,
        min_cpus: Optional[int] = None,
        max_cpus: Optional[int] = None,
        min_gpus: Optional[int] = None,
        max_gpus: Optional[int] = None,
        min_elapsed: Optional[int] = None,
        max_elapsed: Optional[int] = None,
        min_reqmem: Optional[int] = None,
        max_reqmem: Optional[int] = None,
        min_memory_used: Optional[int] = None,
        max_memory_used: Optional[int] = None,
        min_memory_wasted: Optional[int] = None,
        max_memory_wasted: Optional[int] = None,
        facets: Sequence[str] = DEFAULT_FACETS,
        self_exclude: bool = True,
        limit: Optional[int] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Per-dimension value counts for the *current* filter set.

        Lets a UI populate filter dropdowns with live counts ("regular
        (1,204)") and grey out zero-count options, instead of the static
        catalog :meth:`list_qos_names` returns. Filter shape mirrors
        :meth:`jobs_search` / :meth:`jobs_count` exactly — the same
        ``_apply_jobs_search_filters`` helper is used, so facets can never
        drift from the rows they describe.

        Returns ``{dimension: [{'value': ..., 'count': int}, ...]}`` with one
        key per entry in *facets*, each list sorted by count desc then value
        asc (``None`` last) so equal counts do not reshuffle between calls.
        ``value`` is ``None`` for a NULL FK. Only values present in the
        filtered slice appear — a zero-count value is simply absent, which is
        what lets the caller grey it out against the catalog.

        Cost — read this before enabling more facets:

        * **One** SQL statement regardless of how many dimensions are
          requested (plus one trivial lookup-table read per name-resolved
          dimension). All dimensions are grouped simultaneously and folded in
          Python. Measured on PostgreSQL over a 304k-row month: 154 ms for
          one dimension, 154 ms for three, 201 ms for five — against 714 ms
          for the same five run as separate queries.
        * Grouping goes through the integer FK, never the text hybrid.
          ``GROUP BY Job.queue`` compiles to a correlated scalar subquery
          emitted twice and re-evaluated per scanned row: measured 1,223 ms
          vs 122 ms for ``GROUP BY Job.queue_id`` on the same slice.
        * The statement scans **every row in the date slice**. This is the
          first method here that does — ``jobs_search(limit=N)`` walks
          ``ix_jobs_end`` and stops at N, and ``jobs_count`` is index-only.
        * The date window is the only cost lever that matters by an order of
          magnitude: unbounded over full history measured ~200 s. Pass a
          bounded ``start``/``end``.

        Args:
            facets: Dimensions to count. Valid keys are in
                :data:`_FACET_SPECS` — queue, qos, exit_status, user,
                account. Defaults to the low-cardinality set.
            self_exclude: When True (default, and the standard faceted-search
                behaviour), a dimension's own filter does not constrain its
                own counts — with ``queue='main'`` set, the ``queue`` facet
                still lists every queue so the user can switch to one, while
                the other facets stay restricted to ``main``. Implemented by
                moving those predicates out of SQL and into the fold, so it
                costs **no extra query**. It does mean the scan is no longer
                narrowed by them, and self-excluding a *selective indexed*
                filter can flip the plan from an index seek to a slice scan.
                ``account`` is never self-excluded (see
                :data:`_FACET_SCOPE_DIMS`).
            limit: Optional per-dimension top-N truncation, applied after
                sorting. The tail is dropped rather than folded into an
                "other" bucket, so rows sum to at most ``jobs_count``, never
                to a synthetic total. Use it for ``user`` / ``exit_status``.

        Raises:
            ValueError: on an unknown facet name or a non-positive ``limit``.
        """
        dims = tuple(facets)
        unknown = [d for d in dims if d not in _FACET_SPECS]
        if unknown:
            valid = ", ".join(sorted(_FACET_SPECS))
            raise ValueError(
                f"Unknown facet(s): {', '.join(unknown)}. Valid facets: {valid}"
            )
        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise ValueError(f"limit must be a positive integer, got {limit!r}")
        if not dims:
            return {}

        filters = dict(
            start=start, end=end, user=user, account=account, queue=queue,
            qos=qos, exit_status=exit_status, job_id=job_id, name=name,
            ignore_case=ignore_case,
            min_eligible_secs=min_eligible_secs,
            max_eligible_secs=max_eligible_secs,
            min_nodes=min_nodes, max_nodes=max_nodes,
            min_cpus=min_cpus, max_cpus=max_cpus,
            min_gpus=min_gpus, max_gpus=max_gpus,
            min_elapsed=min_elapsed, max_elapsed=max_elapsed,
            min_reqmem=min_reqmem, max_reqmem=max_reqmem,
            min_memory_used=min_memory_used,
            max_memory_used=max_memory_used,
            min_memory_wasted=min_memory_wasted,
            max_memory_wasted=max_memory_wasted,
        )
        # Faceted dimensions that carry a filter and are not a security scope:
        # their predicate leaves the WHERE clause and is re-applied in the fold.
        excluded = {
            d for d in dims
            if self_exclude and filters.get(d) and d not in _FACET_SCOPE_DIMS
        }
        sql_filters = {
            k: (None if k in excluded else v) for k, v in filters.items()
        }

        group_cols = [_FACET_SPECS[d][0] for d in dims]
        query = self.session.query(*group_cols, func.count(Job.id))
        query = self._apply_jobs_search_filters(query, **sql_filters)
        rows = query.group_by(*group_cols).all()

        # id -> name, once per dimension over distinct ids (10^1..10^3) rather
        # than once per scanned row (10^6).
        names: Dict[str, Dict[Any, Any]] = {}
        for pos, dim in enumerate(dims):
            _, model, name_col = _FACET_SPECS[dim]
            if model is not None:
                names[dim] = self._resolve_lookup_names(
                    model, name_col, {r[pos] for r in rows if r[pos] is not None}
                )

        # Filter values for the excluded dimensions, as display labels.
        wanted: Dict[str, set] = {}
        for dim in excluded:
            value = filters[dim]
            wanted[dim] = {value} if isinstance(value, str) else set(value)

        buckets: Dict[str, Dict[Any, int]] = {d: {} for d in dims}
        for row in rows:
            count = row[-1]
            labels = [
                names[d].get(row[i]) if d in names else row[i]
                for i, d in enumerate(dims)
            ]
            for i, dim in enumerate(dims):
                # Every *other* excluded dimension's filter still applies.
                if any(labels[j] not in wanted[d]
                       for j, d in enumerate(dims) if d in wanted and d != dim):
                    continue
                buckets[dim][labels[i]] = buckets[dim].get(labels[i], 0) + count

        return {d: _facet_rows(buckets[d], limit) for d in dims}

    def jobs_histogram(
        self,
        dimension: str,
        *,
        start: Optional[date] = None,
        end: Optional[date] = None,
        user: Optional[str] = None,
        account: Optional[Union[str, Sequence[str]]] = None,
        queue: Optional[str] = None,
        qos: Optional[str] = None,
        exit_status: Optional[str] = None,
        job_id: Optional[str] = None,
        name: Optional[Union[str, Sequence[str]]] = None,
        ignore_case: bool = False,
        min_eligible_secs: Optional[int] = None,
        max_eligible_secs: Optional[int] = None,
        min_nodes: Optional[int] = None,
        max_nodes: Optional[int] = None,
        min_cpus: Optional[int] = None,
        max_cpus: Optional[int] = None,
        min_gpus: Optional[int] = None,
        max_gpus: Optional[int] = None,
        min_elapsed: Optional[int] = None,
        max_elapsed: Optional[int] = None,
        min_reqmem: Optional[int] = None,
        max_reqmem: Optional[int] = None,
        min_memory_used: Optional[int] = None,
        max_memory_used: Optional[int] = None,
        min_memory_wasted: Optional[int] = None,
        max_memory_wasted: Optional[int] = None,
        owners_limit: Optional[int] = None,
        owners_sort_by: str = "hours",
        owners_by: str = "user",
    ) -> Dict[str, Any]:
        """Distribution histogram over one job dimension, for dashboards.

        Buckets every job matching the filter set into fixed bands of
        *dimension* and returns the **full band vector in band order,
        zero-count bands included** — a chart gets a stable x-axis without
        knowing which bands are populated. Filter shape mirrors
        :meth:`jobs_search` / :meth:`jobs_count` exactly (same
        ``_apply_jobs_search_filters`` helper), so a histogram can never
        disagree with the job table it sits next to.

        Args:
            dimension: One of ``wait`` (``eligible_secs``), ``nodes``,
                ``cpus``, ``gpus``, ``memory`` (**requested** memory,
                ``reqmem``), ``duration`` (``elapsed``), ``memory_used``
                (peak memory actually consumed, ``Job.memory``) or
                ``memory_wasted`` (the computed ``reqmem - memory`` delta;
                its leading ``over request`` band collects negative deltas
                — jobs that used more than they requested — and rows with
                a NULL in either column land in ``null_count``). Names
                match the SAM dashboard vocabulary, not column names.
            owners_limit: When set, each bucket additionally carries an
                ``owners`` mapping — its top-N owners (users by default;
                see *owners_by*). ``None`` (the default) leaves the
                envelope as before.
            owners_by: Which entity "owns" a bucket's jobs for the
                ``owners`` breakdown: ``'user'`` (the default — keys are
                usernames) or ``'account'`` (keys are account/project
                codes). Same aggregate shape either way — only the GROUP
                BY key and the name lookup change. Ignored when
                *owners_limit* is None.
            owners_sort_by: Which metric decides *which* N owners survive:
                ``'hours'`` (the default, ``cpu_hours + gpu_hours``),
                ``'cpu_hours'``, ``'gpu_hours'``, or ``'job_count'`` — the
                same vocabulary as :meth:`jobs_usage_by`. A consumer
                stacking a bar by GPU hours must pass ``'gpu_hours'``, for
                the reason spelled out there: ranked by combined hours, the
                top-5 owners of a ``wait`` or ``duration`` band cover ~0% of
                that band's GPU hours on derecho, so every bar renders as
                "Other". Ignored when *owners_limit* is None.

        Returns::

            {
              "dimension": "wait",
              "column": "eligible_secs",
              "unit": "seconds",                # 'seconds' | 'count' | 'bytes'
              "min_param": "min_eligible_secs", # jobs_search kwargs that
              "max_param": "max_eligible_secs", #   replay one band as filters
              "buckets": [
                {"label": "<1m", "lo": 0, "hi": 59,
                 "job_count": 12, "cpu_hours": 190.0, "gpu_hours": 0.0,
                 "cpu_charges": 199.5, "gpu_charges": 0.0},
                ...                             # full vector, band order
              ],
              "null_count": 2,                  # filters matched, column NULL
              "total_count": 14,                # Σ job_count + null_count
            }

        Every band (and every ``owners`` entry) carries the full
        :data:`_METRIC_KEYS` vector: raw hours *and* their QoS-weighted
        ``cpu_charges``/``gpu_charges`` (``hours x qos_factor``, see
        :func:`_charge_expr`). **Charges are not proportional to hours** —
        ``qos_factor`` is a real 0.0 for the ``uncharged`` QoS, so a band can
        legitimately report hours > 0 with charges == 0.

        ``lo``/``hi`` are inclusive native-unit bounds. Either may be None
        where the band is open-ended: always ``hi`` on the last band, and
        ``lo`` on ``memory_wasted``'s leading ``over request`` band, the one
        dimension unbounded below. Replaying a band is
        ``jobs_search(**{min_param: lo, max_param: hi}, ...)``, dropping
        whichever bound is None (passing it through as None is equivalent —
        the filters treat None as unset). Every band replays to exactly its
        own ``job_count``, and ``total_count == jobs_count(**filters)``;
        both are pinned by tests.

        ``null_count`` is the "N jobs unmeasured" story: rows that match
        the filters but have a NULL *dimension* column land there, never in
        a band. In practice only ``wait`` has them — derecho did not record
        ``eligible_time`` before 2025-01-07 17:47:50 UTC (casper history is
        complete), and ``numnodes``/``numcpus``/``numgpus``/``name`` are
        NULL-free across 34M production rows.

        There is no ``self_exclude`` and no ``limit``: filters mean exactly
        what they say. A caller who wants the histogram unconstrained by its
        own dimension's bounds simply omits those kwargs.

        With ``owners_limit=N`` every bucket dict gains an appended
        ``owners`` key::

            "owners": {"alice": {"job_count": 8,
                                 "cpu_hours": 120.0, "gpu_hours": 4.0},
                       ...}    # top-N, insertion-ordered by rank

        Owners are ranked per bucket by *owners_sort_by* (descending; name
        asc tie-break, the facet convention). With ``owners_by='account'``
        the keys are account codes instead of usernames; everything below
        reads the same with "account" substituted. Bucket totals stay
        authoritative: the long tail beyond N — and any rows with a NULL
        owner FK — is exactly ``bucket totals − Σ owners``, derivable,
        never synthesized. Zero-count buckets carry ``owners: {}``. The
        NULL *dimension* band still folds into ``null_count`` and never
        carries owners.

        Every listed owner replays exactly: ``jobs_count(user=name,
        **{min_param: lo, max_param: hi}, **filters)`` — or
        ``account=name`` under ``owners_by='account'`` — equals that
        owner's ``job_count``, the third level of a bin → owner → jobs
        drill-down. Pinned by ``test_owners_round_trip_into_filters``.

        Still one aggregate statement — the GROUP BY grows the owner FK
        key (the 1:1 charge join keeps COUNT/SUM exact) plus the usual
        post-aggregation name lookup — but not the same cost: the group
        cardinality goes from one row per bucket to buckets × distinct
        users, and the Python fold below grows with it. Measured ~1.4× on
        derecho over a one-month window (``cpus`` 761 → 1085 ms, ``wait``
        1202 → 1647 ms).

        Every ``job_count`` — per bucket, ``null_count``, ``total_count`` —
        is identical to the ``owners_limit=None`` envelope. The hour sums
        are equal only to within float rounding: this path adds N per-user
        partial ``SUM``s in Python instead of taking one SQL ``SUM``, and
        float addition is not associative, so they can differ in the last
        ULP (~1e-7 relative, observed on every dimension against real
        data). Compare hours with a tolerance, never ``==``.

        Cost: one aggregate statement — a CASE label + COUNT + two SUMs over
        a LEFT OUTER JOIN to ``job_charges`` (1:1 on the PK), grouped by the
        label. Like :meth:`jobs_facets` it scans every row in the date
        slice, so the window is the cost lever: always pass ``start``/
        ``end`` (facets measured ~200 s unbounded over full history).
        Measured on PostgreSQL over a 308k-row month, machine-wide:
        ~570 ms warm regardless of dimension, against ~150 ms for
        ``jobs_facets`` on the same slice — the charge join is the
        difference. Hours are raw ``cpu_hours``/``gpu_hours`` (not
        QoS-weighted charges), matching :meth:`job_sizes_by_resource`.

        Raises:
            ValueError: on an unknown *dimension*, *owners_sort_by*, or
                *owners_by*, or a non-positive *owners_limit*.
        """
        spec = _HISTOGRAM_SPECS.get(dimension)
        if spec is None:
            valid = ", ".join(sorted(_HISTOGRAM_SPECS))
            raise ValueError(
                f"Unknown dimension: {dimension!r}. Valid dimensions: {valid}"
            )
        if owners_limit is not None and (
            not isinstance(owners_limit, int) or owners_limit <= 0
        ):
            raise ValueError(
                f"owners_limit must be a positive integer, got {owners_limit!r}"
            )
        _check_usage_sort_key(owners_sort_by, "owners_sort_by")
        if owners_by not in ("user", "account"):
            raise ValueError(
                f"owners_by must be 'user' or 'account', got {owners_by!r}"
            )
        owner_fk, owner_model, owner_name_col = _FACET_SPECS[owners_by]
        column, buckets, unit, min_param, max_param = spec

        bucket_label = _bucket_case(column, buckets)
        select_cols = [bucket_label, *_metric_agg_cols()]
        group_cols = [bucket_label]
        if owners_limit is not None:
            # Group the integer FK, never the text hybrid — see
            # _FACET_SPECS. The 1:1 charge join keeps every aggregate exact
            # under the extra key.
            select_cols.insert(1, owner_fk)
            group_cols.append(owner_fk)
        query = (
            self.session.query(*select_cols)
            .outerjoin(JobCharge, Job.id == JobCharge.job_id)
        )
        query = self._apply_jobs_search_filters(
            query, start=start, end=end, user=user, account=account,
            queue=queue, qos=qos, exit_status=exit_status, job_id=job_id,
            name=name, ignore_case=ignore_case,
            min_eligible_secs=min_eligible_secs,
            max_eligible_secs=max_eligible_secs,
            min_nodes=min_nodes, max_nodes=max_nodes,
            min_cpus=min_cpus, max_cpus=max_cpus,
            min_gpus=min_gpus, max_gpus=max_gpus,
            min_elapsed=min_elapsed, max_elapsed=max_elapsed,
            min_reqmem=min_reqmem, max_reqmem=max_reqmem,
            min_memory_used=min_memory_used,
            max_memory_used=max_memory_used,
            min_memory_wasted=min_memory_wasted,
            max_memory_wasted=max_memory_wasted,
        )
        rows = query.group_by(*group_cols).all()

        # Zero-fill from the spec table, in order; SUM over an all-NULL
        # charge group is NULL, hence the `or 0.0`. The NULL band routes to
        # null_count — its hour sums are deliberately dropped (unmeasured
        # rows are excluded from the distribution, not smeared into it).
        # One fold for both shapes: with owners the bucket totals are
        # re-folded from the per-owner rows (identical arithmetic to the
        # ungrouped shape) and the per-owner split is kept on the side.
        # NULL-owner rows count toward totals but are never owner
        # candidates — they live in the derivable remainder.
        owners_by_label: Dict[str, Dict[Any, List[float]]] = {}
        by_label: Dict[str, List[float]] = {}
        names: Dict[Any, Any] = {}
        null_count = 0
        for row in rows:
            if owners_limit is None:
                label, owner_id, metric_cols = row[0], None, row[1:]
            else:
                label, owner_id, metric_cols = row[0], row[1], row[2:]
            if label == _NULL_BUCKET:
                null_count += int(metric_cols[0])
                continue
            _accumulate(by_label.setdefault(label, _zero_metrics()), *metric_cols)
            if owner_id is not None:
                per_owner = owners_by_label.setdefault(label, {})
                _accumulate(per_owner.setdefault(owner_id, _zero_metrics()),
                            *metric_cols)
        if owners_limit is not None:
            names = self._resolve_lookup_names(
                owner_model, owner_name_col,
                {oid for per_owner in owners_by_label.values() for oid in per_owner},
            )

        out_buckets = []
        for label, lo, hi in buckets:
            bucket = {
                "label": label,
                "lo": lo,
                "hi": hi,
                **_metrics_dict(by_label.get(label, _zero_metrics())),
            }
            if owners_limit is not None:
                # Rank by the caller's metric desc, then name asc (the facet
                # tie-break, minus the None arm — NULL owners never rank). The
                # display name is resolved once and used for both the tie-break
                # and the output key, so an id the lookup can't resolve sorts
                # and renders under the same string.
                named = [
                    (str(names.get(oid, oid)), _metrics_dict(agg))
                    for oid, agg in owners_by_label.get(label, {}).items()
                ]
                ranked = sorted(named, key=lambda na: (
                    -_usage_rank(owners_sort_by, na[1]),
                    na[0],
                ))[:owners_limit]
                bucket["owners"] = dict(ranked)
            out_buckets.append(bucket)

        total = sum(b["job_count"] for b in out_buckets) + null_count
        return {
            "dimension": dimension,
            "column": column.key,
            "unit": unit,
            "min_param": min_param,
            "max_param": max_param,
            "buckets": out_buckets,
            "null_count": null_count,
            "total_count": total,
        }

    def jobs_usage_by(
        self,
        dimension: str,
        *,
        start: Optional[date] = None,
        end: Optional[date] = None,
        user: Optional[str] = None,
        account: Optional[Union[str, Sequence[str]]] = None,
        queue: Optional[str] = None,
        qos: Optional[str] = None,
        exit_status: Optional[str] = None,
        job_id: Optional[str] = None,
        name: Optional[Union[str, Sequence[str]]] = None,
        ignore_case: bool = False,
        min_eligible_secs: Optional[int] = None,
        max_eligible_secs: Optional[int] = None,
        min_nodes: Optional[int] = None,
        max_nodes: Optional[int] = None,
        min_cpus: Optional[int] = None,
        max_cpus: Optional[int] = None,
        min_gpus: Optional[int] = None,
        max_gpus: Optional[int] = None,
        min_elapsed: Optional[int] = None,
        max_elapsed: Optional[int] = None,
        min_reqmem: Optional[int] = None,
        max_reqmem: Optional[int] = None,
        min_memory_used: Optional[int] = None,
        max_memory_used: Optional[int] = None,
        min_memory_wasted: Optional[int] = None,
        max_memory_wasted: Optional[int] = None,
        limit: Optional[int] = None,
        sort_by: str = "hours",
    ) -> Dict[str, Any]:
        """Per-entity usage (job count + hours) for one dimension.

        The data behind a "By User" usage pie: for each distinct value of
        *dimension* within the filtered slice, the job count and summed raw
        ``cpu_hours``/``gpu_hours``. Filter shape mirrors
        :meth:`jobs_search` / :meth:`jobs_count` exactly (same helper).

        This is deliberately **not** ``jobs_facets(include_hours=True)``:
        facet self-exclusion is wrong for usage (hours attributed under a
        self-excluded dimension would describe rows the current filters
        exclude), and there is **no self-exclusion of any kind here** —
        every filter, ``account`` included, always applies. That is the
        same security property :data:`_FACET_SCOPE_DIMS` protects: SAM pins
        ``account`` as its authorization scope, and this method can never
        emit usage for projects outside it.

        Args:
            dimension: Any :data:`_FACET_SPECS` key — ``user`` (the pie
                case), ``account``, ``queue``, ``qos``, ``exit_status``.
            limit: Optional top-N truncation, applied after sorting. The
                tail is dropped with no synthetic "other" row, but
                ``totals`` is computed **before** truncation — so a
                consumer's "Other" slice is exactly
                ``totals − Σ returned rows``, an invariant rather than a
                guess.
            sort_by: The ranking metric — and therefore *which* top-N
                survives ``limit``: ``'hours'`` (the default,
                ``cpu_hours + gpu_hours``), ``'cpu_hours'``,
                ``'gpu_hours'``, or ``'job_count'``. A consumer showing a
                GPU-hours view must rank by ``'gpu_hours'``, or a pure-GPU
                user can vanish beneath ``limit`` CPU-heavy ones. Same
                vocabulary as :meth:`jobs_histogram`'s ``owners_sort_by``
                — and *not* the same as :meth:`jobs_search`'s ``sort_by``,
                which takes ``COLUMNS`` keys.

        Returns::

            {
              "dimension": "user",
              "rows": [   # sort_by metric desc, value asc, None last
                {"value": "alice", "job_count": 812,
                 "cpu_hours": 91234.5, "gpu_hours": 120.0,
                 "cpu_charges": 96000.1, "gpu_charges": 180.0},
                ...
              ],
              "totals": {"job_count": 40213,
                         "cpu_hours": 5432100.0, "gpu_hours": 21000.0,
                         "cpu_charges": 5698000.0, "gpu_charges": 31500.0},
            }

        ``value`` is ``None`` for a NULL FK (kept, not dropped — dropping
        would make rows under-sum against ``totals``). Every row carries the
        full :data:`_METRIC_KEYS` vector: raw ``cpu_hours``/``gpu_hours``
        *and* their QoS-weighted ``cpu_charges``/``gpu_charges``
        (``hours x qos_factor``, see :func:`_charge_expr`). ``memory_hours``
        is omitted until a consumer needs it. ``totals["job_count"]`` equals
        :meth:`jobs_count` under the same filters by construction.

        **Charges are not proportional to hours.** ``qos_factor`` is a real
        0.0 for the ``uncharged`` QoS, so a slice can legitimately report
        hours > 0 with charges == 0. A consumer offering a charges metric
        must not present an empty bar as "no activity".

        Cost: one aggregate statement — grouped on the integer FK (never
        the text hybrid; see :data:`_FACET_SPECS`), COUNT + four SUMs over a
        LEFT OUTER JOIN to ``job_charges``, names resolved after
        aggregation. Scans every row in the date slice, so always pass a
        bounded ``start``/``end``. Measured on PostgreSQL over a 308k-row
        month, machine-wide: ~545 ms warm (~150 ms for ``jobs_facets`` on
        the same slice — the charge join is the difference).

        Raises:
            ValueError: on an unknown *dimension*, non-positive *limit*, or
                unknown *sort_by*.
        """
        spec = _FACET_SPECS.get(dimension)
        if spec is None:
            valid = ", ".join(sorted(_FACET_SPECS))
            raise ValueError(
                f"Unknown dimension: {dimension!r}. Valid dimensions: {valid}"
            )
        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise ValueError(f"limit must be a positive integer, got {limit!r}")
        _check_usage_sort_key(sort_by, "sort_by")
        group_col, model, name_col = spec

        query = (
            self.session.query(group_col, *_metric_agg_cols())
            .outerjoin(JobCharge, Job.id == JobCharge.job_id)
        )
        query = self._apply_jobs_search_filters(
            query, start=start, end=end, user=user, account=account,
            queue=queue, qos=qos, exit_status=exit_status, job_id=job_id,
            name=name, ignore_case=ignore_case,
            min_eligible_secs=min_eligible_secs,
            max_eligible_secs=max_eligible_secs,
            min_nodes=min_nodes, max_nodes=max_nodes,
            min_cpus=min_cpus, max_cpus=max_cpus,
            min_gpus=min_gpus, max_gpus=max_gpus,
            min_elapsed=min_elapsed, max_elapsed=max_elapsed,
            min_reqmem=min_reqmem, max_reqmem=max_reqmem,
            min_memory_used=min_memory_used,
            max_memory_used=max_memory_used,
            min_memory_wasted=min_memory_wasted,
            max_memory_wasted=max_memory_wasted,
        )
        raw = query.group_by(group_col).all()

        names = {}
        if model is not None:
            names = self._resolve_lookup_names(
                model, name_col, {r[0] for r in raw if r[0] is not None}
            )

        rows = []
        for key, *metric_cols in raw:
            value = names.get(key) if model is not None else key
            acc = _zero_metrics()
            _accumulate(acc, *metric_cols)
            rows.append({"value": value, **_metrics_dict(acc)})
        # sort_by metric desc, then value asc with None last — the facet
        # tie-break convention, keyed on the caller's ranking metric.
        rows.sort(key=lambda r: (
            -_usage_rank(sort_by, r),
            r["value"] is None,
            str(r["value"]),
        ))

        totals = {key: sum(r[key] for r in rows) for key in _METRIC_KEYS}
        if limit is not None:
            rows = rows[:limit]

        return {"dimension": dimension, "rows": rows, "totals": totals}

    def jobs_timeseries(
        self,
        period: str = "day",
        *,
        start: Optional[date] = None,
        end: Optional[date] = None,
        user: Optional[str] = None,
        account: Optional[Union[str, Sequence[str]]] = None,
        queue: Optional[str] = None,
        qos: Optional[str] = None,
        exit_status: Optional[str] = None,
        job_id: Optional[str] = None,
        name: Optional[Union[str, Sequence[str]]] = None,
        ignore_case: bool = False,
        min_eligible_secs: Optional[int] = None,
        max_eligible_secs: Optional[int] = None,
        min_nodes: Optional[int] = None,
        max_nodes: Optional[int] = None,
        min_cpus: Optional[int] = None,
        max_cpus: Optional[int] = None,
        min_gpus: Optional[int] = None,
        max_gpus: Optional[int] = None,
        min_elapsed: Optional[int] = None,
        max_elapsed: Optional[int] = None,
        min_reqmem: Optional[int] = None,
        max_reqmem: Optional[int] = None,
        min_memory_used: Optional[int] = None,
        max_memory_used: Optional[int] = None,
        min_memory_wasted: Optional[int] = None,
        max_memory_wasted: Optional[int] = None,
        owners_limit: Optional[int] = None,
        owners_sort_by: str = "hours",
        owners_by: str = "user",
    ) -> Dict[str, Any]:
        """Per-period activity series, for a stacked time-series chart.

        Buckets every job matching the filter set into contiguous calendar
        bands of *period* and returns the **full zero-filled band vector in
        chronological order**. Filter shape mirrors :meth:`jobs_search` /
        :meth:`jobs_count` exactly (same ``_apply_jobs_search_filters``
        helper), so a timeline can never disagree with the job table it sits
        next to.

        This is the time axis :meth:`jobs_histogram` deliberately does not
        offer — and the only per-period method here that honours the filter
        set. :meth:`usage_history`, :meth:`jobs_by_entity_period` and
        :meth:`daily_summary_report` all take **dates only**, so a chart
        built on them would silently ignore queue / size / exit-status
        filters.

        Args:
            period: ``'day'``, ``'week'`` (ISO, Monday-start) or
                ``'month'``. Bands are **site-local** calendar periods (see
                :func:`_period_bands`), matching ``_apply_date_filter`` and
                the ``DailySummary`` binning.
            owners_limit: When set, every band additionally carries an
                ``owners`` mapping — the window's top-N owners. Unlike
                :meth:`jobs_histogram`, whose top-N is computed *per band*,
                the set here is ranked **once over the whole window** and
                every band carries the same N keys in the same rank order,
                zero-filled where an owner was idle. A stacked bar chart
                needs exactly that: one legend, and a colour that means the
                same owner in every bar.
            owners_by: ``'user'`` (default) or ``'account'``.
            owners_sort_by: Which metric decides *which* N owners survive —
                a :data:`_USAGE_SORT_KEYS` member. Must follow the displayed
                metric, for the reason spelled out in :meth:`jobs_usage_by`.

        Returns::

            {
              "period": "day",
              "owners_by": "user",
              "start": "2026-05-01",      # resolved window (see below)
              "end":   "2026-05-31",
              "bands": [                  # full vector, chronological
                {"label": "2026-05-01",
                 "start": "2026-05-01", "end": "2026-05-01",
                 "job_count": 812, "cpu_hours": 91234.5, "gpu_hours": 120.0,
                 "cpu_charges": 96000.1, "gpu_charges": 180.0,
                 "owners": {"alice": {...}}},   # top-N, global rank order
                ...
              ],
              "totals": {...},            # Σ over every band
              "null_count": 0,            # filters matched, Job.end NULL
              "total_count": 24680,       # Σ band job_count + null_count
            }

        **Replaying a band is ``jobs_search(start=band['start'],
        end=band['end'], **filters)``** — the window filters *are* this
        dimension, so there is no ``min_param``/``max_param`` to echo. Bands
        are clipped to the window, so every band replays to exactly its own
        ``job_count``, and ``total_count == jobs_count(**filters)``; both
        are pinned by tests.

        ``start``/``end`` echo the **resolved** window, and both statements
        are re-bound to it, so the band ladder's totality is a property of
        the WHERE clause rather than of the probe's snapshot. When the caller
        supplies neither bound (an explicit opt-in to full history), the
        domain is derived from ``MIN``/``MAX`` of ``Job.end`` under the same
        filters, because a band vector needs a domain. That probe is **not**
        cheap under a real filter set — see
        :meth:`_resolve_timeseries_window` — so pass explicit bounds when you
        can. A window with no banded rows yields no bands but still echoes
        whichever bound the caller supplied.

        ``null_count`` collects rows matching the filters whose ``Job.end``
        is NULL; they belong to no calendar band. It rides out on the same
        probe, so it survives the re-bounding that would otherwise drop those
        rows, and ``total_count == jobs_count(**filters)`` holds even for a
        slice whose rows are *all* NULL-``end``. A caller-bounded window
        excludes them by its own date filter, so this is non-zero only for an
        unbounded query.

        The ``owners`` remainder is derivable, never synthesized: a band's
        "Others" is exactly ``band totals − Σ band owners``. Rows whose
        owner FK is NULL, and every owner outside the global top-N, fold
        into that remainder by construction — SQL maps both to the same NULL
        group key.

        Cost: served off ``daily_summary`` when the filter set and the
        window allow it (see :meth:`_timeseries_uses_summary`) — measured
        **9 ms** against **7390 ms** for a 180d daily series on a 21.0M-job
        PostgreSQL, agreeing to the last digit. Otherwise it scans ``jobs``:
        **two aggregate statements** when *owners_limit* is set (one to rank
        owners over the window, one for the series), **one** otherwise, plus
        the MIN/MAX probe when the window is derived. Either way the envelope
        is identical — consumers cannot tell which path ran. On the scan path
        the band ladder costs ~O(bands/2) comparisons per scanned row, which
        is ~10 % at 180 bands and ~65 % at 730, hence
        :data:`_MAX_TIMESERIES_BANDS`; the fast path has no ladder and gets
        the looser :data:`_MAX_SUMMARY_BANDS`.

        Raises:
            ValueError: unknown *period*, non-positive *owners_limit*,
                unknown *owners_by* / *owners_sort_by*, or a window whose
                band count exceeds the cap for the path it routed to.
        """
        if period not in _TIMESERIES_PERIODS:
            valid = ", ".join(_TIMESERIES_PERIODS)
            raise ValueError(
                f"Unknown period: {period!r}. Valid periods: {valid}")
        if owners_limit is not None and (
                not isinstance(owners_limit, int) or owners_limit <= 0):
            raise ValueError(
                f"owners_limit must be a positive integer, got {owners_limit!r}")
        _check_usage_sort_key(owners_sort_by, "owners_sort_by")
        if owners_by not in ("user", "account"):
            raise ValueError(
                f"Unknown owners_by: {owners_by!r}. Valid values: user, account")
        owner_fk, owner_model, owner_name_col = _FACET_SPECS[owners_by]

        filters = dict(
            start=start, end=end, user=user, account=account,
            queue=queue, qos=qos, exit_status=exit_status, job_id=job_id,
            name=name, ignore_case=ignore_case,
            min_eligible_secs=min_eligible_secs,
            max_eligible_secs=max_eligible_secs,
            min_nodes=min_nodes, max_nodes=max_nodes,
            min_cpus=min_cpus, max_cpus=max_cpus,
            min_gpus=min_gpus, max_gpus=max_gpus,
            min_elapsed=min_elapsed, max_elapsed=max_elapsed,
            min_reqmem=min_reqmem, max_reqmem=max_reqmem,
            min_memory_used=min_memory_used,
            max_memory_used=max_memory_used,
            min_memory_wasted=min_memory_wasted,
            max_memory_wasted=max_memory_wasted,
        )

        win_start, win_end, null_count = self._resolve_timeseries_window(
            start, end, filters)

        def _empty(win_lo, win_hi):
            # NULL-``end`` rows match the filters but belong to no calendar
            # band, so they still count toward total_count.
            return {
                "period": period, "owners_by": owners_by,
                "start": win_lo.isoformat() if win_lo is not None else None,
                "end": win_hi.isoformat() if win_hi is not None else None,
                "bands": [],
                "totals": _metrics_dict(_zero_metrics()),
                "null_count": null_count, "total_count": null_count,
            }

        if win_start is None:
            # Nothing landed in a band, and no derivable domain to zero-fill
            # over. Echo whichever bound the caller DID supply rather than
            # discarding it — only a missing bound reaches the probe.
            return _empty(start, end)

        bands = _period_bands(win_start, win_end, period)
        if not bands:
            # Inverted window (start > end). ``jobs_count``/``jobs_search``
            # answer 0 rather than raising, so this does too — and returning
            # early also keeps _period_case from indexing an empty ladder.
            return _empty(win_start, win_end)

        # Route BEFORE capping: the two paths cost ~700x differently, so they
        # do not share a band budget. Routing needs only the filter set and
        # the resolved window, both settled by now.
        via_summary = self._timeseries_uses_summary(
            filters, win_start, win_end, owners_by)
        cap = _MAX_SUMMARY_BANDS if via_summary else _MAX_TIMESERIES_BANDS
        if len(bands) > cap:
            raise ValueError(
                f"{win_start}..{win_end} at period={period!r} needs "
                f"{len(bands)} bands, over the {cap} cap "
                f"({'daily_summary' if via_summary else 'jobs scan'} path). "
                f"Use a coarser period or a narrower window.")

        # Re-bind BOTH statements to the resolved window. Left unbounded (as
        # they are whenever a bound was derived) two things go wrong:
        # _period_case's totality would rest on the probe's snapshot rather
        # than on the WHERE clause — a job synced between the probe and the
        # series lands in the ``else_`` arm and silently breaks that band's
        # ``jobs_search`` replay — and the rank statement and the series
        # statement could disagree about a late-arriving row under READ
        # COMMITTED. ``null_count`` already came off the probe, so bounding
        # here costs no information.
        filters["start"], filters["end"] = win_start, win_end

        if via_summary:
            return self._timeseries_from_summary(
                bands, filters, period, owners_by, owners_limit,
                owners_sort_by, win_start, win_end, null_count)

        # Statement 1 (owners only): rank owners ONCE over the whole window,
        # so the band vector shares one legend and one colour assignment.
        top_ids: List[Any] = []
        top_names: Dict[Any, str] = {}
        if owners_limit is not None:
            rank_q = (
                self.session.query(owner_fk, *_metric_agg_cols())
                .outerjoin(JobCharge, Job.id == JobCharge.job_id)
            )
            rank_q = self._apply_jobs_search_filters(rank_q, **filters)
            candidates = {}
            for owner_id, *metric_cols in rank_q.group_by(owner_fk).all():
                if owner_id is None:
                    continue  # NULL owners live in the derivable remainder
                acc = _zero_metrics()
                _accumulate(acc, *metric_cols)
                candidates[owner_id] = _metrics_dict(acc)
            names = self._resolve_lookup_names(
                owner_model, owner_name_col, set(candidates))
            ranked = sorted(
                ((str(names.get(oid, oid)), oid, m)
                 for oid, m in candidates.items()),
                key=lambda t: (-_usage_rank(owners_sort_by, t[2]), t[0]),
            )[:owners_limit]
            top_ids = [oid for _n, oid, _m in ranked]
            top_names = {oid: nm for nm, oid, _m in ranked}

        # Statement 2: the series. Non-top-N owners collapse to a NULL group
        # key in SQL — same bucket as a genuinely NULL owner FK, and both
        # belong in "Others" — so band totals stay exact with no extra pass.
        # No surviving candidates (an empty slice) means no owner axis to
        # group by — an empty IN-list is both a SQLAlchemy warning and a
        # pointless extra group key.
        with_owners = owners_limit is not None and bool(top_ids)
        period_label = _period_case(Job.end, bands)
        select_cols: List[Any] = [period_label, *_metric_agg_cols()]
        group_cols: List[Any] = [period_label]
        if with_owners:
            owner_key = case((owner_fk.in_(top_ids), owner_fk)).label("owner_key")
            select_cols.insert(1, owner_key)
            group_cols.append(owner_key)
        query = (
            self.session.query(*select_cols)
            .outerjoin(JobCharge, Job.id == JobCharge.job_id)
        )
        query = self._apply_jobs_search_filters(query, **filters)
        rows = query.group_by(*group_cols).all()

        by_label: Dict[str, List[float]] = {}
        owners_by_label: Dict[str, Dict[Any, List[float]]] = {}
        for row in rows:
            if with_owners:
                label, owner_id, metric_cols = row[0], row[1], row[2:]
            else:
                label, owner_id, metric_cols = row[0], None, row[1:]
            if label == _NULL_BUCKET:
                # Unreachable now that the statement is bounded — a date
                # filter drops NULL ``end``, and ``null_count`` comes off the
                # probe instead. Kept because _period_case is total on its own
                # terms and counting beats silently dropping if that changes.
                null_count += int(metric_cols[0])
                continue
            _accumulate(by_label.setdefault(label, _zero_metrics()), *metric_cols)
            if owner_id is not None:
                per_owner = owners_by_label.setdefault(label, {})
                _accumulate(per_owner.setdefault(owner_id, _zero_metrics()),
                            *metric_cols)

        out_bands = []
        for band in bands:
            out = {
                "label": band["label"],
                "start": band["start"].isoformat(),
                "end": band["end"].isoformat(),
                **_metrics_dict(by_label.get(band["label"], _zero_metrics())),
            }
            if owners_limit is not None:
                # EVERY band carries the SAME top-N keys, in global rank
                # order, zero-filled where an owner was idle — the band
                # vector's zero-fill contract applied to the owner axis. A
                # stacked chart can map key -> colour once and trust that a
                # series never changes position or disappears mid-axis.
                # (jobs_histogram differs on purpose: its top-N is per-band,
                # so absent owners have no meaning there.)
                per_owner = owners_by_label.get(band["label"], {})
                out["owners"] = {
                    top_names[oid]: _metrics_dict(
                        per_owner.get(oid, _zero_metrics()))
                    for oid in top_ids
                }
            out_bands.append(out)

        totals = {key: sum(b[key] for b in out_bands) for key in _METRIC_KEYS}
        return {
            "period": period,
            "owners_by": owners_by,
            "start": win_start.isoformat(),
            "end": win_end.isoformat(),
            "bands": out_bands,
            "totals": totals,
            "null_count": null_count,
            "total_count": totals["job_count"] + null_count,
        }

    def _timeseries_uses_summary(self, filters, win_start, win_end, owners_by):
        """Can ``daily_summary`` answer this series, or must we scan ``jobs``?

        The fast path is worth a lot — measured on PG 18 / casper_jobs
        (21.0M jobs), a 180d daily series costs **9 ms** off the summary
        against **7390 ms** scanning ``jobs``, agreeing to the last digit on
        ``job_count``, ``cpu_hours`` and ``cpu_charges``; 730 daily bands are
        21 ms against 14699 ms. It is the same fast/slow split ``fs_scans``
        uses for its precomputed histograms.

        Two independent conditions, both required:

        **Expressible filters.** ``daily_summary``'s unique key is
        ``(date, user_id, account_id, queue_id)``; every other filter is a
        per-job attribute the rollup aggregated away. See
        :data:`_SUMMARY_SERVICEABLE_FILTERS`.

        **Complete coverage.** Every day in the window must actually be
        summarized. A trailing-watermark test is not enough: the summary lags
        ``jobs`` at the top *and* need not reach back to the beginning of
        history (a partial ``--resummarize`` leaves an earlier gap), and a
        single skipped day mid-window would come back as a zero band while
        the scan path finds jobs. :func:`~job_history.sync.summary.generate_daily_summary`
        writes a NO_JOBS marker row for a day with no jobs, so every
        *processed* day has at least one row and ``COUNT(DISTINCT date)``
        against the window's width is an exact check — one aggregate on an
        indexed column of a table three orders of magnitude smaller than
        ``jobs``.

        Anything short of full coverage falls back for the **whole** window.
        There is deliberately no hybrid (summary for the covered days, live
        scan for the rest), because two code paths contributing to one band
        vector is exactly where a silent double-count would live.

        .. warning::

           This inherits ``daily_summary``'s coverage, which is **not** a
           faithful rollup of ``jobs``: :mod:`sync.summary` skips rows whose
           ``user_id`` / ``account_id`` / ``queue_id`` is NULL (summary.py
           ``AND j.* IS NOT NULL``), so where such rows exist the fast path
           under-counts against the scan path. The coverage check above
           cannot detect it — those days are summarized, just incompletely.
           It is currently impossible in production (0 of 21.0M rows on
           casper_jobs) and it is the same bar :meth:`daily_summary_report`
           already ships at, but the exclusion is deliberate rather than
           incidental: a NULL FK triple is already the NO_JOBS marker, so
           such a job cannot be stored without becoming indistinguishable
           from one. Pinned by ``TestTimeseriesSummaryCoverageLimit``. If it
           ever stops holding, the fix belongs in :mod:`sync.summary`.

           Chargeless jobs used to be a second divergence — the rollup
           inner-joined ``job_charges`` — but that is fixed: summary.py now
           LEFT joins, so such a job counts with zero hours on both paths.

        Logs the routing decision at DEBUG on every call, since the two paths
        are deliberately indistinguishable from the envelope and a 7.4 s
        timeline should not look identical to a 15 ms one from the outside.
        """
        # `ignore_case` only modifies `name` matching and defaults to False;
        # every other filter is None when unset. `0` is a meaningful bound,
        # so test against None rather than truthiness.
        active = {k for k, v in filters.items() if v is not None and v is not False}
        unserviceable = active - _SUMMARY_SERVICEABLE_FILTERS
        if unserviceable or owners_by not in ("user", "account"):
            logger.debug(
                "jobs_timeseries %s..%s: jobs-scan path (not expressible in "
                "the rollup: %s)", win_start, win_end,
                ", ".join(sorted(unserviceable)) or f"owners_by={owners_by!r}")
            return False
        needed = (win_end - win_start).days + 1
        covered = self.session.query(
            func.count(distinct(DailySummary.date))
        ).filter(
            DailySummary.date >= win_start, DailySummary.date <= win_end
        ).scalar() or 0
        if covered == needed:
            logger.debug(
                "jobs_timeseries %s..%s: daily_summary path (%d/%d days "
                "covered)", win_start, win_end, covered, needed)
            return True
        # Most often the window simply ends today and sync has not written
        # today's row yet: `SyncBase` only summarizes a day it actually
        # inserted jobs for (`should_summarize`), so between site-local
        # midnight and the first job ending that day there is no row for it.
        logger.debug(
            "jobs_timeseries %s..%s: jobs-scan path (coverage %d/%d days)",
            win_start, win_end, covered, needed)
        return False

    def _timeseries_from_summary(self, bands, filters, period, owners_by,
                                 owners_limit, owners_sort_by,
                                 win_start, win_end, null_count):
        """:meth:`jobs_timeseries` served off ``daily_summary``.

        Returns the same envelope as the ``jobs``-scan path — same band
        vector, same :data:`_METRIC_KEYS`, same owner contract and rank
        order — so a consumer cannot tell which path ran. Pinned by the
        equivalence tests rather than by inspection.

        Counts and structure are **exact**: measured across 11 shapes on
        casper_jobs (21.0M jobs), every ``job_count``, band label, window
        echo and owner key matched the scan path bit for bit. The float
        metrics agree to ~1e-13 relative but not bit for bit, because
        summation order differs — the rollup pre-sums each
        ``(date, user, account, queue)`` group and this re-folds those
        subtotals, where the scan sums every job in one pass, and float
        addition is not associative. Fewer accumulation steps means the fast
        path's sums are, if anything, marginally the more accurate of the
        two. Equivalence tests must compare floats approximately.

        There is no ``CASE`` ladder here: ``DailySummary.date`` is already
        binned into site-local calendar days by :mod:`sync.summary`, using
        the same convention :func:`_period_bands` computes with, so the
        date -> band mapping is a Python dict lookup and band count costs
        nothing.
        """
        owner_fk = {"user": DailySummary.user_id,
                    "account": DailySummary.account_id}[owners_by]
        owner_model, owner_name_col = {
            "user": (User, User.username),
            "account": (Account, Account.account_name),
        }[owners_by]

        label_of: Dict[date, str] = {}
        for band in bands:
            day = band["start"]
            while day <= band["end"]:
                label_of[day] = band["label"]
                day += timedelta(days=1)

        def _filtered(query):
            query = query.filter(DailySummary.date >= win_start,
                                 DailySummary.date <= win_end)
            for dim, col in (("user", DailySummary.user_id),
                             ("account", DailySummary.account_id),
                             ("queue", DailySummary.queue_id)):
                value = filters.get(dim)
                if dim == "account" and isinstance(value, str) and not value:
                    continue        # empty projcode string == no filter
                if value:
                    query = query.filter(
                        self._lookup_fk_clause(dim, value, fk_col=col))
            return query

        metric_cols = [
            func.sum(DailySummary.job_count),
            func.sum(DailySummary.cpu_hours),
            func.sum(DailySummary.gpu_hours),
            func.sum(DailySummary.cpu_charges),
            func.sum(DailySummary.gpu_charges),
        ]

        # Statement 1 (owners only): rank ONCE over the window, exactly as
        # the scan path does, so both produce the same legend.
        top_ids: List[Any] = []
        top_names: Dict[Any, str] = {}
        if owners_limit is not None:
            rank_q = _filtered(self.session.query(owner_fk, *metric_cols))
            candidates = {}
            for owner_id, *cols in rank_q.group_by(owner_fk).all():
                if owner_id is None:
                    continue    # NO_JOBS markers and NULL FKs: the remainder
                acc = _zero_metrics()
                _accumulate(acc, *cols)
                candidates[owner_id] = _metrics_dict(acc)
            names = self._resolve_lookup_names(
                owner_model, owner_name_col, set(candidates))
            ranked = sorted(
                ((str(names.get(oid, oid)), oid, m)
                 for oid, m in candidates.items()),
                key=lambda t: (-_usage_rank(owners_sort_by, t[2]), t[0]),
            )[:owners_limit]
            top_ids = [oid for _n, oid, _m in ranked]
            top_names = {oid: nm for nm, oid, _m in ranked}

        # Statement 2: the series. Same trick as the scan path — non-top-N
        # owners collapse to a NULL group key, joining the derivable
        # remainder without a second pass.
        with_owners = owners_limit is not None and bool(top_ids)
        select_cols: List[Any] = [DailySummary.date, *metric_cols]
        group_cols: List[Any] = [DailySummary.date]
        if with_owners:
            owner_key = case((owner_fk.in_(top_ids), owner_fk)).label("owner_key")
            select_cols.insert(1, owner_key)
            group_cols.append(owner_key)
        rows = _filtered(self.session.query(*select_cols)).group_by(
            *group_cols).all()

        by_label: Dict[str, List[float]] = {}
        owners_by_label: Dict[str, Dict[Any, List[float]]] = {}
        for row in rows:
            if with_owners:
                day, owner_id, cols = row[0], row[1], row[2:]
            else:
                day, owner_id, cols = row[0], None, row[1:]
            label = label_of.get(day)
            if label is None:
                continue        # outside the window; the filter already bars it
            _accumulate(by_label.setdefault(label, _zero_metrics()), *cols)
            if owner_id is not None:
                per_owner = owners_by_label.setdefault(label, {})
                _accumulate(per_owner.setdefault(owner_id, _zero_metrics()), *cols)

        out_bands = []
        for band in bands:
            out = {
                "label": band["label"],
                "start": band["start"].isoformat(),
                "end": band["end"].isoformat(),
                **_metrics_dict(by_label.get(band["label"], _zero_metrics())),
            }
            if owners_limit is not None:
                per_owner = owners_by_label.get(band["label"], {})
                out["owners"] = {
                    top_names[oid]: _metrics_dict(
                        per_owner.get(oid, _zero_metrics()))
                    for oid in top_ids
                }
            out_bands.append(out)

        totals = {key: sum(b[key] for b in out_bands) for key in _METRIC_KEYS}
        return {
            "period": period,
            "owners_by": owners_by,
            "start": win_start.isoformat(),
            "end": win_end.isoformat(),
            "bands": out_bands,
            "totals": totals,
            "null_count": null_count,
            "total_count": totals["job_count"] + null_count,
        }

    def _resolve_timeseries_window(self, start, end, filters):
        """``(win_start, win_end, null_count)`` for the band vector.

        Caller-supplied bounds win. A missing bound is derived from the
        observed ``MIN``/``MAX`` of ``Job.end`` under the same filters —
        converted back to site-local days, so the derived domain and the
        bands share one timezone convention. ``win_start`` is ``None`` when
        nothing at all matched and no bound was supplied.

        The probe also carries the NULL-``Job.end`` count, because
        :meth:`jobs_timeseries` re-bounds its statements to the resolved
        window and a bounded date filter drops those rows (NULL fails both
        comparisons). Counting them here costs nothing — it is one more
        aggregate on a scan already being made — and the coverage lines up
        exactly: this probe runs **iff** a bound is missing, which is **iff**
        ``null_count`` can be non-zero. When the caller supplies both bounds
        their own filter has already excluded NULLs, so the count is 0
        without asking.

        Cost: **not** cheap. PostgreSQL can only shortcut ``MIN``/``MAX``
        through ``ix_jobs_end`` when the predicate is index-compatible; under
        a real filter set this is a full aggregate over the slice, on par
        with the series statement itself. Callers who can supply explicit
        bounds should — it skips this entirely.
        """
        if start is not None and end is not None:
            return start, end, 0
        probe = self.session.query(
            func.min(Job.end), func.max(Job.end),
            func.count(Job.id).filter(Job.end.is_(None)),
        )
        probe = self._apply_jobs_search_filters(probe, **filters)
        lo, hi, nulls = probe.one()
        nulls = int(nulls or 0)
        if lo is None or hi is None:
            # No banded rows, so there is no domain to zero-fill over. (Both
            # bounds supplied already returned above, so there is no
            # caller-supplied domain to fall back on here.) The slice may
            # still hold NULL-``end`` rows — they belong to no calendar band
            # but they DO match the filters, so they ride out in null_count
            # and keep ``total_count == jobs_count()`` honest.
            return None, None, nulls
        site_tz = ZoneInfo(JobHistoryConfig.SITE_TIMEZONE)

        def _to_site_day(naive_utc: datetime) -> date:
            return (naive_utc.replace(tzinfo=timezone.utc)
                    .astimezone(site_tz).date())

        return (start if start is not None else _to_site_day(lo),
                end if end is not None else _to_site_day(hi),
                nulls)

    def _resolve_lookup_names(self, model, name_col, ids) -> Dict[Any, Any]:
        """``{id: display_name}`` for a lookup table, post-aggregation.

        Deliberately not a join into the aggregate query: a join probes the
        lookup once per *scanned* row, this runs once per *distinct id*.
        """
        if not ids:
            return {}
        query = self.session.query(model.id, name_col)
        if len(ids) <= _LOOKUP_FETCH_ALL_THRESHOLD:
            query = query.filter(model.id.in_(ids))
        return dict(query.all())

    def _lookup_fk_clause(self, dim: str, value, *, fk_col=None):
        """Filter clause matching a lookup dimension by *name*, via its FK.

        The inverse of :meth:`_resolve_lookup_names`: one indexed probe of
        the unique name column, then an integer-FK predicate — instead of
        ``Job.user == name``, whose hybrid re-runs a correlated subquery for
        every scanned row (see :data:`_LOOKUP_DIMS` for the measurements).

        *value* is a single name or a sequence of them. Semantics match the
        hybrid comparison exactly: a name with no lookup row matches nothing,
        so an unresolvable name — or an empty sequence — yields ``false()``
        rather than silently dropping the filter. Rows with a NULL FK are
        excluded either way (``NULL = 'x'`` is not true).

        *fk_col* overrides which table's FK the predicate lands on, so the
        ``daily_summary`` fast path resolves names through this one code path
        rather than a parallel copy. The lookup tables are shared, so only
        the column changes.
        """
        model, fk_col_default, name_col = _LOOKUP_DIMS[dim]
        fk_col = fk_col_default if fk_col is None else fk_col
        names = (value,) if isinstance(value, str) else tuple(value)
        if not names:
            return false()
        ids = [i for (i,) in
               self.session.query(model.id).filter(name_col.in_(names))]
        if not ids:
            return false()
        return fk_col == ids[0] if len(ids) == 1 else fk_col.in_(ids)

    def list_qos_names(self, *, active_only: bool = True) -> List[str]:
        """Return JobQoS names from the lookup table, alphabetically ordered.

        Lets a UI populate a QoS filter dropdown without hardcoding the
        canonical seed list (``premium`` / ``regular`` / ``economy`` /
        ``uncharged`` / ``special``) — the lookup table is the source of
        truth, so a future addition shows up automatically.

        Args:
            active_only: If True (default), restrict to rows with
                ``active = True``. Set False to include retired QoS names
                still referenced by historical jobs.
        """
        query = self.session.query(JobQoS.name)
        if active_only:
            query = query.filter(JobQoS.active.is_(True))
        return [name for (name,) in query.order_by(JobQoS.name).all()]

    def _apply_jobs_search_filters(
        self, query, *, start, end, user, account, queue, qos, exit_status,
        job_id, name, ignore_case,
        min_eligible_secs, max_eligible_secs,
        min_nodes, max_nodes, min_cpus, max_cpus, min_gpus, max_gpus,
        min_elapsed, max_elapsed, min_reqmem, max_reqmem,
        min_memory_used, max_memory_used,
        min_memory_wasted, max_memory_wasted,
    ):
        """Apply the shared filter set used by jobs_search/count/facets.

        Every parameter is required (no defaults) on purpose: adding a filter
        to ``jobs_search`` and forgetting it in ``jobs_count`` then fails
        loudly with a TypeError on the first count call, instead of silently
        returning a page total that disagrees with the page.
        ``TestFilterSignatureParity`` enforces the same invariant statically.
        """
        query = self._apply_date_filter(query, start, end)
        # user/account/queue/qos filter through `_lookup_fk_clause`, never
        # the `Job.user`-style text hybrids: those stay the *display* path,
        # but in a WHERE clause they compile to a correlated subquery run
        # once per scanned row and force a full Seq Scan. See _LOOKUP_DIMS.
        if user:
            query = query.filter(self._lookup_fk_clause("user", user))
        if account is not None:
            # `account` accepts a single projcode or a sequence — sequence
            # form lets callers pull every projcode in a project tree
            # (parent + descendants) in one query. `str` is iterable so it
            # has to be detected first. An empty sequence is treated as
            # "no rows", not "no filter" — callers asking for an empty tree
            # get an empty result, not the whole table.
            if not (isinstance(account, str) and not account):
                query = query.filter(self._lookup_fk_clause("account", account))
        if queue:
            query = query.filter(self._lookup_fk_clause("queue", queue))
        if qos:
            query = query.filter(self._lookup_fk_clause("qos", qos))
        if exit_status:
            query = query.filter(Job.status == exit_status)
        if job_id:
            jid = job_id.strip()
            if '.' in jid:
                # Host suffix supplied → exact match.
                query = query.filter(Job.job_id == jid)
            else:
                # Bare digits or partial array form (e.g. "6049117",
                # "6049117[28]", "6049117[]"). Boundary-anchor on the char
                # immediately after the user-supplied prefix — either `.`
                # (scalar) or `[` (array) — so a search for "6049117"
                # matches every variant of that job but never bleeds into
                # an unrelated longer-prefix id like "60491170.host". Both
                # clauses are leading-anchored LIKE patterns that use the
                # `ix_jobs_job_id` btree on both Postgres and SQLite;
                # `[` and `]` are literal in `LIKE` on both backends.
                query = query.filter(or_(
                    Job.job_id.like(f"{jid}.%"),   # scalar form
                    Job.job_id.like(f"{jid}[%"),   # array forms (parent + elements)
                ))
        if name:
            # `str` is iterable, so the single-pattern form must be detected
            # first — same trap as `account` above. UNLIKE `account`, an empty
            # sequence is "no filter", not "no rows": Click's multiple=True
            # yields () for an unsupplied -N, so the empty case has to be the
            # identity or the CLI default would return nothing. That inversion
            # is deliberate; see the jobs_search docstring.
            patterns = (name,) if isinstance(name, str) else tuple(name)
            # Resolve the dialect lazily — only pattern matching needs it.
            clause = glob_match_clause(
                Job.name, patterns,
                dialect=self.session.get_bind().dialect.name,
                ignore_case=ignore_case,
            )
            if clause is not None:
                query = query.filter(clause)
        # Numeric range bounds: all inclusive, all NULL-strict (a NULL column
        # value fails both comparisons, so those rows drop out). `is not None`
        # rather than truthiness throughout — 0 is a meaningful bound.
        for _column, _lo, _hi in (
            (Job.eligible_secs, min_eligible_secs, max_eligible_secs),
            (Job.numnodes,      min_nodes,         max_nodes),
            (Job.numcpus,       min_cpus,          max_cpus),
            (Job.numgpus,       min_gpus,          max_gpus),
            (Job.elapsed,       min_elapsed,       max_elapsed),
            (Job.reqmem,        min_reqmem,        max_reqmem),
            (Job.memory,        min_memory_used,   max_memory_used),
            (_MEMORY_WASTED,    min_memory_wasted, max_memory_wasted),
        ):
            if _lo is not None:
                query = query.filter(_column >= _lo)
            if _hi is not None:
                query = query.filter(_column <= _hi)
        return query

    def usage_summary(
        self,
        account: str,
        start: date,
        end: date,
    ) -> Dict[str, Any]:
        """Get usage summary for an account over a date range.

        Aggregates job counts and resource usage using pre-computed charging hours
        from the job_charges table.

        Args:
            account: Account name to query
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            Dict with aggregated metrics:
                - job_count: Total number of jobs
                - total_elapsed_seconds: Sum of all job elapsed times
                - total_cpu_hours: Sum of computed CPU hours (from charging view)
                - total_gpu_hours: Sum of computed GPU hours (from charging view)
                - total_memory_hours: Sum of computed memory hours (from charging view)
                - users: List of unique users
                - queues: List of unique queues
        """
        # JOIN Job and JobCharge to get both job metadata and charges
        query = self.session.query(Job, JobCharge).join(
            JobCharge, Job.id == JobCharge.job_id
        ).filter(
            and_(
                Job.account == account,
                Job.end >= datetime.combine(start, datetime.min.time()),
                Job.end <= datetime.combine(end, datetime.max.time()),
            )
        )

        results = query.all()

        if not results:
            return {
                "job_count": 0,
                "total_elapsed_seconds": 0,
                "total_cpu_hours": 0.0,
                "total_gpu_hours": 0.0,
                "total_memory_hours": 0.0,
                "users": [],
                "queues": [],
            }

        total_elapsed = sum((job.elapsed or 0) for job, charge in results)
        total_cpu_hours = sum((charge.cpu_hours or 0.0) for job, charge in results)
        total_gpu_hours = sum((charge.gpu_hours or 0.0) for job, charge in results)
        total_memory_hours = sum((charge.memory_hours or 0.0) for job, charge in results)

        unique_users = sorted(set(job.user for job, charge in results if job.user))
        unique_queues = sorted(set(job.queue for job, charge in results if job.queue))

        return {
            "job_count": len(results),
            "total_elapsed_seconds": total_elapsed,
            "total_cpu_hours": total_cpu_hours,
            "total_gpu_hours": total_gpu_hours,
            "total_memory_hours": total_memory_hours,
            "users": unique_users,
            "queues": unique_queues,
        }

    def user_summary(
        self,
        user: str,
        start: date,
        end: date,
    ) -> Dict[str, Any]:
        """Get usage summary for a user over a date range.

        Aggregates job counts and resource usage using pre-computed charging hours
        from the job_charges table.

        Args:
            user: Username to query
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            Dict with aggregated metrics similar to usage_summary
        """
        # JOIN Job and JobCharge to get both job metadata and charges
        query = self.session.query(Job, JobCharge).join(
            JobCharge, Job.id == JobCharge.job_id
        ).filter(
            and_(
                Job.user == user,
                Job.end >= datetime.combine(start, datetime.min.time()),
                Job.end <= datetime.combine(end, datetime.max.time()),
            )
        )

        results = query.all()

        if not results:
            return {
                "job_count": 0,
                "total_elapsed_seconds": 0,
                "total_cpu_hours": 0.0,
                "total_gpu_hours": 0.0,
                "total_memory_hours": 0.0,
                "accounts": [],
                "queues": [],
            }

        total_elapsed = sum((job.elapsed or 0) for job, charge in results)
        total_cpu_hours = sum((charge.cpu_hours or 0.0) for job, charge in results)
        total_gpu_hours = sum((charge.gpu_hours or 0.0) for job, charge in results)
        total_memory_hours = sum((charge.memory_hours or 0.0) for job, charge in results)

        unique_accounts = sorted(set(job.account for job, charge in results if job.account))
        unique_queues = sorted(set(job.queue for job, charge in results if job.queue))

        return {
            "job_count": len(results),
            "total_elapsed_seconds": total_elapsed,
            "total_cpu_hours": total_cpu_hours,
            "total_gpu_hours": total_gpu_hours,
            "total_memory_hours": total_memory_hours,
            "accounts": unique_accounts,
            "queues": unique_queues,
        }

    def daily_summary_by_account(
        self,
        account: str,
        start: date,
        end: date,
    ) -> List[DailySummary]:
        """Get daily summaries for an account over a date range.

        Uses pre-aggregated DailySummary table for efficient retrieval.

        Args:
            account: Account name to query
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            List of DailySummary objects
        """
        query = self.session.query(DailySummary).filter(
            and_(
                DailySummary.account == account,
                DailySummary.date >= start,
                DailySummary.date <= end,
                DailySummary.user_id.isnot(None),  # Exclude empty day markers
            )
        )

        return query.order_by(DailySummary.date).all()

    def daily_summary_report(
        self,
        start: date,
        end: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get daily usage summary rows for a date range.

        Reads directly from the pre-aggregated DailySummary table.
        Excludes NO_JOBS marker rows (where user_id IS NULL).

        Args:
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            List of dicts with keys: date, user, account, queue, job_count,
            cpu_hours, gpu_hours, memory_hours, and their QoS-weighted
            counterparts cpu_charges, gpu_charges, memory_charges
            (``hours x qos_factor``, folded in by :mod:`sync.summary` with
            the same formula :func:`_charge_expr` uses on the live tables).

            **Charges are not proportional to hours** — ``qos_factor`` is a
            genuine 0.0 for the ``uncharged`` QoS, so a row can report hours
            > 0 with charges == 0.
        """
        if end is None:
            end = date.today()

        rows = (
            self.session.query(DailySummary)
            .filter(
                DailySummary.date >= start,
                DailySummary.date <= end,
                DailySummary.user_id.isnot(None),  # exclude NO_JOBS markers
            )
            .order_by(DailySummary.date, DailySummary.user_id,
                      DailySummary.account_id, DailySummary.queue_id)
            .all()
        )
        return [
            {
                "date": str(row.date),
                "user": row.user or "",
                "account": row.account or "",
                "queue": row.queue or "",
                "job_count": row.job_count or 0,
                "cpu_hours": row.cpu_hours or 0.0,
                "gpu_hours": row.gpu_hours or 0.0,
                "memory_hours": row.memory_hours or 0.0,
                "cpu_charges": row.cpu_charges or 0.0,
                "gpu_charges": row.gpu_charges or 0.0,
                "memory_charges": row.memory_charges or 0.0,
            }
            for row in rows
        ]

    def daily_summary_by_user(
        self,
        user: str,
        start: date,
        end: date,
    ) -> List[DailySummary]:
        """Get daily summaries for a user over a date range.

        Uses pre-aggregated DailySummary table for efficient retrieval.

        Args:
            user: Username to query
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            List of DailySummary objects
        """
        query = self.session.query(DailySummary).filter(
            and_(
                DailySummary.user == user,
                DailySummary.date >= start,
                DailySummary.date <= end,
                DailySummary.user_id.isnot(None),  # Exclude empty day markers
            )
        )

        return query.order_by(DailySummary.date).all()

    def jobs_by_entity_period(
        self,
        primary_entity: str = "user",
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get the number of jobs grouped by period and entity pair.

        Args:
            primary_entity: Primary grouping entity ('user' or 'account').
                           'user' gives Period > User > Account ordering.
                           'account' gives Period > Account > User ordering.
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day', 'month', 'quarter', 'year')

        Returns:
            List of dicts with 'period', 'user', 'account', and 'job_count' keys,
            ordered by period, then primary_entity, then secondary_entity.
        """
        from .builders import PeriodGrouper

        period_func = PeriodGrouper.get_period_func(period, Job.end)

        if primary_entity == "user":
            order_fields = [Job.user, Job.account]
        else:  # "account"
            order_fields = [Job.account, Job.user]

        query = self.session.query(
            period_func.label("period"),
            Job.user,
            Job.account,
            func.count(Job.id).label("job_count")
        )

        query = self._apply_date_filter(query, start, end)

        results = query.group_by("period", Job.user, Job.account).order_by(
            "period", *order_fields
        ).all()

        return [
            {"period": row[0], "user": row[1], "account": row[2], "job_count": row[3]}
            for row in results
        ]

    def jobs_per_user_account_by_period(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get the number of jobs per user per account by period in a date range.

        Delegates to jobs_by_entity_period() for backward compatibility.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day', 'month', 'quarter', 'year')

        Returns:
            A list of dicts with 'period', 'user', 'account', and 'job_count' keys.
        """
        return self.jobs_by_entity_period(
            primary_entity="user",
            start=start,
            end=end,
            period=period
        )

    def unique_projects_by_period(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get the number of unique projects by period in a date range.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day', 'month', 'quarter', 'year')

        Returns:
            A list of dicts with 'period' and 'project_count' keys.
        """
        from .builders import PeriodGrouper

        # Get period function
        period_func = PeriodGrouper.get_period_func(period, Job.end)

        query = self.session.query(
            period_func.label("period"),
            func.count(func.distinct(Job.account)).label("project_count")
        )

        query = self._apply_date_filter(query, start, end)

        results = query.group_by("period").order_by("period").all()

        return [{"period": row[0], "project_count": row[1]} for row in results]

    def unique_users_by_period(
        self,
        start: Optional[date] = None,
        end: Optional[date] = None,
        period: str = "day",
    ) -> List[Dict[str, Any]]:
        """Get the number of unique users by period in a date range.

        Args:
            start: Optional start date (inclusive) - filters on job end time
            end: Optional end date (inclusive) - filters on job end time
            period: Grouping period ('day', 'month', 'quarter', 'year')

        Returns:
            A list of dicts with 'period' and 'user_count' keys.
        """
        from .builders import PeriodGrouper

        # Get period function
        period_func = PeriodGrouper.get_period_func(period, Job.end)

        query = self.session.query(
            period_func.label("period"),
            func.count(func.distinct(Job.user)).label("user_count")
        )

        query = self._apply_date_filter(query, start, end)

        results = query.group_by("period").order_by("period").all()

        return [{"period": row[0], "user_count": row[1]} for row in results]

    def top_users_by_jobs(
        self,
        start: date,
        end: date,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get top users by job count in a date range.

        Args:
            start: Start date (inclusive)
            end: End date (inclusive)
            limit: Maximum number of users to return

        Returns:
            List of dicts with 'user' and 'job_count' keys
        """
        result = (
            self.session.query(
                Job.user,
                func.count(Job.id).label("job_count")
            )
            .filter(
                and_(
                    Job.end >= datetime.combine(start, datetime.min.time()),
                    Job.end <= datetime.combine(end, datetime.max.time()),
                )
            )
            .group_by(Job.user)
            .order_by(func.count(Job.id).desc())
            .limit(limit)
            .all()
        )

        return [{"user": user, "job_count": count} for user, count in result]

    def queue_statistics(
        self,
        start: date,
        end: date,
    ) -> List[Dict[str, Any]]:
        """Get statistics by queue for a date range.

        Args:
            start: Start date (inclusive)
            end: End date (inclusive)

        Returns:
            List of dicts with queue statistics
        """
        result = (
            self.session.query(
                Job.queue,
                func.count(Job.id).label("job_count"),
                func.sum(Job.elapsed).label("total_elapsed"),
                func.avg(Job.elapsed).label("avg_elapsed"),
            )
            .filter(
                and_(
                    Job.end >= datetime.combine(start, datetime.min.time()),
                    Job.end <= datetime.combine(end, datetime.max.time()),
                )
            )
            .group_by(Job.queue)
            .order_by(func.count(Job.id).desc())
            .all()
        )

        return [
            {
                "queue": queue,
                "job_count": count,
                "total_elapsed_seconds": elapsed or 0,
                "avg_elapsed_seconds": avg or 0,
            }
            for queue, count, elapsed, avg in result
        ]

    @classmethod
    def multi_machine_query(
        cls,
        machines: List[str],
        method_name: str,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Execute a query across multiple machines and aggregate results.

        This class method allows running the same query against multiple machine
        databases (casper, derecho) and combining the results with machine labels.

        Args:
            machines: List of machine names to query (e.g., ['casper', 'derecho'])
            method_name: Name of the JobQueries method to call
            **kwargs: Additional keyword arguments to pass to the query method

        Returns:
            List of result dictionaries, each tagged with a 'machine' field

        Example:
            >>> results = JobQueries.multi_machine_query(
            ...     machines=['casper', 'derecho'],
            ...     method_name='usage_by_group',
            ...     resource_type='cpu',
            ...     group_by='user',
            ...     start=date(2025, 11, 1),
            ...     end=date(2025, 11, 30)
            ... )
            >>> # Results contain data from both machines with 'machine' field
        """
        from ..database import get_session

        all_results = []

        for machine in machines:
            session = get_session(machine)
            try:
                queries = cls(session, machine=machine)
                method = getattr(queries, method_name)
                results = method(**kwargs)

                # Tag each result with the machine name
                for row in results:
                    row['machine'] = machine

                all_results.extend(results)
            finally:
                session.close()

        return all_results


if __name__ == "__main__":
    """Example usage of the JobQueries interface."""
    import sys
    from datetime import timedelta
    from ..database import get_session

    # Check if database exists
    try:
        # Connect to Derecho by default (change to "casper" if needed)
        machine = "derecho"
        session = get_session(machine)
        queries = JobQueries(session)

        print(f"=== JobQueries Examples ({machine}) ===\n")

        # Example 1: Get recent jobs for a specific user
        print("Example 1: Recent jobs by user")
        print("-" * 50)

        # Get the date range for last 7 days
        end_date = date.today()
        start_date = end_date - timedelta(days=7)

        # Find a user with jobs in the database
        result = session.query(Job.user).filter(Job.user.isnot(None)).limit(1).first()

        if result:
            example_user = result[0]
            jobs = queries.jobs_by_user(example_user, start=start_date, end=end_date)
            print(f"User: {example_user}")
            print(f"Date range: {start_date} to {end_date}")
            print(f"Found {len(jobs)} jobs")

            if jobs:
                print(f"\nFirst 3 jobs:")
                for job in jobs[:3]:
                    print(f"  - Job {job.job_id}: {job.queue}, elapsed={job.elapsed}s")
        else:
            print("No users found in database")

        print()

        # Example 2: Usage summary for an account
        print("Example 2: Account usage summary")
        print("-" * 50)

        # Find an account with jobs
        result = session.query(Job.account).filter(Job.account.isnot(None)).limit(1).first()

        if result:
            example_account = result[0]
            summary = queries.usage_summary(example_account, start=start_date, end=end_date)
            print(f"Account: {example_account}")
            print(f"Date range: {start_date} to {end_date}")
            print(f"Job count: {summary['job_count']}")
            print(f"Total elapsed: {summary['total_elapsed_seconds']:,} seconds")
            print(f"Total CPU-hours: {summary['total_cpu_hours']:,.2f}")
            print(f"Total GPU-hours: {summary['total_gpu_hours']:,.2f}")
            print(f"Total Memory-hours: {summary['total_memory_hours']:,.2f}")
            print(f"Users: {', '.join(summary['users'][:5])}")
            print(f"Queues: {', '.join(summary['queues'])}")
        else:
            print("No accounts found in database")

        print()

        # Example 3: Top users by job count
        print("Example 3: Top 5 users by job count")
        print("-" * 50)

        top_users = queries.top_users_by_jobs(start=start_date, end=end_date, limit=5)

        if top_users:
            for i, user_stat in enumerate(top_users, 1):
                print(f"{i}. {user_stat['user']}: {user_stat['job_count']} jobs")
        else:
            print("No jobs found in date range")

        print()

        # Example 4: Queue statistics
        print("Example 4: Queue statistics")
        print("-" * 50)

        queue_stats = queries.queue_statistics(start=start_date, end=end_date)

        if queue_stats:
            for stat in queue_stats[:5]:  # Show top 5 queues
                avg_hours = stat['avg_elapsed_seconds'] / 3600
                print(f"{stat['queue']}:")
                print(f"  Jobs: {stat['job_count']}")
                print(f"  Avg elapsed: {avg_hours:.2f} hours")
        else:
            print("No queue statistics available")

        print()

        # Example 5: Daily summaries (if available)
        print("Example 5: Daily summaries for user")
        print("-" * 50)

        if result:
            result = session.query(DailySummary.user).limit(1).first()
            if result:
                example_user = result[0]
                daily = queries.daily_summary_by_user(example_user, start=start_date, end=end_date)
                print(f"User: {example_user}")
                print(f"Found {len(daily)} daily summary records")

                if len(daily) > 3:
                    print("\nFirst 3 days:")
                    for summary in daily[:3]:
                        print(f"  {summary.date}: {summary.job_count} jobs, "
                              f"{summary.cpu_hours:.2f} CPU-hours")
            else:
                print("No daily summaries found in database")

        session.close()

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        print("\nMake sure you have:")
        print("1. Run 'make sync' to populate the database")
        print("2. Or specify a different machine with the QHIST_DERECHO_DB env var")
        sys.exit(1)
