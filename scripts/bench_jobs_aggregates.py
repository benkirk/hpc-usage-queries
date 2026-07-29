#!/usr/bin/env python
"""Benchmark the JobQueries aggregates against a real database.

Reproduces the cost tables in ``docs/plans/JOBS_TIMESERIES.md``. Reports the
**minimum** of N runs: timing noise is one-sided, so the minimum is the only
stable estimator on a shared/laptop instance (observed spread on a single
arm: 1241-2127 ms across 10 runs).

Three measurements:

**Gate A** — an interleaved A/B of the charge SUMs *alone*: the same grouped
aggregate built once with 2 hour-SUMs and once with 2 hour-SUMs + 2
charge-SUMs, alternated so buffer-cache state and background load hit both
arms equally. Running the two shapes as separate processes instead lets drift
between runs swamp the effect.

**Gate B** — ``jobs_timeseries`` (two scans: rank + series) against one
``jobs_histogram`` on the same window.

**Band sensitivity** — one fixed window at three granularities, so row count
and filters are identical and the CASE ladder's width is the only variable.
The periods are **interleaved within each round**, not timed one after the
other: a sequential loop lets cache warming ride along with band count, which
is how this benchmark's first published table reported +54 % at 180 bands
where the real figure is ~10 %.

**Fast path** — ``daily_summary`` against the ``jobs`` scan for the same
series, with a ``job_count`` agreement check.

Usage::

    # from the repo root, so the working tree shadows any installed copy
    PYTHONPATH=$PWD python scripts/bench_jobs_aggregates.py --machine casper

Requires a populated database (``JOB_HISTORY_DB_BACKEND`` etc. in ``.env``).
Windows are anchored on ``max(job.end)`` rather than the wall clock, so the
numbers are stable against a snapshot database.
"""
import argparse
import statistics
import time
from datetime import timedelta

from sqlalchemy import func

from job_history import Job, JobCharge, JobQueries, get_session
from job_history.queries import jobs as jobs_mod
from job_history.queries.jobs import QueryConfig, _bucket_case, _charge_expr


def best(fn, repeat):
    """(min, max, median) wall-clock ms over *repeat* runs."""
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return min(times), max(times), statistics.median(times)


def gate_a(q, session, start, end, repeat):
    """Same grouped aggregate, 2 SUMs vs 4 SUMs, interleaved."""
    bucket = _bucket_case(Job.numcpus, QueryConfig.CPU_HIST_BUCKETS)

    def run(with_charges):
        cols = [bucket, func.count(Job.id),
                func.sum(JobCharge.cpu_hours), func.sum(JobCharge.gpu_hours)]
        if with_charges:
            cols += [func.sum(_charge_expr(JobCharge.cpu_hours)),
                     func.sum(_charge_expr(JobCharge.gpu_hours))]
        query = (session.query(*cols)
                 .outerjoin(JobCharge, Job.id == JobCharge.job_id))
        return q._apply_date_filter(query, start, end).group_by(bucket).all()

    plain, charged = [], []
    for _ in range(repeat):
        t0 = time.perf_counter(); run(False)
        plain.append((time.perf_counter() - t0) * 1000)
        t0 = time.perf_counter(); run(True)
        charged.append((time.perf_counter() - t0) * 1000)
    return plain, charged


def band_sensitivity(q, anchor, span_days, repeat):
    """Fixed window, three granularities, periods INTERLEAVED across rounds.

    The interleaving is the whole point. Timing the periods in sequence lets
    buffer-cache warming ride along with band count and inflates the effect —
    that is exactly how this benchmark's first published table reported +54 %
    at 180 bands where the real figure is ~10 %.
    """
    start = anchor - timedelta(days=span_days - 1)
    rows = q.jobs_count(start=start, end=anchor)
    periods = ('month', 'week', 'day')

    # This benchmark exists to locate the ladder's knee, so it deliberately
    # measures the SCAN path (the fast path has no ladder) and past the
    # shipped cap (730 daily bands is exactly the width worth knowing about).
    orig_uses = JobQueries._timeseries_uses_summary
    orig_cap = jobs_mod._MAX_TIMESERIES_BANDS
    JobQueries._timeseries_uses_summary = lambda *a, **k: False
    jobs_mod._MAX_TIMESERIES_BANDS = 10_000
    try:
        counts = {p: len(q.jobs_timeseries(p, start=start, end=anchor)['bands'])
                  for p in periods}
        times = {p: [] for p in periods}
        for _ in range(repeat):
            for period in periods:        # <- alternate WITHIN the round
                t0 = time.perf_counter()
                q.jobs_timeseries(period, start=start, end=anchor)
                times[period].append((time.perf_counter() - t0) * 1000)
    finally:
        JobQueries._timeseries_uses_summary = orig_uses
        jobs_mod._MAX_TIMESERIES_BANDS = orig_cap

    base = min(times['month'])
    print(f'\nBand-count sensitivity - fixed {span_days}d window '
          f'({rows:,} jobs), jobs-scan path, ladder width is the only '
          f'variable (interleaved, min of {repeat}):')
    for period in periods:
        flag = '  <- past the shipped cap' if counts[period] > orig_cap else ''
        print(f'  period={period:5s} {counts[period]:4d} bands  '
              f'{min(times[period]):8.1f} ms  '
              f'{min(times[period]) / base:5.2f}x{flag}')


def fast_path(q, session, repeat):
    """daily_summary fast path vs the jobs scan, and do they agree?

    Anchored on the summary watermark, not on ``max(job.end)``: the rollup
    lags ``jobs`` by up to a day, and a window touching an unsummarized day
    correctly refuses the fast path.
    """
    from job_history import DailySummary
    anchor = session.query(func.max(DailySummary.date)).scalar()
    if anchor is None:
        print('\ndaily_summary fast path: no summary rows - skipped')
        return
    print(f'\ndaily_summary fast path vs scanning jobs (anchored on the '
          f'summary watermark {anchor})')
    orig = JobQueries._timeseries_uses_summary
    for span, period in ((180, 'day'), (730, 'day')):
        start = anchor - timedelta(days=span - 1)
        kw = dict(start=start, end=anchor)
        if not q._timeseries_uses_summary(
                {'start': start, 'end': anchor}, start, anchor, 'user'):
            print(f'  {span}d/{period}: summary not usable here - skipped')
            continue
        fast, _, _ = best(lambda: q.jobs_timeseries(period, **kw), repeat)
        # The scan arm is measured past its own cap on purpose — 730 daily
        # bands is a window the scan path legitimately refuses, and the point
        # of the comparison is to show what it would have cost.
        JobQueries._timeseries_uses_summary = lambda *a, **k: False
        orig_cap = jobs_mod._MAX_TIMESERIES_BANDS
        jobs_mod._MAX_TIMESERIES_BANDS = 10_000
        try:
            scan, _, _ = best(lambda: q.jobs_timeseries(period, **kw),
                              max(2, repeat // 4))
            scanned = q.jobs_timeseries(period, **kw)
        finally:
            JobQueries._timeseries_uses_summary = orig
            jobs_mod._MAX_TIMESERIES_BANDS = orig_cap
        summed = q.jobs_timeseries(period, **kw)
        agree = summed['total_count'] == scanned['total_count']
        print(f'  {span:4d}d/{period:5s}  summary {fast:8.1f} ms   '
              f'scan {scan:9.1f} ms   {scan / fast:6.1f}x   '
              f'job_count agrees={agree} ({summed["total_count"]:,})')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--machine', default='casper')
    ap.add_argument('--days', type=int, default=30,
                    help='window for Gate A and Gate B (default 30)')
    ap.add_argument('--band-span', type=int, default=180,
                    help='first band-sensitivity window (default 180)')
    ap.add_argument('--repeat', type=int, default=10)
    args = ap.parse_args()

    session = get_session(args.machine)
    q = JobQueries(session, machine=args.machine)
    anchor = session.query(func.max(Job.end)).scalar().date()
    start, end = anchor - timedelta(days=args.days - 1), anchor
    n = q.jobs_count(start=start, end=end)

    print(f'\n== {args.machine}: {args.days}d ({start}..{end}), '
          f'{n:,} jobs, MIN of {args.repeat} ==\n')

    plain, charged = gate_a(q, session, start, end, args.repeat)
    delta = (min(charged) - min(plain)) / min(plain) * 100
    print('Gate A - charge SUMs added to the same grouped aggregate')
    print(f'  2 SUMs (previous shape) : {min(plain):8.1f} ms'
          f'   (spread {min(plain):.0f}-{max(plain):.0f})')
    print(f'  4 SUMs (with charges)   : {min(charged):8.1f} ms'
          f'   (spread {min(charged):.0f}-{max(charged):.0f})')
    print(f'  delta                   : {delta:+7.1f} %\n')

    hist, _, _ = best(
        lambda: q.jobs_histogram('cpus', start=start, end=end, owners_limit=10),
        args.repeat)
    print('Gate B - jobs_timeseries vs one jobs_histogram (same window)')
    print(f'  jobs_histogram+owners10        : {hist:8.1f} ms   1.00x')
    for period in ('day', 'week'):
        bands = len(q.jobs_timeseries(period, start=start, end=end)['bands'])
        series, _, _ = best(
            lambda p=period: q.jobs_timeseries(
                p, start=start, end=end, owners_limit=10),
            args.repeat)
        print(f'  jobs_timeseries({period:5s}) {bands:4d} bands: '
              f'{series:8.1f} ms   {series / hist:.2f}x')

    for span in (args.band_span, 730):
        band_sensitivity(q, anchor, span, max(3, args.repeat // 3))

    fast_path(q, session, args.repeat)


if __name__ == '__main__':
    main()
