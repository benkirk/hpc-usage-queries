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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--machine', default='casper')
    ap.add_argument('--days', type=int, default=30,
                    help='window for Gate A and Gate B (default 30)')
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

    iso_start = anchor - timedelta(days=179)
    iso_rows = q.jobs_count(start=iso_start, end=anchor)
    print(f'\nBand-count sensitivity - fixed 180d window ({iso_rows:,} jobs), '
          f'ladder width is the only variable:')
    for period in ('month', 'week', 'day'):
        bands = len(q.jobs_timeseries(period, start=iso_start, end=anchor)['bands'])
        t, _, _ = best(
            lambda p=period: q.jobs_timeseries(p, start=iso_start, end=anchor),
            max(3, args.repeat // 3))
        print(f'  period={period:5s} {bands:4d} bands  {t:8.1f} ms')
    print()


if __name__ == '__main__':
    main()
