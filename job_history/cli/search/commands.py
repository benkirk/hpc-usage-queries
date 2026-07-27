"""Search command class.

Resolves the column set (default / verbose / custom ``--display``), calls
:meth:`JobQueries.jobs_search`, and emits the standard JSON envelope.
"""

from typing import Optional, Sequence

from ..core import (
    BaseHistoryCommand,
    EXIT_ERROR,
    EXIT_SUCCESS,
    ExporterRegistry,
)
from job_history.columns import COLUMNS, DEFAULT_COLUMNS, VERBOSE_COLUMNS
from . import builders


class SearchCommand(BaseHistoryCommand):
    """Drives the ``jobhist search`` subcommand."""

    def execute(
        self,
        *,
        user: Optional[str] = None,
        account: Optional[str] = None,
        queue: Optional[str] = None,
        qos: Optional[str] = None,
        exit_status: Optional[str] = None,
        job_id: Optional[str] = None,
        name: Sequence[str] = (),
        ignore_case: bool = False,
        min_wait_hours: Optional[float] = None,
        max_wait_hours: Optional[float] = None,
        min_nodes: Optional[int] = None,
        max_nodes: Optional[int] = None,
        min_cpus: Optional[int] = None,
        max_cpus: Optional[int] = None,
        min_gpus: Optional[int] = None,
        max_gpus: Optional[int] = None,
        verbose: bool = False,
        display: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> int:
        try:
            cols = _resolve_columns(display=display, verbose=verbose)
        except ValueError as exc:
            self.ctx.stderr_console.print(f"❌ {exc}", style="bold red")
            return EXIT_ERROR

        # The CLI talks hours ("jobs that waited more than 6h"); the column and
        # the query API talk seconds. Convert at the boundary and publish the
        # *resolved* seconds in the envelope, so a consumer can replay
        # ``filters`` straight into jobs_search() without redoing the
        # conversion. 0.0 is a meaningful bound, hence ``is not None``.
        min_eligible_secs = (
            int(min_wait_hours * 3600) if min_wait_hours is not None else None
        )
        max_eligible_secs = (
            int(max_wait_hours * 3600) if max_wait_hours is not None else None
        )
        # Click's multiple=True yields () when -N is not supplied; normalize to
        # None so the envelope keeps its "null means unset" convention rather
        # than emitting an empty array.
        name_patterns = tuple(name) if name else None

        try:
            rows = self.get_queries().jobs_search(
                start=self.ctx.start_date,
                end=self.ctx.end_date,
                user=user,
                account=account,
                queue=queue,
                qos=qos,
                exit_status=exit_status,
                job_id=job_id,
                name=name_patterns,
                ignore_case=ignore_case,
                min_eligible_secs=min_eligible_secs,
                max_eligible_secs=max_eligible_secs,
                min_nodes=min_nodes, max_nodes=max_nodes,
                min_cpus=min_cpus, max_cpus=max_cpus,
                min_gpus=min_gpus, max_gpus=max_gpus,
                columns=cols,
                limit=limit,
            )
            envelope = builders.build_search(
                rows,
                ctx=self.ctx,
                requested_cols=cols,
                filters={
                    "user": user,
                    "account": account,
                    "queue": queue,
                    "qos": qos,
                    "exit_status": exit_status,
                    "job_id": job_id,
                    "name": list(name_patterns) if name_patterns else None,
                    "ignore_case": ignore_case,
                    "min_eligible_secs": min_eligible_secs,
                    "max_eligible_secs": max_eligible_secs,
                    "min_nodes": min_nodes,
                    "max_nodes": max_nodes,
                    "min_cpus": min_cpus,
                    "max_cpus": max_cpus,
                    "min_gpus": min_gpus,
                    "max_gpus": max_gpus,
                    "limit": limit,
                },
            )
            ExporterRegistry.resolve(self.ctx.output_format).emit(envelope, ctx=self.ctx)
            return EXIT_SUCCESS
        except Exception as exc:
            return self.handle_exception(exc)


def _resolve_columns(*, display: Optional[str], verbose: bool):
    """Apply the precedence: --display > --verbose > defaults."""
    if display:
        requested = [c.strip() for c in display.split(",") if c.strip()]
        unknown = [c for c in requested if c not in COLUMNS]
        if unknown:
            valid = ", ".join(sorted(COLUMNS))
            raise ValueError(
                f"Unknown column(s): {', '.join(unknown)}. Valid columns: {valid}"
            )
        return tuple(requested)
    if verbose:
        return VERBOSE_COLUMNS
    return DEFAULT_COLUMNS
