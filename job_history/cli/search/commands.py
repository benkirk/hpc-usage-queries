"""Search command class.

Resolves the column set (default / verbose / custom ``--display``), calls
:meth:`JobQueries.jobs_search`, and emits the standard JSON envelope.
"""

from typing import Optional

from ..core import (
    BaseHistoryCommand,
    EXIT_ERROR,
    EXIT_SUCCESS,
    ExporterRegistry,
)
from . import builders
from .columns import COLUMNS, DEFAULT_COLUMNS, VERBOSE_COLUMNS


class SearchCommand(BaseHistoryCommand):
    """Drives the ``jobhist search`` subcommand."""

    def execute(
        self,
        *,
        user: Optional[str] = None,
        account: Optional[str] = None,
        queue: Optional[str] = None,
        status: Optional[str] = None,
        verbose: bool = False,
        display: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> int:
        try:
            cols = _resolve_columns(display=display, verbose=verbose)
        except ValueError as exc:
            self.ctx.stderr_console.print(f"❌ {exc}", style="bold red")
            return EXIT_ERROR

        try:
            rows = self.get_queries().jobs_search(
                start=self.ctx.start_date,
                end=self.ctx.end_date,
                user=user,
                account=account,
                queue=queue,
                status=status,
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
                    "status": status,
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
