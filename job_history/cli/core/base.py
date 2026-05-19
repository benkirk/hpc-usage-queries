"""Base command classes for the jobhist CLI.

Mirrors project_samuel's src/cli/core/base.py. Every subcommand is a
subclass with an ``execute(**kwargs) -> int`` method returning an
EXIT_* code. The top-level Click group instantiates one command,
calls execute, and `sys.exit`s the returned code.
"""

from abc import ABC, abstractmethod

from .context import Context
from .utils import EXIT_ERROR


class BaseCommand(ABC):
    """Base class for all jobhist commands."""

    def __init__(self, ctx: Context):
        self.ctx = ctx
        self.session = ctx.session
        self.console = ctx.console

    @abstractmethod
    def execute(self, **kwargs) -> int:
        """Run the command. Returns an EXIT_* code."""

    def handle_exception(self, exc: Exception) -> int:
        """Common error handler — prints to stderr, traceback if verbose."""
        self.ctx.stderr_console.print(f"❌ Error: {exc}", style="bold red")
        if self.ctx.verbose:
            import traceback
            self.console.print(traceback.format_exc(), style="dim")
        return EXIT_ERROR


class BaseHistoryCommand(BaseCommand):
    """Base for history subcommands. Exposes a JobQueries factory."""

    def get_queries(self):
        # Import lazily so the cli package can be imported in test contexts
        # without pulling the full ORM/queries graph.
        from job_history.queries import JobQueries
        return JobQueries(self.session, self.ctx.machine)


class BaseResourceCommand(BaseCommand):
    """Drives a single ReportConfig.

    The same class instance is reused for all ~30 resource reports —
    the declarative RESOURCE_REPORTS list supplies the query method
    name, parameters, and column spec.
    """

    def __init__(self, ctx: Context, config):
        super().__init__(ctx)
        self.config = config

    def get_queries(self):
        from job_history.queries import JobQueries
        return JobQueries(self.session, self.ctx.machine)

    def execute(self, **kwargs) -> int:
        try:
            rows = self._run_query()
            return self._emit(rows)
        except Exception as exc:
            return self.handle_exception(exc)

    def _run_query(self):
        method = getattr(self.get_queries(), self.config.query_method)
        return method(
            start=self.ctx.start_date,
            end=self.ctx.end_date,
            **self.config.query_params,
        )

    def _emit(self, rows) -> int:
        from .output import ExporterRegistry
        from .utils import EXIT_SUCCESS

        exporter = ExporterRegistry.resolve(self.ctx.output_format)
        envelope = {
            "kind": self.config.command_name.replace("-", "_"),
            "machine": self.ctx.machine,
            "start": self.ctx.start_date,
            "end": self.ctx.end_date,
            "columns": [
                {"key": c.key, "header": c.header, "width": c.width, "format": c.format}
                for c in self.config.columns
            ],
            "rows": rows,
        }
        exporter.emit(envelope, ctx=self.ctx, config=self.config)
        return EXIT_SUCCESS


class BaseSyncCommand(BaseCommand):
    """Base for the sync subcommand. Wired in Phase 4."""
