"""Resource command execution.

A single :class:`ResourceCommand` drives every :class:`ReportConfig`
in ``RESOURCE_REPORTS``. It extends :class:`BaseResourceCommand` from
``cli.core.base`` with two behaviours specific to resource reports:

1. **Multi-machine fan-out** — when ``ctx.machine == "all"`` the query
   is delegated to ``JobQueries.multi_machine_query`` (which opens its
   own sessions per machine and tags rows with the machine name).
2. **Period injection** — for time-series query methods listed in
   :data:`PERIODIC_QUERY_METHODS` the resource group's ``--group-by``
   value is added to the query kwargs.
"""

from ..core import BaseResourceCommand, EXIT_SUCCESS, ExporterRegistry
from .reports import PERIODIC_QUERY_METHODS, ReportConfig


class ResourceCommand(BaseResourceCommand):
    """Concrete resource-report driver. Wired in ``cli/cmds/jobhist.py``."""

    def __init__(self, ctx, config: ReportConfig):
        super().__init__(ctx, config)

    def _run_query(self):
        query_params = dict(self.config.query_params)
        if self.config.query_method in PERIODIC_QUERY_METHODS:
            query_params["period"] = self.ctx.group_by

        if self.ctx.machine == "all":
            from job_history.queries import JobQueries
            return JobQueries.multi_machine_query(
                machines=["casper", "derecho"],
                method_name=self.config.query_method,
                start=self.ctx.start_date,
                end=self.ctx.end_date,
                **query_params,
            )

        # Single-machine: BaseResourceCommand pattern, but with merged params.
        method = getattr(self.get_queries(), self.config.query_method)
        return method(
            start=self.ctx.start_date,
            end=self.ctx.end_date,
            **query_params,
        )

    def _emit(self, rows) -> int:
        exporter = ExporterRegistry.resolve(self.ctx.output_format)
        envelope = {
            "kind": self.config.command_name.replace("-", "_"),
            "machine": self.ctx.machine,
            "start": self.ctx.start_date,
            "end": self.ctx.end_date,
            "group_by": self.ctx.group_by,
            "columns": [
                {"key": c.key, "header": c.header, "width": c.width, "format": c.format}
                for c in self.config.columns
            ],
            "rows": rows,
        }
        exporter.emit(envelope, ctx=self.ctx, config=self.config)
        return EXIT_SUCCESS
