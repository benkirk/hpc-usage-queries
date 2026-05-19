"""History command classes.

One :class:`BaseHistoryCommand` subclass per subcommand. Each ``execute()``
runs its query, hands the rows to a builder for envelope construction,
then emits via the active :class:`Exporter`.
"""

from ..core import (
    BaseHistoryCommand,
    EXIT_ERROR,
    EXIT_SUCCESS,
    ExporterRegistry,
)
from . import builders


class _HistoryCommandMixin:
    """Shared emit + group_by resolution for history commands."""

    def _resolved_group_by(self, group_by_override):
        return group_by_override or self.ctx.group_by

    def _emit(self, envelope) -> int:
        ExporterRegistry.resolve(self.ctx.output_format).emit(envelope, ctx=self.ctx)
        return EXIT_SUCCESS


class _JobsPerEntityCommand(_HistoryCommandMixin, BaseHistoryCommand):
    """Common logic for jobs-per-user and jobs-per-project."""

    primary_entity: str = "user"

    def execute(self, *, group_by=None, verbose: bool = False) -> int:
        try:
            period = self._resolved_group_by(group_by)
            rows = self.get_queries().jobs_by_entity_period(
                primary_entity=self.primary_entity,
                start=self.ctx.start_date,
                end=self.ctx.end_date,
                period=period,
            )
            # The builder needs the resolved group_by; reflect it on ctx so
            # the envelope carries the value actually used.
            previous_group_by = self.ctx.group_by
            self.ctx.group_by = period
            try:
                envelope = builders.build_jobs_per_entity(
                    rows, ctx=self.ctx,
                    primary_entity=self.primary_entity, verbose=verbose,
                )
                return self._emit(envelope)
            finally:
                self.ctx.group_by = previous_group_by
        except Exception as exc:
            return self.handle_exception(exc)


class JobsPerUserCommand(_JobsPerEntityCommand):
    primary_entity = "user"


class JobsPerProjectCommand(_JobsPerEntityCommand):
    primary_entity = "account"


class UniqueProjectsCommand(_HistoryCommandMixin, BaseHistoryCommand):
    def execute(self, *, group_by=None) -> int:
        try:
            period = self._resolved_group_by(group_by)
            rows = self.get_queries().unique_projects_by_period(
                start=self.ctx.start_date,
                end=self.ctx.end_date,
                period=period,
            )
            previous_group_by = self.ctx.group_by
            self.ctx.group_by = period
            try:
                envelope = builders.build_unique_projects(rows, ctx=self.ctx)
                return self._emit(envelope)
            finally:
                self.ctx.group_by = previous_group_by
        except Exception as exc:
            return self.handle_exception(exc)


class UniqueUsersCommand(_HistoryCommandMixin, BaseHistoryCommand):
    def execute(self, *, group_by=None) -> int:
        try:
            period = self._resolved_group_by(group_by)
            rows = self.get_queries().unique_users_by_period(
                start=self.ctx.start_date,
                end=self.ctx.end_date,
                period=period,
            )
            previous_group_by = self.ctx.group_by
            self.ctx.group_by = period
            try:
                envelope = builders.build_unique_users(rows, ctx=self.ctx)
                return self._emit(envelope)
            finally:
                self.ctx.group_by = previous_group_by
        except Exception as exc:
            return self.handle_exception(exc)


class DailySummaryCommand(_HistoryCommandMixin, BaseHistoryCommand):
    def execute(self) -> int:
        try:
            rows = self.get_queries().daily_summary_report(
                start=self.ctx.start_date,
                end=self.ctx.end_date,
            )
            envelope = builders.build_daily_summary(rows, ctx=self.ctx)
            return self._emit(envelope)
        except Exception as exc:
            return self.handle_exception(exc)
