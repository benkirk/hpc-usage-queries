"""Top-level ``jobhist`` Click entry point — SAM-aligned.

Builds a :class:`Context`, dispatches to history / resource / sync command
classes. Old ``job_history/cli.py`` remains in place until Phase 5 swaps
the ``[project.scripts]`` binding over.
"""

import sys
from pathlib import Path

import click

from job_history.cli.core import (
    Context,
    EXIT_KEYBOARD_INTERRUPT,
    parse_date,
)
from job_history.cli.core.file_exporters import register_file_exporters
from job_history.cli.history import (
    JobsPerUserCommand,
    JobsPerProjectCommand,
    UniqueProjectsCommand,
    UniqueUsersCommand,
    DailySummaryCommand,
)
from job_history.cli.resource import RESOURCE_REPORTS, ResourceCommand
from job_history.cli.search import SearchCommand
from job_history.cli.sync import sync as sync_command


# Make dat / csv / md / json-file available from the moment the CLI loads.
register_file_exporters()


GROUP_BY_CHOICES = click.Choice(["day", "month", "quarter", "year"])
MACHINE_CHOICES_HISTORY = click.Choice(["casper", "derecho"])
MACHINE_CHOICES_RESOURCE = click.Choice(["casper", "derecho", "all"])
# Top-level format: None means "let the group pick its sensible default"
# (history → rich, resource → dat). This preserves the legacy UX where
# `jobhist resource pie-user-cpu` wrote a .dat file by default.
FORMAT_CHOICES = click.Choice(["rich", "json", "dat", "csv", "md", "json-file"])


@click.group(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--format", "output_format", type=FORMAT_CHOICES, default=None,
              help="Output format. Defaults: 'rich' for history, 'dat' for resource. "
                   "rich/json go to stdout; dat/csv/md/json-file go to --output-dir.")
@click.option("--verbose", "-v", is_flag=True, default=False,
              help="Verbose error output (full traceback on failure).")
@click.pass_context
def cli(ctx, output_format, verbose):
    """A CLI for querying HPC job history (SAM-aligned)."""
    ctx.obj = Context()
    ctx.obj.output_format = output_format  # may be None; group resolves default
    ctx.obj.verbose = verbose


@cli.group(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--start-date", type=str, callback=parse_date,
              help="Start date for analysis (YYYY-MM-DD).")
@click.option("--end-date", type=str, callback=parse_date,
              help="End date for analysis (YYYY-MM-DD).")
@click.option("--group-by", type=GROUP_BY_CHOICES, default="day", show_default=True,
              help="Group results by day, month, quarter, or year.")
@click.option("-m", "--machine", type=MACHINE_CHOICES_HISTORY, default="derecho", show_default=True,
              help="The machine to query.")
@click.pass_obj
def history(jh_ctx: Context, start_date, end_date, group_by, machine):
    """Time history view of job data."""
    jh_ctx.start_date = start_date
    jh_ctx.end_date = end_date
    jh_ctx.group_by = group_by
    jh_ctx.machine = machine
    # Resolve the group's default output format if the top-level didn't pin one.
    if jh_ctx.output_format is None:
        jh_ctx.output_format = "rich"
    from job_history.database import get_session
    jh_ctx.session = get_session(machine)


@cli.group(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--start-date", type=str, callback=parse_date,
              help="Start date for analysis (YYYY-MM-DD).")
@click.option("--end-date", type=str, callback=parse_date,
              help="End date for analysis (YYYY-MM-DD).")
@click.option("-m", "--machine", type=MACHINE_CHOICES_RESOURCE, default="derecho", show_default=True,
              help="The machine to query (use 'all' for both).")
@click.option("--group-by", type=GROUP_BY_CHOICES, default="day", show_default=True,
              help="Group time-series results by day, month, quarter, or year.")
@click.option("--output-dir",
              type=click.Path(file_okay=False, dir_okay=True, writable=True, resolve_path=True),
              default=".", show_default=True,
              help="Directory to save dat/csv/md/json-file output.")
@click.pass_obj
def resource(jh_ctx: Context, start_date, end_date, machine, group_by, output_dir):
    """Resource-centric view of job data."""
    jh_ctx.start_date = start_date
    jh_ctx.end_date = end_date
    jh_ctx.machine = machine
    jh_ctx.group_by = group_by
    jh_ctx.output_dir = Path(output_dir)
    # Resource keeps its legacy default of 'dat' if the top-level didn't override.
    if jh_ctx.output_format is None:
        jh_ctx.output_format = "dat"
    # Single-machine: open the session for ResourceCommand.get_queries().
    # For machine='all' the multi_machine_query classmethod handles its own
    # sessions, so we leave jh_ctx.session None to avoid an extra open/close.
    if machine != "all":
        from job_history.database import get_session
        jh_ctx.session = get_session(machine)


def _make_resource_callback(config):
    """Build a Click callback bound to one ReportConfig.

    Each generated callback instantiates a fresh :class:`ResourceCommand`
    with the active Context and returns its exit code via ``sys.exit``.
    """
    @click.pass_obj
    def _callback(jh_ctx: Context):
        code = ResourceCommand(jh_ctx, config).execute()
        _close_session(jh_ctx)
        sys.exit(code)
    _callback.__name__ = config.command_name.replace("-", "_")
    _callback.__doc__ = config.description
    return _callback


# Register every resource subcommand declaratively from RESOURCE_REPORTS.
for _config in RESOURCE_REPORTS:
    resource.command(_config.command_name, help=_config.description)(
        _make_resource_callback(_config)
    )


# Register the existing sync command as a top-level subcommand.
cli.add_command(sync_command, name="sync")


@history.command("jobs-per-user")
@click.option("--group-by", type=GROUP_BY_CHOICES, default=None,
              help="Override the group-by setting from the history group.")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Show account column in addition to user.")
@click.pass_obj
def jobs_per_user(jh_ctx: Context, group_by, verbose):
    """Number of jobs per user, grouped by time period."""
    code = JobsPerUserCommand(jh_ctx).execute(group_by=group_by, verbose=verbose)
    _close_session(jh_ctx)
    sys.exit(code)


@history.command("jobs-per-project")
@click.option("--group-by", type=GROUP_BY_CHOICES, default=None,
              help="Override the group-by setting from the history group.")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Show user column in addition to account.")
@click.pass_obj
def jobs_per_project(jh_ctx: Context, group_by, verbose):
    """Number of jobs per project (account), grouped by time period."""
    code = JobsPerProjectCommand(jh_ctx).execute(group_by=group_by, verbose=verbose)
    _close_session(jh_ctx)
    sys.exit(code)


@history.command("unique-projects")
@click.option("--group-by", type=GROUP_BY_CHOICES, default=None,
              help="Override the group-by setting from the history group.")
@click.pass_obj
def unique_projects(jh_ctx: Context, group_by):
    """Number of unique projects per time period."""
    code = UniqueProjectsCommand(jh_ctx).execute(group_by=group_by)
    _close_session(jh_ctx)
    sys.exit(code)


@history.command("unique-users")
@click.option("--group-by", type=GROUP_BY_CHOICES, default=None,
              help="Override the group-by setting from the history group.")
@click.pass_obj
def unique_users(jh_ctx: Context, group_by):
    """Number of unique users per time period."""
    code = UniqueUsersCommand(jh_ctx).execute(group_by=group_by)
    _close_session(jh_ctx)
    sys.exit(code)


@history.command("daily-summary")
@click.pass_obj
def daily_summary(jh_ctx: Context):
    """Daily breakdown by user/account/queue (jobs, CPU-h, GPU-h, mem-h)."""
    code = DailySummaryCommand(jh_ctx).execute()
    _close_session(jh_ctx)
    sys.exit(code)


@cli.command("search", context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--start-date", type=str, callback=parse_date,
              help="Start date for the search window (YYYY-MM-DD); filters on Job.end.")
@click.option("--end-date", type=str, callback=parse_date,
              help="End date for the search window (YYYY-MM-DD); filters on Job.end.")
@click.option("-m", "--machine", type=MACHINE_CHOICES_HISTORY, default="derecho", show_default=True,
              help="The machine to query.")
@click.option("--user", default=None, help="Filter by username.")
@click.option("--project", "account", default=None,
              help="Filter by project (account) code.")
@click.option("--queue", default=None, help="Filter by queue name.")
@click.option("--status", default=None,
              help="Filter by job status (e.g. 'F' for finished).")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Show all columns instead of the default subset.")
@click.option("--display", default=None,
              help="Comma-separated list of columns to display (overrides --verbose).")
@click.pass_obj
def search(jh_ctx: Context, start_date, end_date, machine,
           user, account, queue, status, verbose, display):
    """List individual job records matching the filters."""
    jh_ctx.start_date = start_date
    jh_ctx.end_date = end_date
    jh_ctx.machine = machine
    if jh_ctx.output_format is None:
        jh_ctx.output_format = "rich"
    from job_history.database import get_session
    jh_ctx.session = get_session(machine)
    code = SearchCommand(jh_ctx).execute(
        user=user, account=account, queue=queue, status=status,
        verbose=verbose, display=display,
    )
    _close_session(jh_ctx)
    sys.exit(code)


def _close_session(jh_ctx: Context) -> None:
    if jh_ctx.session is not None:
        try:
            jh_ctx.session.close()
        except Exception:
            pass
        jh_ctx.session = None


def main():
    """Process-level entry point — handles Ctrl-C cleanly."""
    try:
        cli()
    except KeyboardInterrupt:
        sys.exit(EXIT_KEYBOARD_INTERRUPT)


if __name__ == "__main__":
    main()
