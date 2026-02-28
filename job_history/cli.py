"""Command-line interface for qhist-queries."""

import click
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Dict, Any

# Import sync command
from .sync.cli import sync

@dataclass
class ColumnSpec:
    """Specification for a single output column."""
    key: str          # Dict key from query result
    header: str       # Column header in output file
    width: int        # Column width for formatting (0 = last column, no padding)
    format: str       # Format spec: "s" (string), ".1f" (float with 1 decimal), ".4f", "" (no format)

@dataclass
class ReportConfig:
    """Configuration for a single resource report."""
    command_name: str                    # CLI command name
    description: str                     # Help text
    query_method: str                    # Method name on JobQueries object
    query_params: Dict[str, Any]         # Parameters to pass to query method
    filename_base: str                   # Base name for output file
    columns: List[ColumnSpec]            # Column specifications

    def get_filename(self, machine: str, start: date, end: date) -> str:
        """Generate filename using standard pattern."""
        prefix = machine[:2].capitalize()
        return f"{prefix}_{self.filename_base}_{start}_{end}.dat"


def parse_date(ctx, param, value):
    """Callback to parse date strings into date objects."""
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise click.BadParameter("Date must be in YYYY-MM-DD format.")

@click.group()
def cli():
    """A CLI for querying HPC job history."""
    pass

from .database import get_session
from .queries import JobQueries
from .exporters import get_exporter


@click.group(invoke_without_command=True)
@click.option("--start-date", type=str, callback=parse_date, help="Start date for analysis (YYYY-MM-DD).")
@click.option("--end-date", type=str, callback=parse_date, help="End date for analysis (YYYY-MM-DD).")
@click.option("--group-by", type=click.Choice(["day", "month", "quarter", "year"]), default="day", help="Group results by day, month, quarter, or year.")
@click.option("-m", "--machine", type=click.Choice(["casper", "derecho"]), default="derecho", help="The machine to query.")
@click.pass_context
def history(ctx, start_date, end_date, group_by, machine):
    """Time history view of job data."""
    ctx.ensure_object(dict)
    ctx.obj['start_date'] = start_date
    ctx.obj['end_date'] = end_date
    ctx.obj['group_by'] = group_by
    ctx.obj['machine'] = machine
    if ctx.invoked_subcommand is None:
        click.echo(f"History view for {machine} from {start_date} to {end_date}, grouped by {group_by}")

from rich.table import Table
from rich.console import Console


def _run_jobs_per_entity_report(ctx, group_by, primary_entity: str, verbose: bool):
    """Common implementation for jobs-per-user and jobs-per-project commands."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']

    if group_by is None:
        group_by = ctx.obj['group_by']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.jobs_by_entity_period(
        primary_entity=primary_entity,
        start=start_date,
        end=end_date,
        period=group_by
    )

    # If not verbose, aggregate data to collapse secondary dimension
    if not verbose:
        from collections import defaultdict
        aggregated = defaultdict(int)

        # Group by (period, primary_entity) and sum job counts
        for row in data:
            if primary_entity == "user":
                key = (row['period'], row['user'])
            else:  # "account"
                key = (row['period'], row['account'])
            aggregated[key] += row['job_count']

        # Convert back to list of dicts
        data = []
        for (period, entity), job_count in sorted(aggregated.items()):
            if primary_entity == "user":
                data.append({'period': period, 'user': entity, 'job_count': job_count})
            else:  # "account"
                data.append({'period': period, 'account': entity, 'job_count': job_count})

    console = Console()

    # Determine columns based on verbose flag
    if primary_entity == "user":
        if verbose:
            table_columns = ("Period", "User", "Account", "Job Count")
        else:
            table_columns = ("Period", "User", "Job Count")
    else:  # "account"
        if verbose:
            table_columns = ("Period", "Account", "User", "Job Count")
        else:
            table_columns = ("Period", "Account", "Job Count")

    table = Table(*table_columns)

    key_map = {"Period": "period", "User": "user", "Account": "account", "Job Count": "job_count"}
    for row in data:
        table.add_row(*[str(row[key_map[col]]) for col in table_columns])

    console.print(table)
    session.close()


@history.command("jobs-per-user")
@click.option("--group-by", type=click.Choice(["day", "month", "quarter", "year"]),
              help="Group results by day, month, quarter, or year (overrides history setting).")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Show account column in addition to user.")
@click.pass_context
def jobs_per_user(ctx, group_by, verbose):
    """Prints the number of jobs per user."""
    _run_jobs_per_entity_report(ctx, group_by, "user", verbose)


@history.command("jobs-per-project")
@click.option("--group-by", type=click.Choice(["day", "month", "quarter", "year"]),
              help="Group results by day, month, quarter, or year (overrides history setting).")
@click.option("-v", "--verbose", is_flag=True, default=False,
              help="Show user column in addition to account.")
@click.pass_context
def jobs_per_project(ctx, group_by, verbose):
    """Prints the number of jobs per project (account)."""
    _run_jobs_per_entity_report(ctx, group_by, "account", verbose)

@history.command("unique-projects")
@click.option("--group-by", type=click.Choice(["day", "month", "quarter", "year"]), help="Group results by day, month, quarter, or year (overrides history setting).")
@click.pass_context
def unique_projects(ctx, group_by):
    """Prints the number of unique projects."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    
    # Use provided group_by or fall back to context
    if group_by is None:
        group_by = ctx.obj['group_by']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.unique_projects_by_period(start=start_date, end=end_date, period=group_by)
    
    console = Console()
    table = Table("Period", "Unique Projects")
    for row in data:
        table.add_row(row['period'], str(row['project_count']))
        
    console.print(table)

    session.close()

@history.command("unique-users")
@click.option("--group-by", type=click.Choice(["day", "month", "quarter", "year"]), help="Group results by day, month, quarter, or year (overrides history setting).")
@click.pass_context
def unique_users(ctx, group_by):
    """Prints the number of unique users."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    
    # Use provided group_by or fall back to context
    if group_by is None:
        group_by = ctx.obj['group_by']
    
    session = get_session(machine)
    queries = JobQueries(session)
    
    data = queries.unique_users_by_period(start=start_date, end=end_date, period=group_by)
    
    console = Console()
    table = Table("Period", "Unique Users")
    for row in data:
        table.add_row(row['period'], str(row['user_count']))
        
    console.print(table)

    session.close()


@history.command("daily-summary")
@click.pass_context
def daily_summary_cmd(ctx):
    """Show daily usage summary by date, user, account, and queue."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.daily_summary_report(start=start_date, end=end_date)

    console = Console()
    table = Table("Date", "User", "Account", "Queue", "Jobs",
                  "CPU-h", "GPU-h", "Mem-h")
    for row in data:
        table.add_row(
            row["date"],
            row["user"],
            row["account"],
            row["queue"],
            str(row["job_count"]),
            f"{row['cpu_hours']:.1f}",
            f"{row['gpu_hours']:.1f}",
            f"{row['memory_hours']:.1f}",
        )

    console.print(table)
    session.close()


cli.add_command(history)


class ColumnSpecs:
    """Factory for common column specifications.

    Reduces duplication in RESOURCE_REPORTS by providing reusable
    column spec patterns for different report types.
    """

    @staticmethod
    def usage_counts(label: str = "User-ids", label_width: int = 15) -> List[ColumnSpec]:
        """Standard usage/counts columns for pie charts.

        Used by: pie-user-cpu, pie-user-gpu, pie-proj-cpu, pie-proj-gpu,
                 pie-group-cpu, pie-group-gpu

        Args:
            label: Label for the first column (e.g., "User-ids", "Accounts")
            label_width: Width for the label column (default 15)

        Returns:
            List of 3 ColumnSpec objects
        """
        return [
            ColumnSpec("label", label, label_width, "s"),
            ColumnSpec("usage_hours", "Usage", 15, ".1f"),
            ColumnSpec("job_count", "Counts", 0, ""),
        ]

    @staticmethod
    def range_waits(range_label: str) -> List[ColumnSpec]:
        """Standard columns for wait time reports.

        Used by: gpu-job-waits, cpu-job-waits, job-waits, memory-job-waits

        Args:
            range_label: Label for the range column (e.g., "GPUs", "Nodes", "Cores")

        Returns:
            List of 3 ColumnSpec objects
        """
        return [
            ColumnSpec("range_label", range_label, 20, "s"),
            ColumnSpec("avg_wait_hours", "AveWait-hrs", 12, ".4f"),
            ColumnSpec("job_count", "#-Jobs", 0, ""),
        ]

    @staticmethod
    def range_sizes(range_label: str) -> List[ColumnSpec]:
        """Standard columns for job size reports.

        Used by: gpu-job-sizes, cpu-job-sizes, job-sizes, memory-job-sizes

        Args:
            range_label: Label for the range column (e.g., "GPUs", "Nodes", "Cores")

        Returns:
            List of 4 ColumnSpec objects
        """
        return [
            ColumnSpec("range_label", range_label, 20, "s"),
            ColumnSpec("job_count", "#-Jobs", 12, ""),
            ColumnSpec("user_count", "#-Users", 12, ""),
            ColumnSpec("hours", "Cr-hrs", 0, ".1f"),
        ]

    @staticmethod
    def duration_buckets() -> List[ColumnSpec]:
        """Standard columns for duration histograms.

        Used by: gpu-job-durations, cpu-job-durations

        Returns:
            List of 8 ColumnSpec objects
        """
        return [
            ColumnSpec("date", "Date", 20, "s"),
            ColumnSpec("<30s", "<30s", 12, ".1f"),
            ColumnSpec("30s-30m", "30s-30m", 12, ".1f"),
            ColumnSpec("30-60m", "30-60m", 12, ".1f"),
            ColumnSpec("1-5h", "1-5h", 12, ".1f"),
            ColumnSpec("5-12h", "5-12h", 12, ".1f"),
            ColumnSpec("12-18h", "12-18h", 12, ".1f"),
            ColumnSpec(">18h", ">18h", 0, ".1f"),
        ]

    @staticmethod
    def memory_per_rank_buckets() -> List[ColumnSpec]:
        """Standard columns for memory-per-rank histograms.

        Used by: cpu-job-memory-per-rank, gpu-job-memory-per-rank

        Returns:
            List of 13 ColumnSpec objects
        """
        return [
            ColumnSpec("date", "Date", 20, "s"),
            ColumnSpec("<128MB", "<128MB", 12, ".1f"),
            ColumnSpec("128MB-512MB", "128MB-512MB", 14, ".1f"),
            ColumnSpec("512MB-1GB", "512MB-1GB", 12, ".1f"),
            ColumnSpec("1-2GB", "1-2GB", 12, ".1f"),
            ColumnSpec("2-4GB", "2-4GB", 12, ".1f"),
            ColumnSpec("4-8GB", "4-8GB", 12, ".1f"),
            ColumnSpec("8-16GB", "8-16GB", 12, ".1f"),
            ColumnSpec("16-32GB", "16-32GB", 12, ".1f"),
            ColumnSpec("32-64GB", "32-64GB", 12, ".1f"),
            ColumnSpec("64-128GB", "64-128GB", 12, ".1f"),
            ColumnSpec("128-256GB", "128-256GB", 12, ".1f"),
            ColumnSpec(">256GB", ">256GB", 0, ".1f"),
        ]

    @staticmethod
    def usage_history() -> List[ColumnSpec]:
        """Standard columns for usage history report.

        Used by: usage-history

        Returns:
            List of 11 ColumnSpec objects
        """
        return [
            ColumnSpec("Date", "Date", 18, "s"),
            ColumnSpec("#-Users", "#-Users", 12, ""),
            ColumnSpec("#-Proj", "#-Proj", 8, ""),
            ColumnSpec("#-CPU-Users", "#-CPU-Users", 13, ""),
            ColumnSpec("#-CPU-Proj", "#-CPU-Proj", 13, ""),
            ColumnSpec("#-CPU-Jobs", "#-CPU-Jobs", 13, ""),
            ColumnSpec("#-CPU-Hrs", "#-CPU-Hrs", 12, ".1f"),
            ColumnSpec("#-GPU-Users", "#-GPU-Users", 13, ""),
            ColumnSpec("#-GPU-Proj", "#-GPU-Proj", 13, ""),
            ColumnSpec("#-GPU-Jobs", "#-GPU-Jobs", 13, ""),
            ColumnSpec("#-GPU-Hrs", "#-GPU-Hrs", 0, ".1f"),
        ]


# Resource report configurations
RESOURCE_REPORTS = [
    # Pie chart reports - Usage by group
    ReportConfig(
        command_name="pie-proj-cpu",
        description="CPU usage by project (account)",
        query_method="usage_by_group",
        query_params={"resource_type": "cpu", "group_by": "account"},
        filename_base="pie_proj_cpu",
        columns=ColumnSpecs.usage_counts(label="Accounts")
    ),
    ReportConfig(
        command_name="pie-user-gpu",
        description="GPU usage by user",
        query_method="usage_by_group",
        query_params={"resource_type": "gpu", "group_by": "user"},
        filename_base="pie_user_gpu",
        columns=ColumnSpecs.usage_counts(label="User-ids")
    ),
    ReportConfig(
        command_name="pie-user-cpu",
        description="CPU usage by user",
        query_method="usage_by_group",
        query_params={"resource_type": "cpu", "group_by": "user"},
        filename_base="pie_user_cpu",
        columns=ColumnSpecs.usage_counts(label="User-ids")
    ),
    ReportConfig(
        command_name="pie-proj-gpu",
        description="GPU usage by project (account)",
        query_method="usage_by_group",
        query_params={"resource_type": "gpu", "group_by": "account"},
        filename_base="pie_proj_gpu",
        columns=ColumnSpecs.usage_counts(label="Accounts")
    ),
    ReportConfig(
        command_name="pie-group-gpu",
        description="GPU usage by account",
        query_method="usage_by_group",
        query_params={"resource_type": "gpu", "group_by": "account"},
        filename_base="pie_group_gpu",
        columns=ColumnSpecs.usage_counts(label="Accounts")
    ),
    ReportConfig(
        command_name="pie-group-cpu",
        description="CPU usage by account",
        query_method="usage_by_group",
        query_params={"resource_type": "cpu", "group_by": "account"},
        filename_base="pie_group_cpu",
        columns=ColumnSpecs.usage_counts(label="Accounts")
    ),

    # Duration reports
    ReportConfig(
        command_name="gpu-job-durations",
        description="GPU job durations by period",
        query_method="job_durations",
        query_params={"resource_type": "gpu"},
        filename_base="gpu_job_durations",
        columns=ColumnSpecs.duration_buckets()
    ),
    ReportConfig(
        command_name="cpu-job-durations",
        description="CPU job durations by period",
        query_method="job_durations",
        query_params={"resource_type": "cpu"},
        filename_base="cpu_job_durations",
        columns=ColumnSpecs.duration_buckets()
    ),

    # Memory-per-rank histogram reports
    ReportConfig(
        command_name="cpu-job-memory-per-rank",
        description="CPU job memory-per-rank histogram by period",
        query_method="job_memory_per_rank",
        query_params={"resource_type": "cpu"},
        filename_base="cpu_job_memory_per_rank",
        columns=ColumnSpecs.memory_per_rank_buckets()
    ),
    ReportConfig(
        command_name="gpu-job-memory-per-rank",
        description="GPU job memory-per-rank histogram by period",
        query_method="job_memory_per_rank",
        query_params={"resource_type": "gpu"},
        filename_base="gpu_job_memory_per_rank",
        columns=ColumnSpecs.memory_per_rank_buckets()
    ),

    # Wait time reports
    ReportConfig(
        command_name="gpu-job-waits",
        description="GPU job waits by GPU count",
        query_method="job_waits_by_resource",
        query_params={"resource_type": "gpu", "range_type": "gpu"},
        filename_base="gpu_job_waits",
        columns=ColumnSpecs.range_waits("GPUs")
    ),
    ReportConfig(
        command_name="cpu-job-waits",
        description="CPU job waits by node count",
        query_method="job_waits_by_resource",
        query_params={"resource_type": "cpu", "range_type": "node"},
        filename_base="cpu_job_waits",
        columns=ColumnSpecs.range_waits("Nodes")
    ),
    ReportConfig(
        command_name="job-waits",
        description="Job waits by core count",
        query_method="job_waits_by_resource",
        query_params={"resource_type": "all", "range_type": "core"},
        filename_base="bycore_job_waits",
        columns=ColumnSpecs.range_waits("Cores")
    ),

    # Job size reports
    ReportConfig(
        command_name="gpu-job-sizes",
        description="GPU job sizes by GPU count",
        query_method="job_sizes_by_resource",
        query_params={"resource_type": "gpu", "range_type": "gpu"},
        filename_base="gpu_job_sizes",
        columns=ColumnSpecs.range_sizes("GPUs")
    ),
    ReportConfig(
        command_name="cpu-job-sizes",
        description="CPU job sizes by node count",
        query_method="job_sizes_by_resource",
        query_params={"resource_type": "cpu", "range_type": "node"},
        filename_base="cpu_job_sizes",
        columns=ColumnSpecs.range_sizes("Nodes")
    ),
    ReportConfig(
        command_name="job-sizes",
        description="Job sizes by core count",
        query_method="job_sizes_by_resource",
        query_params={"resource_type": "all", "range_type": "core"},
        filename_base="bycore_job_sizes",
        columns=ColumnSpecs.range_sizes("Cores")
    ),

    # Usage history report
    ReportConfig(
        command_name="usage-history",
        description="Usage history by period",
        query_method="usage_history",
        query_params={},
        filename_base="usage_history",
        columns=ColumnSpecs.usage_history()
    ),

    # Memory-based reports
    ReportConfig(
        command_name="memory-job-waits",
        description="Job waits by memory requirement",
        query_method="memory_job_waits",
        query_params={},
        filename_base="memory_job_waits",
        columns=ColumnSpecs.range_waits("Memory(GB)")
    ),
    ReportConfig(
        command_name="memory-job-sizes",
        description="Job sizes by memory requirement",
        query_method="memory_job_sizes",
        query_params={},
        filename_base="memory_job_sizes",
        columns=ColumnSpecs.range_sizes("Memory(GB)")
    ),
]

def _write_report(data: List[Dict[str, Any]], config: ReportConfig,
                  machine: str, start: date, end: date, output_dir: str,
                  output_format: str = "dat") -> str:
    """Write report data to file using the specified configuration and format.

    Args:
        data: Query results as list of dicts
        config: Report configuration
        machine: Machine name for filename
        start: Start date for filename
        end: End date for filename
        output_dir: Output directory path
        output_format: Output format ('dat', 'json', 'csv', 'md')

    Returns:
        Full path to written file
    """
    # Get base filename and replace extension based on format
    base_filename = config.get_filename(machine, start, end)
    if output_format != "dat":
        # Replace .dat extension with appropriate format extension
        base_filename = base_filename.replace(".dat", f".{output_format}")

    filepath = os.path.join(output_dir, base_filename)

    # Get appropriate exporter and write file
    exporter = get_exporter(output_format)
    exporter.export(data, config.columns, filepath)

    return filepath

def create_resource_command(config: ReportConfig):
    """Factory function to create a resource command from configuration.

    Generates a Click command that:
    1. Extracts context (dates, machine, output_dir)
    2. Executes the appropriate query method
    3. Writes formatted output to file
    4. Cleans up session

    Args:
        config: ReportConfig specifying command behavior

    Returns:
        Click command function
    """
    @click.pass_context
    def command_func(ctx):
        # Extract context
        start_date = ctx.obj['start_date']
        end_date = ctx.obj['end_date']
        machine = ctx.obj['machine']
        output_dir = ctx.obj['output_dir']
        output_format = ctx.obj.get('output_format', 'dat')
        group_by = ctx.obj.get('group_by', 'day')

        # Prepare query parameters
        query_params = dict(config.query_params)

        # Add period parameter for time-series queries that support it
        if config.query_method in ['usage_history', 'job_durations', 'job_memory_per_rank']:
            query_params['period'] = group_by

        # Execute query (single or multi-machine)
        if machine == "all":
            # Multi-machine query
            machines = ["casper", "derecho"]
            data = JobQueries.multi_machine_query(
                machines=machines,
                method_name=config.query_method,
                **query_params,
                start=start_date,
                end=end_date
            )
            machine_label = "All"
        else:
            # Single machine query
            session = get_session(machine)
            queries = JobQueries(session, machine=machine)
            query_func = getattr(queries, config.query_method)
            data = query_func(**query_params, start=start_date, end=end_date)
            session.close()
            machine_label = machine

        # Write output
        filepath = _write_report(data, config, machine_label, start_date, end_date, output_dir, output_format)
        click.echo(f"Report saved to {filepath}")

    # Set command metadata for Click
    command_func.__name__ = config.command_name.replace("-", "_")
    command_func.__doc__ = config.description

    return command_func

@click.group(invoke_without_command=True)
@click.option("--start-date", type=str, callback=parse_date, help="Start date for analysis (YYYY-MM-DD).")
@click.option("--end-date", type=str, callback=parse_date, help="End date for analysis (YYYY-MM-DD).")
@click.option("-m", "--machine", type=click.Choice(["casper", "derecho", "all"]), default="derecho", help="The machine to query (use 'all' for both).")
@click.option("--output-dir", type=click.Path(file_okay=False, dir_okay=True, writable=True, resolve_path=True), default=".", help="Directory to save the reports.")
@click.option("--format", "output_format", type=click.Choice(["dat", "json", "csv", "md"]), default="dat", help="Output format (dat, json, csv, md).")
@click.option("--group-by", type=click.Choice(["day", "month", "quarter", "year"]), default="day", help="Group time-based results by day, month, quarter, or year.")
@click.pass_context
def resource(ctx, start_date, end_date, machine, output_dir, output_format, group_by):
    """Resource-centric view of job data."""
    ctx.ensure_object(dict)
    ctx.obj['start_date'] = start_date
    ctx.obj['end_date'] = end_date
    ctx.obj['machine'] = machine
    ctx.obj['output_dir'] = output_dir
    ctx.obj['output_format'] = output_format
    ctx.obj['group_by'] = group_by
    if ctx.invoked_subcommand is None:
        machines_desc = "all machines" if machine == "all" else machine
        click.echo(f"Resource view for {machines_desc} from {start_date} to {end_date}, output to {output_dir}")

# Dynamically register all resource commands from configuration
for report_config in RESOURCE_REPORTS:
    command = create_resource_command(report_config)
    resource.command(report_config.command_name)(command)

cli.add_command(resource)

cli.add_command(sync)

if __name__ == "__main__":
    cli()
