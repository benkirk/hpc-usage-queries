"""Command-line interface for qhist-queries."""

import click
from datetime import date, datetime

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

@click.group(invoke_without_command=True)
@click.option("--start-date", type=str, callback=parse_date, help="Start date for analysis (YYYY-MM-DD).")
@click.option("--end-date", type=str, callback=parse_date, help="End date for analysis (YYYY-MM-DD).")
@click.option("--group-by", type=click.Choice(["day", "month", "quarter"]), default="day", help="Group results by day, month, or quarter.")
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

# ... (rest of the imports)

# ... (history command group)

@history.command("jobs-per-user")
@click.pass_context
def jobs_per_user(ctx):
    """Prints the number of jobs per user per account."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    group_by = ctx.obj['group_by']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.jobs_per_user_account_by_period(start=start_date, end=end_date, period=group_by)
    
    console = Console()
    table = Table("Period", "User", "Account", "Job Count")
    for row in data:
        table.add_row(row['period'], row['user'], row['account'], str(row['job_count']))
        
    console.print(table)

    session.close()

@history.command("unique-projects")
@click.pass_context
def unique_projects(ctx):
    """Prints the number of unique projects."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
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
@click.pass_context
def unique_users(ctx):
    """Prints the number of unique users."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
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

cli.add_command(history)

@click.group(invoke_without_command=True)
@click.option("--start-date", type=str, callback=parse_date, help="Start date for analysis (YYYY-MM-DD).")
@click.option("--end-date", type=str, callback=parse_date, help="End date for analysis (YYYY-MM-DD).")
@click.option("-m", "--machine", type=click.Choice(["casper", "derecho"]), default="derecho", help="The machine to query.")
@click.option("--output-dir", type=click.Path(file_okay=False, dir_okay=True, writable=True, resolve_path=True), default=".", help="Directory to save the reports.")
@click.pass_context
def resource(ctx, start_date, end_date, machine, output_dir):
    """Resource-centric view of job data."""
    ctx.ensure_object(dict)
    ctx.obj['start_date'] = start_date
    ctx.obj['end_date'] = end_date
    ctx.obj['machine'] = machine
    ctx.obj['output_dir'] = output_dir
    if ctx.invoked_subcommand is None:
        click.echo(f"Resource view for {machine} from {start_date} to {end_date}, output to {output_dir}")

import os

# ... (other imports)

@resource.command("pie-proj-cpu")
@click.pass_context
def pie_proj_cpu(ctx):
    """Generates a pie chart report of CPU usage grouped by project."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.pie_group_cpu(start=start_date, end=end_date) # Reusing existing query

    filename = f"{machine[:2].capitalize()}_pie_proj_cpu_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = f"{'Accounts':<15}{'Usage':<15}{'Counts'}\n"
        f.write(header)
        for row in data:
            f.write(
                f"{row['Accounts']:<15}{row['Usage']:<15.1f}{row['Counts']}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("pie-user-gpu")
@click.pass_context
def pie_user_gpu(ctx):
    """Generates a pie chart report of GPU usage grouped by user."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.pie_user_gpu(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_pie_user_gpu_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = f"{'User-ids':<15}{'Usage':<15}{'Counts'}\n"
        f.write(header)
        for row in data:
            f.write(
                f"{row['User-ids']:<15}{row['Usage']:<15.1f}{row['Counts']}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("pie-user-cpu")
@click.pass_context
def pie_user_cpu(ctx):
    """Generates a pie chart report of CPU usage grouped by user."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.pie_user_cpu(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_pie_user_cpu_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = f"{'User-ids':<15}{'Usage':<15}{'Counts'}\n"
        f.write(header)
        for row in data:
            f.write(
                f"{row['User-ids']:<15}{row['Usage']:<15.1f}{row['Counts']}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("pie-proj-gpu")
@click.pass_context
def pie_proj_gpu(ctx):
    """Generates a pie chart report of GPU usage grouped by project."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.pie_group_gpu(start=start_date, end=end_date) # Reusing existing query

    filename = f"{machine[:2].capitalize()}_pie_proj_gpu_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = f"{'Accounts':<15}{'Usage':<15}{'Counts'}\n"
        f.write(header)
        for row in data:
            f.write(
                f"{row['Accounts']:<15}{row['Usage']:<15.1f}{row['Counts']}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("pie-group-gpu")
@click.pass_context
def pie_group_gpu(ctx):
    """Generates a pie chart report of GPU usage grouped by account."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.pie_group_gpu(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_pie_group_gpu_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = f"{'Accounts':<15}{'Usage':<15}{'Counts'}\n"
        f.write(header)
        for row in data:
            f.write(
                f"{row['Accounts']:<15}{row['Usage']:<15.1f}{row['Counts']}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("pie-group-cpu")
@click.pass_context
def pie_group_cpu(ctx):
    """Generates a pie chart report of CPU usage grouped by account."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.pie_group_cpu(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_pie_group_cpu_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = f"{'Accounts':<15}{'Usage':<15}{'Counts'}\n"
        f.write(header)
        for row in data:
            f.write(
                f"{row['Accounts']:<15}{row['Usage']:<15.1f}{row['Counts']}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("gpu-job-durations")
@click.pass_context
def gpu_job_durations(ctx):
    """Generates a report on GPU job durations by day."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.gpu_job_durations_by_day(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_gpu_job_durations_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = f"{'Date':<20}{'<30s':<12}{'30s-30m':<12}{'30-60m':<12}{'1-5h':<12}{'5-12h':<12}{'12-18h':<12}{'>18h'}\n"
        f.write(header)
        for row in data:
            f.write(
                f"{row['date']:<20}"
                f"{row['<30s']:<12.1f}"
                f"{row['30s-30m']:<12.1f}"
                f"{row['30-60m']:<12.1f}"
                f"{row['1-5h']:<12.1f}"
                f"{row['5-12h']:<12.1f}"
                f"{row['12-18h']:<12.1f}"
                f"{row['>18h']:.1f}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("gpu-job-waits")
@click.pass_context
def gpu_job_waits(ctx):
    """Generates a report on GPU job waits by GPU count."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.gpu_job_waits_by_gpu_ranges(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_gpu_job_waits_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = f"{'GPUs':<20}{'AveWait-hrs':<12}{'#-Jobs'}\n"
        f.write(header)
        for row in data:
            f.write(
                f"{row['gpu_range']:<20}{row['avg_wait_hours']:<12.4f}{row['job_count']}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("gpu-job-sizes")
@click.pass_context
def gpu_job_sizes(ctx):
    """Generates a report on GPU job sizes by GPU count."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.gpu_job_sizes_by_gpu_ranges(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_gpu_job_sizes_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = f"{'GPUs':<20}{'#-Jobs':<12}{'#-Users':<12}{'Cr-hrs'}\n"
        f.write(header)
        for row in data:
            f.write(
                f"{row['gpu_range']:<20}{row['job_count']:<12}{row['user_count']:<12}{row['gpu_hours']:.1f}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("usage-history")
@click.pass_context
def usage_history(ctx):
    """Generates a report on daily usage history."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.usage_history_by_day(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_usage_history_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = (
            f"{'Date':<18}{'#-Users':<12}{'#-Proj':<8}{'#-CPU-Users':<13}{'#-CPU-Proj':<13}"
            f"{'#-CPU-Jobs':<13}{'#-CPU-Hrs':<12}{'#-GPU-Users':<13}{'#-GPU-Proj':<13}"
            f"{'#-GPU-Jobs':<13}{'#-GPU-Hrs'}\n"
        )
        f.write(header)
        for row in data:
            f.write(
                f"{row['Date']:<18}{row['#-Users']:<12}{row['#-Proj']:<8}{row['#-CPU-Users']:<13}"
                f"{row['#-CPU-Proj']:<13}{row['#-CPU-Jobs']:<13}{row['#-CPU-Hrs']:<12.1f}"
                f"{row['#-GPU-Users']:<13}{row['#-GPU-Proj']:<13}{row['#-GPU-Jobs']:<13}"
                f"{row['#-GPU-Hrs']:.1f}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("cpu-job-waits")
@click.pass_context
def cpu_job_waits(ctx):
    """Generates a report on CPU job waits by node count."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.cpu_job_waits_by_node_ranges(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_cpu_job_waits_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = f"{'Nodes':<20}{'AveWait-hrs':<12}{'#-Jobs'}\n"
        f.write(header)
        for row in data:
            f.write(
                f"{row['node_range']:<20}{row['avg_wait_hours']:<12.4f}{row['job_count']}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("cpu-job-sizes")
@click.pass_context
def cpu_job_sizes(ctx):
    """Generates a report on CPU job sizes by node count."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.cpu_job_sizes_by_node_ranges(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_cpu_job_sizes_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = f"{'Nodes':<20}{'#-Jobs':<12}{'#-Users':<12}{'Cr-hrs'}\n"
        f.write(header)
        for row in data:
            f.write(
                f"{row['node_range']:<20}{row['job_count']:<12}{row['user_count']:<12}{row['core_hours']:.1f}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("cpu-job-durations")
@click.pass_context
def cpu_job_durations(ctx):
    """Generates a report on CPU job durations by day."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.cpu_job_durations_by_day(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_cpu_job_durations_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        header = f"{'Date':<20}{'<30s':<12}{'30s-30m':<12}{'30-60m':<12}{'1-5h':<12}{'5-12h':<12}{'12-18h':<12}{'>18h'}\n"
        f.write(header)
        for row in data:
            f.write(
                f"{row['date']:<20}"
                f"{row['<30s']:<12.1f}"
                f"{row['30s-30m']:<12.1f}"
                f"{row['30-60m']:<12.1f}"
                f"{row['1-5h']:<12.1f}"
                f"{row['5-12h']:<12.1f}"
                f"{row['12-18h']:<12.1f}"
                f"{row['>18h']:.1f}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("job-waits")
@click.pass_context
def job_waits(ctx):
    """Generates a report on job waits by core count."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.job_waits_by_core_ranges(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_bycore_job_waits_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        f.write(f"{'Cores':<20}{'AveWait-hrs':<12}{'#-Jobs'}\n")
        for row in data:
            f.write(
                f"{row['core_range']:<20}{row['avg_wait_hours']:<12.4f}{row['job_count']}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

@resource.command("job-sizes")
@click.pass_context
def job_sizes(ctx):
    """Generates a report on job sizes by core count."""
    start_date = ctx.obj['start_date']
    end_date = ctx.obj['end_date']
    machine = ctx.obj['machine']
    output_dir = ctx.obj['output_dir']

    session = get_session(machine)
    queries = JobQueries(session)

    data = queries.job_sizes_by_core_ranges(start=start_date, end=end_date)

    filename = f"{machine[:2].capitalize()}_bycore_job_sizes_{start_date}_{end_date}.dat"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        f.write(f"{'Cores':<20}{'#-Jobs':<12}{'#-Users':<12}{'Cr-hrs'}\n")
        for row in data:
            f.write(
                f"{row['core_range']:<20}{row['job_count']:<12}{row['user_count']:<12}{row['core_hours']:.1f}\n"
            )

    click.echo(f"Report saved to {filepath}")

    session.close()

cli.add_command(resource)

if __name__ == "__main__":
    cli()
