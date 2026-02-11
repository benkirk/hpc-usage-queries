"""Shared Click options and utilities for CLI commands."""

import click
from datetime import datetime, timedelta
from typing import Callable


def machine_option(allow_all: bool = True) -> Callable:
    """Machine selection option decorator.

    Args:
        allow_all: If True, include 'all' choice for syncing both machines
    """
    choices = ["casper", "derecho", "all"] if allow_all else ["casper", "derecho"]
    default = "all" if allow_all else "derecho"
    return click.option(
        "-m", "--machine",
        type=click.Choice(choices),
        default=default,
        show_default=True,
        help="Machine to sync from" if allow_all else "Machine name"
    )


def date_options() -> Callable:
    """Decorator that adds --date, --start, --end options."""
    def decorator(func):
        func = click.option(
            "-d", "--date",
            type=str,
            help="Sync jobs for a specific date (YYYY-MM-DD)"
        )(func)
        func = click.option(
            "--start",
            type=str,
            help="Start date for range (YYYY-MM-DD, default: 2024-01-01)"
        )(func)
        func = click.option(
            "--end",
            type=str,
            help="End date for range (YYYY-MM-DD, default: yesterday)"
        )(func)
        return func
    return decorator


def sync_options() -> Callable:
    """Decorator that adds common sync options."""
    def decorator(func):
        func = click.option(
            "--batch-size",
            type=int,
            default=1000,
            show_default=True,
            help="Number of records per batch insert"
        )(func)
        func = click.option(
            "--dry-run",
            is_flag=True,
            help="Fetch and parse data but don't insert into database"
        )(func)
        func = click.option(
            "-v", "--verbose",
            is_flag=True,
            help="Enable verbose output"
        )(func)
        func = click.option(
            "--force",
            is_flag=True,
            help="Force sync even for days already summarized"
        )(func)
        func = click.option(
            "--no-summary",
            is_flag=True,
            help="Skip generating daily summaries after sync"
        )(func)
        func = click.option(
            "--summary-only",
            is_flag=True,
            help="Only regenerate summaries (no data fetch)"
        )(func)
        return func
    return decorator


def validate_dates(date: str | None, start: str | None, end: str | None) -> None:
    """Validate date arguments.

    Raises:
        click.BadParameter: If dates are invalid or conflicting
    """
    if date and (start or end):
        raise click.BadParameter("Cannot use --date with --start/--end")

    for date_str, name in [(date, "--date"), (start, "--start"), (end, "--end")]:
        if date_str:
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                raise click.BadParameter(f"{name} must be in YYYY-MM-DD format")


def print_sync_stats(stats: dict, machine: str, verbose: bool = False) -> None:
    """Print sync statistics in consistent format."""
    click.echo(f"\nSync complete for {machine}:")
    click.echo(f"  Fetched:  {stats['fetched']:,}")
    click.echo(f"  Inserted: {stats['inserted']:,}")
    click.echo(f"  Skipped:  {stats['fetched'] - stats['inserted'] - stats['errors']:,} (duplicates)")
    click.echo(f"  Errors:   {stats['errors']:,}")

    if stats.get("days_skipped", 0) > 0:
        click.echo(f"  Days skipped: {stats['days_skipped']} (already summarized)")
    if stats.get("days_failed", 0) > 0:
        click.echo(f"  Days failed: {stats['days_failed']} (missing accounting data)")
        if verbose and stats.get("failed_days"):
            click.echo(f"    Failed dates: {', '.join(stats['failed_days'])}")
    if stats.get("days_summarized", 0) > 0:
        click.echo(f"  Days summarized: {stats['days_summarized']}")

    # Per-machine breakdown if machine='all'
    if machine == "all" and "machines" in stats:
        click.echo("\nPer-machine breakdown:")
        for m, mstats in stats["machines"].items():
            click.echo(f"\n  {m}:")
            click.echo(f"    Fetched:  {mstats['fetched']:,}")
            click.echo(f"    Inserted: {mstats['inserted']:,}")
            click.echo(f"    Errors:   {mstats['errors']:,}")
