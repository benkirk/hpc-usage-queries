"""Exit codes and shared CLI helpers."""

from datetime import datetime

import click


EXIT_SUCCESS = 0
EXIT_NOT_FOUND = 1
EXIT_ERROR = 2
EXIT_KEYBOARD_INTERRUPT = 130


def parse_date(ctx, param, value):
    """Click callback: parse YYYY-MM-DD into a date object."""
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise click.BadParameter("Date must be in YYYY-MM-DD format.")
