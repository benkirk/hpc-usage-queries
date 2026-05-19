"""Context object carried through all jobhist CLI commands.

Mirrors project_samuel's src/cli/core/context.py: a single mutable bag of
shared state (session, console, format, flags) populated by the top-level
Click group and consumed by every command class.
"""

import sys
from datetime import date
from pathlib import Path
from typing import Optional

from rich.console import Console
from sqlalchemy.orm import Session


class Context:
    """Shared context for jobhist CLI commands."""

    def __init__(self):
        # Database / query state — set by the top-level Click group once a
        # machine has been resolved.
        self.session: Optional[Session] = None
        self.machine: str = "derecho"          # "casper" | "derecho" | "all"

        # Time window + grouping (history + resource commands).
        self.start_date: Optional[date] = None
        self.end_date: Optional[date] = None
        self.group_by: str = "day"             # "day" | "month" | "quarter" | "year"

        # Output format dispatch.
        self.output_format: str = "rich"       # rich | json | dat | csv | md
        self.output_dir: Optional[Path] = None  # required for dat/csv/md/json-file

        # Logging / display.
        self.verbose: bool = False
        self.console = Console()
        self.stderr_console = Console(file=sys.stderr)
