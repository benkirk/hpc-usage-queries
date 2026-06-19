"""Consolidate subcommand for fs-scans CLI.

Loads finished per-collection SQLite ``.db`` files into PostgreSQL schemas and
atomically swaps them into place.  Requires ``FS_SCAN_DB_BACKEND=postgres``.
"""

from pathlib import Path

import click

from ..cli.common import console, data_dir_option, make_dynamic_help_command
from ..core.config import FsScanConfig
from ..core.database import get_data_dir, set_data_dir


def _resolve_sources(filesystems, data_dir, db):
    """Return a list of (collection, sqlite_path) sources to consolidate."""
    if db is not None:
        if len(filesystems) != 1:
            raise click.UsageError("--db requires exactly one FILESYSTEM argument.")
        return [(filesystems[0].lower(), Path(db))]

    base = data_dir or get_data_dir()
    if filesystems:
        return [(fs.lower(), base / f"{fs.lower()}.db") for fs in filesystems]

    # --all (no explicit filesystems): every .db in the data directory.
    return [(p.stem.lower(), p) for p in sorted(base.glob("*.db"))]


DynamicHelpCommand = make_dynamic_help_command("fs-scans consolidate")


@click.command(cls=DynamicHelpCommand, context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("filesystems", nargs=-1)
@click.option("--all", "all_fs", is_flag=True, help="Consolidate every .db in the data directory.")
@data_dir_option()
@click.option("--db", type=click.Path(exists=True, dir_okay=False, path_type=Path),
              help="Explicit source .db file (single collection).")
@click.option("--no-swap", is_flag=True, help="Load into the staging schema only; do not swap.")
@click.option("--keep-old", is_flag=True, help="Keep the previous <collection>_old schema (more disk).")
@click.option("--no-fk", is_flag=True, help="Skip re-adding/validating foreign keys after load.")
def consolidate_cmd(filesystems, all_fs, data_dir, db, no_swap, keep_old, no_fk):
    """Consolidate SQLite scan databases into PostgreSQL.

    \b
    Examples:
      fs-scans consolidate asp                # one collection
      fs-scans consolidate asp cgd univ       # several
      fs-scans consolidate --all              # every .db in the data dir
      fs-scans consolidate asp --no-swap      # stage only (no swap)
    """
    if FsScanConfig.DB_BACKEND != "postgres":
        console.print(
            "[red]consolidate requires FS_SCAN_DB_BACKEND=postgres[/red] "
            f"(current: {FsScanConfig.DB_BACKEND})."
        )
        raise SystemExit(2)

    if not filesystems and not all_fs and db is None:
        raise click.UsageError("Provide FILESYSTEM(s), --all, or --db.")

    if data_dir is not None:
        set_data_dir(data_dir)

    # Import here so the CLI loads even without psycopg2 installed.
    from ..consolidate.consolidator import consolidate_sqlite_to_postgres

    sources = _resolve_sources(filesystems, data_dir, db)
    if not sources:
        console.print("[yellow]No source .db files found.[/yellow]")
        raise SystemExit(1)

    failures = []
    for collection, path in sources:
        console.print(f"\n[bold]Consolidating[/bold] {collection} [dim]({path})[/dim]")
        if not path.exists():
            console.print(f"  [red]missing source:[/red] {path}")
            failures.append(collection)
            continue
        try:
            t = consolidate_sqlite_to_postgres(
                path, collection,
                swap=not no_swap, keep_old=keep_old, validate_fks=not no_fk,
            )
            total_rows = sum(t["rows"].values())
            console.print(
                f"  [green]done[/green] {collection}: {total_rows:,} rows, "
                f"copy={t.get('copy_s', 0):.1f}s index={t.get('index_s', 0):.1f}s "
                f"analyze={t.get('analyze_s', 0):.1f}s "
                f"total={t.get('total_s', 0):.1f}s"
                + ("" if no_swap else f" swap={t.get('swap_s', 0):.2f}s")
            )
        except Exception as exc:  # isolate per-collection failures
            console.print(f"  [red]FAILED[/red] {collection}: {exc}")
            failures.append(collection)

    if failures:
        console.print(f"\n[red]Failed collections:[/red] {', '.join(failures)}")
        raise SystemExit(1)
    console.print("\n[green]All collections consolidated.[/green]")
