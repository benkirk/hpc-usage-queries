from .common_imports import *
from ..core.models import SCOPE_MAX_DEPTH


def pass2c_populate_ancestor_columns(session) -> None:
    """
    Phase 2c: populate the denormalized ancestor-at-depth columns
    (anc_d1 .. anc_d{SCOPE_MAX_DEPTH}) on directory_stats.

    Top-down (depth ascending) — the mirror of pass2b's bottom-up aggregation.
    For each depth D, a row inherits its parent's already-populated ancestor
    slots (anc_d1 .. anc_d{D-1}) and then overrides anc_d{D} with its own dir_id.
    Because the parent at depth D-1 is fully populated before we reach D, the
    inherited slots are correct, anc_d{D} is self, and anc_d{D+1..N} stay NULL.

    A scoped query for ancestor X resolved at depth k then becomes
    `WHERE anc_d{k} = X` — a single indexed equality replacing the recursive
    walk. See docs/plans/FS_SCANS_ANCESTOR_AT_DEPTH.md.

    Uses SQLite 'UPDATE ... FROM' (requires SQLite 3.33+), same as pass2b.
    """
    console.print("  [bold]Phase 2c:[/bold] Populating ancestor-at-depth columns...")

    max_depth = session.execute(text("SELECT MAX(depth) FROM directories")).scalar() or 0
    effective = min(max_depth, SCOPE_MAX_DEPTH)
    console.print(f"    Max directory depth: {max_depth} (populating anc_d1..anc_d{effective})")

    with create_progress_bar(show_rate=False) as progress:
        task = progress.add_task(
            "[green]Populating ancestors by depth...",
            total=max_depth,
        )

        # Process from root (depth=1) down to leaves so each row's parent is
        # already populated when we reach it.
        for depth in range(1, max_depth + 1):
            # 1. Inherit the parent's populated ancestor slots. The parent lives
            #    at depth-1 and has slots anc_d1 .. anc_d{min(depth-1, N)} set;
            #    deeper slots are NULL there, so copying only the populated ones
            #    is sufficient (the rest keep their NULL default).
            ncols = min(depth - 1, SCOPE_MAX_DEPTH)
            if ncols >= 1:
                set_inherit = ", ".join(f"anc_d{k} = p.anc_d{k}" for k in range(1, ncols + 1))
                session.execute(
                    text(f"""
                    UPDATE directory_stats
                    SET {set_inherit}
                    FROM directories d, directory_stats p
                    WHERE d.dir_id = directory_stats.dir_id
                      AND d.depth = :depth
                      AND p.dir_id = d.parent_id
                    """),
                    {"depth": depth},
                )

            # 2. A directory is its own depth-D ancestor (only within range).
            if depth <= SCOPE_MAX_DEPTH:
                session.execute(
                    text(f"""
                    UPDATE directory_stats
                    SET anc_d{depth} = dir_id
                    WHERE dir_id IN (SELECT dir_id FROM directories WHERE depth = :depth)
                    """),
                    {"depth": depth},
                )

            session.commit()
            progress.update(task, advance=1)

    console.print(f"    Populated ancestor columns up to depth {effective}")
