from .common_imports import *
from ..core.models import SCOPE_MAX_DEPTH


def pass2c_populate_ancestor_columns(session) -> None:
    """
    Phase 2c: populate the denormalized ancestor-at-(relative-)level columns
    (anc_d1 .. anc_d{SCOPE_MAX_DEPTH}) on directory_stats.

    Levels are measured *relative to the collection root* (the shallowest
    directory, MIN(depth)): root = level 1, one below = level 2, etc. This keeps
    the useful band uniform no matter how deep the collection roots on disk.

    Top-down (depth ascending) — the mirror of pass2b's bottom-up aggregation.
    For a row at relative level L, it inherits its parent's already-populated
    slots (anc_d1 .. anc_d{L-1}) and then overrides anc_d{L} with its own dir_id.
    Because the parent (level L-1) is fully populated before we reach L, the
    inherited slots are correct, anc_d{L} is self, and anc_d{L+1..N} stay NULL.

    A scoped query for ancestor X at relative level k then becomes
    `WHERE anc_d{k} = X` — a single indexed equality replacing the recursive
    walk. See docs/plans/FS_SCANS_ANCESTOR_AT_DEPTH.md.

    Uses SQLite 'UPDATE ... FROM' (requires SQLite 3.33+), same as pass2b.
    """
    console.print("  [bold]Phase 2c:[/bold] Populating ancestor-at-level columns...")

    root_depth = session.execute(text("SELECT MIN(depth) FROM directories")).scalar()
    max_depth = session.execute(text("SELECT MAX(depth) FROM directories")).scalar()
    if root_depth is None or max_depth is None:
        console.print("    No directories — nothing to populate")
        return

    max_level = max_depth - root_depth + 1
    effective = min(max_level, SCOPE_MAX_DEPTH)
    console.print(
        f"    Root depth {root_depth}, max depth {max_depth} "
        f"(populating anc_d1..anc_d{effective}, relative to root)"
    )

    with create_progress_bar(show_rate=False) as progress:
        task = progress.add_task(
            "[green]Populating ancestors by level...",
            total=max_depth - root_depth + 1,
        )

        # Process from the root downward so each row's parent is already
        # populated when we reach it.
        for depth in range(root_depth, max_depth + 1):
            level = depth - root_depth + 1

            # 1. Inherit the parent's populated ancestor slots. The parent is one
            #    level shallower and has slots anc_d1 .. anc_d{min(level-1, N)};
            #    deeper slots are NULL there, so copying only the populated ones
            #    is sufficient (the rest keep their NULL default).
            ncols = min(level - 1, SCOPE_MAX_DEPTH)
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

            # 2. A directory is its own level-L ancestor (only within range).
            if level <= SCOPE_MAX_DEPTH:
                session.execute(
                    text(f"""
                    UPDATE directory_stats
                    SET anc_d{level} = dir_id
                    WHERE dir_id IN (SELECT dir_id FROM directories WHERE depth = :depth)
                    """),
                    {"depth": depth},
                )

            session.commit()
            progress.update(task, advance=1)

    console.print(f"    Populated ancestor columns up to level {effective}")
