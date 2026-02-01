from .common_imports import *
from ..parsers.base import FilesystemParser
from .file_handling import *


def pass2b_aggregate_recursive_stats(session) -> None:
    """
    Phase 2b: compute recursive stats via bottom-up SQL aggregation.

    Processes directories by depth, from deepest to shallowest.
    Each directory's recursive stats = its non-recursive stats + sum of children's recursive stats.

    Optimized to use SQLite 'UPDATE FROM' (requires SQLite 3.33+).
    """
    console.print("  [bold]Phase 2b:[/bold] Computing recursive statistics...")

    # Get max depth
    max_depth = session.execute(text("SELECT MAX(depth) FROM directories")).scalar() or 0

    console.print(f"    Max directory depth: {max_depth}")

    with create_progress_bar(show_rate=False) as progress:
        task = progress.add_task(
            "[green]Aggregating by depth...",
            total=max_depth,
        )

        # Process from leaves (max_depth) down to root (depth=1)
        for depth in range(max_depth, 0, -1):
            # 1. Initialize recursive stats with non-recursive stats for this level
            #    (This covers leaf nodes and prepares parents for accumulation)
            session.execute(
                text("""
                UPDATE directory_stats
                SET
                    file_count_r = file_count_nr,
                    total_size_r = total_size_nr,
                    max_atime_r = max_atime_nr,
                    dir_count_r = dir_count_nr
                WHERE dir_id IN (SELECT dir_id FROM directories WHERE depth = :depth)
                """),
                {"depth": depth},
            )

            # 2. Accumulate stats from children (depth + 1) using UPDATE FROM
            #    (Only updates parents that actually have children)
            session.execute(
                text("""
                WITH child_agg AS (
                    SELECT
                        d.parent_id,
                        SUM(s.file_count_r) as sum_files,
                        SUM(s.total_size_r) as sum_size,
                        SUM(s.dir_count_r) as sum_dirs,
                        MAX(s.max_atime_r) as max_atime,
                        -- Owner UID Aggregation:
                        -- Check if any child has a NULL owner (conflict)
                        MAX(CASE WHEN s.owner_uid IS NULL THEN 1 ELSE 0 END) as has_uid_conflict,
                        -- Count distinct valid owners (ignoring -1/no-files)
                        COUNT(DISTINCT CASE WHEN s.owner_uid >= 0 THEN s.owner_uid END) as distinct_valid_owners,
                        -- Get the potential common owner (if count is 1)
                        MAX(CASE WHEN s.owner_uid >= 0 THEN s.owner_uid END) as common_owner,
                        -- Owner GID Aggregation:
                        -- Check if any child has a NULL group (conflict)
                        MAX(CASE WHEN s.owner_gid IS NULL THEN 1 ELSE 0 END) as has_gid_conflict,
                        -- Count distinct valid groups (ignoring -1/no-files)
                        COUNT(DISTINCT CASE WHEN s.owner_gid >= 0 THEN s.owner_gid END) as distinct_valid_groups,
                        -- Get the potential common group (if count is 1)
                        MAX(CASE WHEN s.owner_gid >= 0 THEN s.owner_gid END) as common_group
                    FROM directories d
                    JOIN directory_stats s ON d.dir_id = s.dir_id
                    WHERE d.depth = :child_depth
                    GROUP BY d.parent_id
                )
                UPDATE directory_stats
                SET
                    file_count_r = file_count_r + agg.sum_files,
                    total_size_r = total_size_r + agg.sum_size,
                    dir_count_r = dir_count_r + agg.sum_dirs,
                    max_atime_r = MAX(COALESCE(max_atime_r, 0), COALESCE(agg.max_atime, 0)),
                    owner_uid = CASE
                        -- Already conflicted -> stay conflicted
                        WHEN owner_uid IS NULL THEN NULL

                        -- Direct files exist (owner_uid >= 0) -> check for conflict with children
                        WHEN owner_uid >= 0 THEN
                             CASE
                                WHEN agg.has_uid_conflict = 1 THEN NULL
                                WHEN agg.distinct_valid_owners > 0 AND agg.common_owner != owner_uid THEN NULL
                                ELSE owner_uid
                             END

                        -- No direct files (-1) -> inherit from children
                        ELSE -- owner_uid == -1
                             CASE
                                WHEN agg.has_uid_conflict = 1 THEN NULL
                                WHEN agg.distinct_valid_owners > 1 THEN NULL
                                WHEN agg.distinct_valid_owners = 1 THEN agg.common_owner
                                ELSE -1 -- Still no owner seen
                             END
                    END,
                    owner_gid = CASE
                        -- Already conflicted -> stay conflicted
                        WHEN owner_gid IS NULL THEN NULL

                        -- Direct files exist (owner_gid >= 0) -> check for conflict with children
                        WHEN owner_gid >= 0 THEN
                             CASE
                                WHEN agg.has_gid_conflict = 1 THEN NULL
                                WHEN agg.distinct_valid_groups > 0 AND agg.common_group != owner_gid THEN NULL
                                ELSE owner_gid
                             END

                        -- No direct files (-1) -> inherit from children
                        ELSE -- owner_gid == -1
                             CASE
                                WHEN agg.has_gid_conflict = 1 THEN NULL
                                WHEN agg.distinct_valid_groups > 1 THEN NULL
                                WHEN agg.distinct_valid_groups = 1 THEN agg.common_group
                                ELSE -1 -- Still no group seen
                             END
                    END
                FROM child_agg AS agg
                WHERE directory_stats.dir_id = agg.parent_id
                """),
                {"child_depth": depth + 1},
            )

            session.commit()
            progress.update(task, advance=1)

    console.print(f"    Processed {max_depth} depth levels")
