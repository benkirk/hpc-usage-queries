from rich.progress import Progress
from .common_imports import *
from ..core.models import SCOPE_INDEX_MIN_DEPTH, SCOPE_INDEX_MAX_DEPTH


def add_directories_indexing(session):
    with Progress() as progress:
        desc = "  [green]Indexing directories table..."
        task = progress.add_task(desc, total=None)

        session.execute(text("CREATE INDEX IF NOT EXISTS ix_directories_parent ON directories(parent_id);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_directories_depth  ON directories(depth);"))
        session.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_dir_parent_name ON directories (parent_id, name);"))
        session.commit()
        progress.update(task, description=f"{desc} [dim]done in {progress.tasks[task].elapsed:.1f}s[/dim]")

    return



def add_directory_stats_nr_indexing(session):
    with Progress() as progress:
        desc = "  [green]Indexing directory_stats table..."
        task = progress.add_task(desc, total=None)

        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_file_count_nr  ON directory_stats(file_count_nr);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_total_size_nr  ON directory_stats(total_size_nr);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_dir_count_nr   ON directory_stats(dir_count_nr);"))
        session.commit()
        progress.update(task, description=f"{desc} [dim]done in {progress.tasks[task].elapsed:.1f}s[/dim]")

    return



def add_directory_stats_indexing(session):
    with Progress() as progress:
        desc = "  [green]Indexing directory_stats table..."
        task = progress.add_task(desc, total=None)

        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_file_count_nr  ON directory_stats(file_count_nr);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_total_size_nr  ON directory_stats(total_size_nr);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_dir_count_nr   ON directory_stats(dir_count_nr);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_file_count_r   ON directory_stats(file_count_r);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_total_size_r   ON directory_stats(total_size_r);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_dir_count_r    ON directory_stats(dir_count_r);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_owner_uid      ON directory_stats(owner_uid);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_owner_gid      ON directory_stats(owner_gid);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_owner_size     ON directory_stats(owner_uid, total_size_r);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_owner_files    ON directory_stats(owner_uid, file_count_r);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_group_size     ON directory_stats(owner_gid, total_size_r);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_stats_group_files    ON directory_stats(owner_gid, file_count_r);"))

        # Scoped-query (ancestor-at-level) indexes over the selective band of
        # root-relative levels (SCOPE_INDEX_MIN_DEPTH..SCOPE_INDEX_MAX_DEPTH in
        # core/models.py). Both backends index the band so a scoped query is an
        # index seek instead of a full scan; the index *shape* differs by engine.
        dialect = session.get_bind().dialect.name
        if dialect == "postgresql":
            # Covering indexes → index-only aggregates for owner/group/listing.
            for k in range(SCOPE_INDEX_MIN_DEPTH, SCOPE_INDEX_MAX_DEPTH + 1):
                session.execute(text(
                    f"CREATE INDEX IF NOT EXISTS ix_stats_anc_d{k}_owner ON directory_stats "
                    f"(anc_d{k}, owner_uid) INCLUDE (total_size_nr, file_count_nr);"
                ))
                session.execute(text(
                    f"CREATE INDEX IF NOT EXISTS ix_stats_anc_d{k}_group ON directory_stats "
                    f"(anc_d{k}, owner_gid) INCLUDE (total_size_nr, file_count_nr);"
                ))
                session.execute(text(
                    f"CREATE INDEX IF NOT EXISTS ix_stats_anc_d{k}_size  ON directory_stats "
                    f"(anc_d{k}, total_size_r);"
                ))
        else:
            # SQLite (and others): plain single-column anc_d{k} indexes. SQLite
            # has no INCLUDE/covering, and the full covering set would ~double the
            # published .db; a plain index still turns the scoped equality into an
            # index seek (measured ~4-5x on cgd-scale data), keeping the .db
            # standalone-performant for the local CLI and other non-CNPG consumers
            # at a modest (~+20% on cgd) size cost.
            for k in range(SCOPE_INDEX_MIN_DEPTH, SCOPE_INDEX_MAX_DEPTH + 1):
                session.execute(text(
                    f"CREATE INDEX IF NOT EXISTS ix_stats_anc_d{k} ON directory_stats(anc_d{k});"
                ))

        session.commit()
        progress.update(task, description=f"{desc} [dim]done in {progress.tasks[task].elapsed:.1f}s[/dim]")

    return
