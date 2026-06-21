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

        # Scoped-query (ancestor-at-depth) indexes. PostgreSQL only: covering
        # indexes give index-only aggregates for the scoped owner/group/listing
        # paths over the selective depth band. SQLite leaves the anc_d* columns
        # unindexed — the local CLI uses the recursive-CTE fallback and these
        # heavy indexes would only bloat the published .db. Tune the band via
        # SCOPE_INDEX_MIN_DEPTH / SCOPE_INDEX_MAX_DEPTH in core/models.py.
        if session.get_bind().dialect.name == "postgresql":
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

        session.commit()
        progress.update(task, description=f"{desc} [dim]done in {progress.tasks[task].elapsed:.1f}s[/dim]")

    return
