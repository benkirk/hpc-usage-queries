from rich.progress import Progress
from .common_imports import *


def add_directories_indexing(session):
    with Progress() as progress:
        task = progress.add_task(
            f"  [green]Indexing directories table...",
            total=None
        )

        session.execute(text("CREATE INDEX IF NOT EXISTS ix_directories_parent ON directories(parent_id);"))
        session.execute(text("CREATE INDEX IF NOT EXISTS ix_directories_depth  ON directories(depth);"))
        session.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_dir_parent_name ON directories (parent_id, name);"))
        session.commit()
        progress.console.print(f"   [dim]...done in {progress.tasks[task].elapsed:.1f}s[/dim]")

    return



def add_directory_stats_indexing(session):
    with Progress() as progress:
        task = progress.add_task(
            f"  [green]Indexing directory_stats table...",
            total=None
        )

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
        session.commit()
        progress.console.print(f"   [dim]...done in {progress.tasks[task].elapsed:.1f}s[/dim]")

    return
