for db in fs_scans/data/*.db; do
    echo "Adding indexes to ${db}..."
    ls -lh ${db}
    sqlite3 "${db}" <<EOF
CREATE INDEX IF NOT EXISTS ix_stats_owner_size ON directory_stats(owner_uid, total_size_r);
CREATE INDEX IF NOT EXISTS ix_stats_owner_files ON directory_stats(owner_uid, file_count_r);
-- Stop writes
ANALYZE;
PRAGMA page_size = 32768;   -- optional, must come before VACUUM
VACUUM;
EOF
    ls -lh ${db}
done
