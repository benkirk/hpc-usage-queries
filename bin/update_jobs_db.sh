#!/usr/bin/env bash
# update_jobs_db.sh — Add new schema columns to existing HPC job databases
# Usage: bin/update_jobs_db.sh [db_path ...] (default: data/casper.db data/derecho.db)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_DBS=("$SCRIPT_DIR/../data/casper.db" "$SCRIPT_DIR/../data/derecho.db")

DB_PATHS=("${@:-${DEFAULT_DBS[@]}}")

add_column_if_missing() {
    local db="$1" table="$2" column="$3" definition="$4"
    if sqlite3 "$db" "SELECT $column FROM $table LIMIT 1;" 2>/dev/null; then
        echo "  $table.$column already exists — skipping"
    else
        echo "  Adding $table.$column ..."
        sqlite3 "$db" "ALTER TABLE $table ADD COLUMN $column $definition;"
        echo "  Done."
    fi
}

for db in "${DB_PATHS[@]}"; do
    if [[ ! -f "$db" ]]; then
        echo "WARNING: $db not found — skipping"
        continue
    fi
    echo "Updating: $db"
    add_column_if_missing "$db" "job_charges" "qos_factor" "REAL DEFAULT 1.0"
    add_column_if_missing "$db" "jobs" "priority" "TEXT"
    echo ""
done

echo "Migration complete."
