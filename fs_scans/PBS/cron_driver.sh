#!/bin/bash
#----------------------------------------------------------------------------
# Determine the directory containing this script, compatible with bash and zsh
if [ -n "${BASH_SOURCE[0]}" ]; then
  SCRIPT_PATH="${BASH_SOURCE[0]}"
elif [ -n "${ZSH_VERSION}" ]; then
  SCRIPT_PATH="${(%):-%x}"
else
  echo "Unknown shell!"
fi
SCRIPT_DIR=$(realpath $(dirname ${SCRIPT_PATH}))
TOP_DIR=$(git rev-parse --show-toplevel)
#----------------------------------------------------------------------------

set -e

cd ${SCRIPT_DIR}
git clean -xdf .

# Enable the SQLite→CNPG PostgreSQL consolidation step for the weekly automated
# run (after collect_results). Manual submit_all.sh invocations stay opt-in.
# Validated 2026-06-17: all 13 collections, ~50 GB in PG (~1.14×), sub-second
# atomic swaps, full SQLite↔PostgreSQL parity. See fs_scans/README.md.
export FS_SCAN_ENABLE_CONSOLIDATE=1

./submit_all.sh > submit_all.log 2>&1
