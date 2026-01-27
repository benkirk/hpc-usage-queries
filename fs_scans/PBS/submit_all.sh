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

scans_logdir="/glade/u/hdig/reports/logs"
readarray -t files < <(ls -1 ${scans_logdir}/*_csfs1_*.list_all.log)

# determine most recent scan date
latest_date=$(
    printf '%s\n' "${files[@]}" \
        | sed -E 's#.*/([0-9]{8})_.*#\1#' \
        | sort -u | tail -n1
)

readarray -t files < <(ls -1 ${scans_logdir}/${latest_date}_csfs1_*.list_all.log)
# determine available file systems
labels=$(
   printf '%s\n' "${files[@]}" \
       | sed -E 's#.*/[0-9]{8}_csfs1_([^.]*)\..*#\1#' \
       | sort -u
)

for label in ${labels}; do

    file=$(realpath ${scans_logdir}/${latest_date}*${label}*.list_all.log)

    ls -lh ${file}

    qsub \
        -N fs_scan_${label} \
        -v SCAN_LOG_FILE="${file}" \
        ${SCRIPT_DIR}/fs_scan.pbs
done
