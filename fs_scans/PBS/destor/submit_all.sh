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

sep="#----------------------------------------------------------------------------"

# Target list defaults to the five desc1 filesystem roots. Override by passing
# one or more directories as arguments, e.g. for a smaller/deeper test scan:
#   ./submit_all.sh /lustre/desc1/p/nral0032
scan_dirs=("$@")
if [ ${#scan_dirs[@]} -eq 0 ]; then
    scan_dirs=(/lustre/desc1/{espat,gdex,glade_p_archive,mirrors,p})
fi

unset all_ids

for scan_dir in "${scan_dirs[@]}"; do
    scan_dir=$(realpath ${scan_dir})
    [ -d "${scan_dir}" ] || { echo "skipping (not a directory): ${scan_dir}"; continue; }

    label=$(basename ${scan_dir})
    echo ${sep}
    echo "submitting metadata scan: ${scan_dir}"

    jobid=$(qsub \
                -N fs_scan_${label} \
                -v TARGET_DIR="${scan_dir}" \
                ${SCRIPT_DIR}/create_metadata_scan.pbs)
    echo "  ${jobid}"
    all_ids=${jobid}:${all_ids}
done

echo ${sep}
echo "all_ids=${all_ids}"

# Future (Phase 2): chain an import job after the metadata scans complete,
# mirroring the dependency pattern in fs_scans/PBS/submit_all.sh, e.g.
# collect_jobid=$(qsub -W depend=afterany:${all_ids} \
#                      -N import_metadata_scans \
#                      ${SCRIPT_DIR}/import_metadata_scan.pbs)
