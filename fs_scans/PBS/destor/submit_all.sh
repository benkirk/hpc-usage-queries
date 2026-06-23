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
    echo "submitting metadata scan + import: ${scan_dir}"

    # 1) metadata scan (lustre_scan_depth.sh -> .lfs-scan on shared scratch)
    scan_id=$(qsub \
                  -N fs_scan_${label} \
                  -v TARGET_DIR="${scan_dir}" \
                  ${SCRIPT_DIR}/create_metadata_scan.pbs)
    echo "  scan:   ${scan_id}"

    # 2) import the just-generated scan, gated on the scan succeeding. Submitted
    #    immediately so each target's import queues as soon as its scan lands,
    #    rather than waiting on the whole batch.
    import_id=$(qsub \
                    -N fs_import_${label} \
                    -v TARGET_DIR="${scan_dir}" \
                    -W depend=afterok:${scan_id} \
                    ${SCRIPT_DIR}/import_metadata_scan.pbs)
    echo "  import: ${import_id} (afterok:${scan_id})"

    # Track the import (terminal) jobs so a future collect/consolidate stage can
    # depend on the jobs that actually produce the .db files.
    all_ids=${import_id}:${all_ids}
done

echo ${sep}
echo "all_ids=${all_ids}"
set -x
collect_jobid=$(qsub -W depend=afterany:${all_ids} \
                     -N collect_results \
                     ${SCRIPT_DIR}/rsync_results.pbs)
set +x
echo collect_jobid=${collect_jobid}


# Optional: consolidate the published .db files into CNPG PostgreSQL after the
# rsync collection succeeds. Disabled by default — enable with
# FS_SCAN_ENABLE_CONSOLIDATE=1 once the per-collection performance pass and the
# CNPG disk/role review have signed off (see the plan / fs_scans README).
if [[ "${FS_SCAN_ENABLE_CONSOLIDATE:-}" == "1" ]]; then
    set -x
    qsub -W depend=afterok:${collect_jobid} \
         -N fs_scans_consolidate \
         ${SCRIPT_DIR}/consolidate.pbs
    set +x
fi
