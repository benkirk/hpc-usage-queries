#!/usr/bin/env bash


set -e

path="${1}"
path="$(realpath ${path})"

ofile_stub="$(date +%Y%m%d)_desc1"
echo "ofile_stub=${ofile_stub}"
scan_dirname="$(basename ${path})"
echo scan_dirname=${scan_dirname}
ofile="${ofile_stub}_${scan_dirname}.lfs-scan"

echo "ofile=${ofile}"

rm -f ${ofile}*

# Recursively find work units by subdividing large directories
# Args:
#   $1: dir                    - Directory to analyze
#   $2: current_depth          - Current recursion depth (0 = starting point)
#   $3: max_subdivision_depth  - Maximum recursion depth (stop subdividing after this)
#   $4: subdir_threshold       - Subdivide if dir has more than this many subdirs
#   $5: lookahead_depth        - How many levels deep to count subdirs for size estimation
find_work_units()
{
    local dir=$1
    local current_depth=${2:-0}
    local max_subdivision_depth=${3:-3}
    local subdir_threshold=${4:-10}
    local lookahead_depth=${5:-3}

    # Safety: don't recurse forever
    if [ ${current_depth} -ge ${max_subdivision_depth} ]; then
        echo "${dir}"
        return
    fi

    # Count subdirectories within lookahead depth to catch fan-out cases
    # A dir with 2 immediate subdirs but 1000 total descendants will be caught
    local subdir_count=$(find "${dir}" -maxdepth ${lookahead_depth} -mindepth 1 -type d 2>/dev/null | wc -l)

    if [ ${subdir_count} -gt ${subdir_threshold} ]; then
        # Large directory tree - subdivide it
        # Scan this dir non-recursively first to catch files directly in it
        echo "${dir} --maxdepth 1"
        find "${dir}" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | while IFS= read -r subdir; do
            find_work_units "${subdir}" $((current_depth + 1)) ${max_subdivision_depth} ${subdir_threshold} ${lookahead_depth}
        done
    else
        # Small enough subtree - use as work unit (recursive)
        echo "${dir}"
    fi
}

lfs_cmd()
{
    local path=$1
    #echo path=${path}
    shift
    local args=$@

    local out=${ofile}

    if [ '' != "${XARGS_RANK}" ]; then
        out=${out}.${XARGS_RANK}
        echo "[${XARGS_RANK}] ${path}"
    else
        echo "${path}"
    fi

    lfs find \
        ${path} \
        --lazy ${args} \
        --printf "%LF s=%s b=%b u=%U g=%G p=%LP type=%y perm=%m a=%A@ m=%T@ c=%C@ -- %p\n" \
        >> ${out} 2>/dev/null
}

# Process a work unit specification (may include --maxdepth args)
process_work_unit()
{
    local spec="$1"
    # Check if spec contains "--maxdepth N" suffix
    if [[ "$spec" =~ ^(.*)\ --maxdepth\ ([0-9]+)$ ]]; then
        lfs_cmd "${BASH_REMATCH[1]}" --maxdepth "${BASH_REMATCH[2]}"
    else
        lfs_cmd "$spec"
    fi
}

export ofile
export -f find_work_units
export -f lfs_cmd
export -f process_work_unit

cat <<EOF >${ofile}
# lfs scan of ${path}
# $(date)
EOF

#----------------------------
# Generate work units using adaptive depth subdivision starting from root
# Parameters: dir, current_depth=0, max_subdivision_depth=3, subdir_threshold=10, lookahead_depth=3
work_units=$(mktemp)
set +e  # Disable exit-on-error for permission errors during work unit generation
find_work_units "${path}" 0 3 10 3 > ${work_units}
set -e

# Process work units in parallel
cat ${work_units} | \
    xargs -d '\n' -n 1 -P 8 --process-slot-var=XARGS_RANK bash -c '
        set +e  # Disable exit-on-error for permission errors
        process_work_unit "$@"
    ' _


cat ${ofile}.* >> ${ofile}

rm -f ${ofile}.* ${work_units}

exit 0
