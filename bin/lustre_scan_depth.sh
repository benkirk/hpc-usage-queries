#!/usr/bin/env bash

#set -e

path="${1}"
path="$(realpath ${path})"

scan_dirname="$(basename ${path})"
echo scan_dirname=${scan_dirname}
ofile="$(date +%Y%m%d)_desc1_${scan_dirname}.lfs-scan"
echo "ofile=${ofile}"

rm -f ${ofile}*

# lfs find command to inspect a single directory to be
# intended to be used in a depth-first find pipeline.
lfs_cmd()
{
    #set -x

    local out=${ofile}
    local path=${1}

    if [ '' != "${XARGS_RANK}" ]; then
        out=${out}.${XARGS_RANK}
        echo "[${XARGS_RANK}] ${path}"
    else
        echo "${path}"
    fi

    # first; just the direcctory (hence maxdepth 0, type d...)
    lfs find \
        ${path} \
        --maxdepth 0 \
        --lazy \
        --type d \
        --printf "%LF s=%s b=%b u=%U g=%G p=%LP type=%y perm=%m a=%A@ m=%T@ c=%C@ -- %p\n" \
    >> ${out} \
    2>/dev/null

    # next; its non-recursive non-directory contents
    lfs find \
        ${path} \
        --maxdepth 1 \
        --lazy \
        ! --type d \
        --printf "%LF s=%s b=%b u=%U g=%G p=%LP type=%y perm=%m a=%A@ m=%T@ c=%C@ -- %p\n" \
    >> ${out} \
    2>/dev/null
}

export ofile
export -f lfs_cmd

cat <<EOF >${ofile}
# lfs scan of ${path}
# $(date)
EOF

#----------------------------------------------
find ${path} -depth -type d -readable -print0 | \
    xargs -0 -n 1 -P 8 --process-slot-var=XARGS_RANK \
          bash -c 'lfs_cmd ${@}' _

#----------------------------------------------
cat ${ofile}.* >> ${ofile} && rm -f ${ofile}.*

exit 0
