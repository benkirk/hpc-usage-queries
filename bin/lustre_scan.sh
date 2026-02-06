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

export ofile
export -f lfs_cmd

cat <<EOF >${ofile}
# lfs scan of ${path}
# $(date)
EOF

#----------------------------
lfs_cmd ${path} --maxdepth 3


find ${path} -maxdepth 3 -mindepth 3 -type d -print0 \
    | \
    xargs -0 -n 1 -P 8 --process-slot-var=XARGS_RANK bash -c 'lfs_cmd ${@}' _


cat ${ofile}.* \
    >> ${ofile}

rm -f ${ofile}.*

exit 0
