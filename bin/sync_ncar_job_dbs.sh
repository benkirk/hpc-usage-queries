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

sep="#----------------------------------------------------------------------------"

set -x

source ${TOP_DIR}/etc/config_env.sh
which python3
which jobhist-sync
module load peak-memusage conda
hostname
w
free -g

pwd


unset machine
unset log_path

export JH_DB_BACKEND=sqlite
export JOB_HISTORY_DATA_DIR=/glade/u/apps/opt/hpc-usage-queries/data

case "${NCAR_HOST}" in
    "casper")
        machine="${NCAR_HOST}"
        log_path="/ssg/pbs/casper/accounting"
        ;;
    "derecho")
        machine="${NCAR_HOST}"
        log_path="/ncar/pbs/accounting"
        ;;
    *)
        echo "ERROR: unhandled NCAR_HOST=${NCAR_HOST}"
        exit 1
        ;;
esac

cd ${log_path} && cd -

time peak_memusage \
     jobhist-sync -m ${machine} -l ${log_path} --start 2026-01-01 --verbose
