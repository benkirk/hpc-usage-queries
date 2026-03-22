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
TIMEFORMAT='(%3R seconds elapsed)'

# Argument parsing
# Default to Cirrus PostgreSQL if no backend specified
env_file="${TOP_DIR}/.env.cirrus"
backend_label="Cirrus PostgreSQL (default)"

jobhist_args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sqlite)
            env_file="${TOP_DIR}/.env.sqlite"
            backend_label="SQLite"
            shift
            ;;
        --cirrus-postgres)
            env_file="${TOP_DIR}/.env.cirrus"
            backend_label="Cirrus PostgreSQL"
            shift
            ;;
        *)
            jobhist_args+=("$1")
            shift
            ;;
    esac
done

# Default jobhist-sync args if none provided
if [[ ${#jobhist_args[@]} -eq 0 ]]; then
    jobhist_args=(--last 30d --verbose --incremental)
fi

source ${TOP_DIR}/etc/config_env.sh
#which python3
#which jobhist-sync

# Source backend-specific env file (overrides any .env settings)
if [[ -f "${env_file}" ]]; then
    set -a
    source "${env_file}" || { echo "Could not source ${env_file}!"; exit 1; }
    set +a
    echo "${sep}"
    echo "# $(date)"
    echo "# Backend: ${backend_label}  (${env_file})"
    echo "${sep}"
else
    echo "ERROR: env file not found: ${env_file}"
    exit 1
fi

unset machine
unset log_path

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

time \
     jobhist-sync -m ${machine} -l ${log_path} "${jobhist_args[@]}"
