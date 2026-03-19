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

# Argument parsing
env_file=""
backend_label=""

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
            echo "Unknown argument: $1"
            echo "Usage: $0 [--sqlite | --cirrus-postgres]"
            exit 1
            ;;
    esac
done

if [[ -z "${env_file}" ]]; then
    echo "ERROR: must specify --sqlite or --cirrus-postgres"
    echo "Usage: $0 [--sqlite | --cirrus-postgres]"
    exit 1
fi

source ${TOP_DIR}/etc/config_env.sh
which python3
which jobhist-sync
module load peak-memusage conda
#hostname
#w
#free -g

#pwd

# Source backend-specific env file (overrides any .env settings)
if [[ -f "${env_file}" ]]; then
    set -a
    source "${env_file}" || { echo "Could not source ${env_file}!"; exit 1; }
    set +a
    echo "${sep}"
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

time peak_memusage \
     jobhist-sync -m ${machine} -l ${log_path} --last 30d --verbose --incremental
