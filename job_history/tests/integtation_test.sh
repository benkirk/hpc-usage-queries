#----------------------------------------------------------------------------
# Determine the directory containing this script, compatible with bash and zsh
if [ -n "${BASH_SOURCE[0]}" ]; then
  SCRIPT_PATH="${BASH_SOURCE[0]}"
elif [ -n "${ZSH_VERSION}" ]; then
  SCRIPT_PATH="${(%):-%x}"
else
  echo "Unknown shell!"
fi
SCRIPTDIR="$(cd "$(dirname "$(realpath "${SCRIPT_PATH}")")" >/dev/null 2>&1 && pwd)"
#----------------------------------------------------------------------------

set -e

export JOB_HISTORY_DATA_DIR=$(mktemp -d)

python3 -c "from job_history import init_db; init_db()"

ls -lh ${JOB_HISTORY_DATA_DIR}/*.db

for machine in casper derecho; do
    jobhist-sync -m ${machine} -l ${SCRIPTDIR}/fixtures/pbs_logs/${machine}/ --start 2026-01-01 --end 2026-01-05 --verbose

    jobhist-history -m ${machine} --start-date 2026-01-01 daily-summary
done

ls -lh ${JOB_HISTORY_DATA_DIR}/*.db

rm -rf ${JOB_HISTORY_DATA_DIR}
