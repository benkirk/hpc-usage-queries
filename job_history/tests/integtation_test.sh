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

JH_DB_BACKEND="${JH_DB_BACKEND:-sqlite}"
echo "Integration test backend: ${JH_DB_BACKEND}"

# SQLite: point at a temporary directory so we don't touch production data.
if [ "${JH_DB_BACKEND}" = "sqlite" ]; then
    export JOB_HISTORY_DATA_DIR=$(mktemp -d)
fi

python3 -c "from job_history import init_db; init_db()"

# Show what was created
if [ "${JH_DB_BACKEND}" = "sqlite" ]; then
    ls -lh ${JOB_HISTORY_DATA_DIR}/*.db
else
    python3 -c "
from job_history import get_db_url, VALID_MACHINES
for m in sorted(VALID_MACHINES):
    print(f'  {m}: {get_db_url(m)}')
"
fi

for machine in casper derecho; do
    jobhist-sync -m ${machine} -l ${SCRIPTDIR}/fixtures/pbs_logs/${machine}/ --start 2026-01-01 --end 2026-01-05 --verbose

    jobhist-history -m ${machine} --start-date 2026-01-01 daily-summary
done

# Show final state
if [ "${JH_DB_BACKEND}" = "sqlite" ]; then
    ls -lh ${JOB_HISTORY_DATA_DIR}/*.db
    rm -rf ${JOB_HISTORY_DATA_DIR}
else
    python3 -c "
from job_history import VALID_MACHINES
from job_history.database import get_engine
from sqlalchemy import text
for m in sorted(VALID_MACHINES):
    engine = get_engine(m)
    with engine.connect() as conn:
        jobs  = conn.execute(text('SELECT COUNT(*) FROM jobs')).scalar()
        summ  = conn.execute(text('SELECT COUNT(*) FROM daily_summary')).scalar()
    print(f'  {m}: {jobs} jobs, {summ} daily_summary rows')
    engine.dispose()
"
fi
