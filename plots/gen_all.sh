#!/bin/bash

# Calculate dates for past full month and past full year
# Past month: first to last day of previous month
# Past year: 12 months ending with the past full month

# macOS date commands (use -v for relative dates)
if date -v-1d > /dev/null 2>&1; then
    # macOS
    month_start=$(date -v-1m -v1d +%Y-%m-%d)
    month_end=$(date -v-1m -v1d -v+1m -v-1d +%Y-%m-%d)
    year_start=$(date -v-12m -v1d +%Y-%m-%d)
    year_end=$(date -v-1m -v1d -v+1m -v-1d +%Y-%m-%d)
else
    # GNU/Linux
    month_start=$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m-%d)
    month_end=$(date -d "$(date +%Y-%m-01) -1 day" +%Y-%m-%d)
    year_start=$(date -d "$(date +%Y-%m-01) -12 months" +%Y-%m-%d)
    year_end=$(date -d "$(date +%Y-%m-01) -1 day" +%Y-%m-%d)
fi

echo "Generating reports for:"
echo "  Past month: ${month_start} to ${month_end}"
echo "  Past year:  ${year_start} to ${year_end}"
echo

subcommands=(
  job-sizes
  job-waits
  cpu-job-sizes
  cpu-job-waits
  cpu-job-durations
  cpu-job-memory-per-rank
  gpu-job-sizes
  gpu-job-waits
  gpu-job-durations
  gpu-job-memory-per-rank
  pie-user-cpu
  pie-user-gpu
  pie-proj-cpu
  pie-proj-gpu
  pie-group-cpu
  pie-group-gpu
  usage-history
)

for cmd in "${subcommands[@]}"; do
    echo $cmd
    jobhist resource --machine derecho --start-date ${month_start} --end-date ${month_end} --group-by day ${cmd}
    jobhist resource --machine derecho --start-date ${year_start} --end-date ${year_end} --group-by month ${cmd}
done
