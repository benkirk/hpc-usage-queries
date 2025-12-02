#!/bin/bash

subcommands=(
  job-sizes
  job-waits
  cpu-job-sizes
  cpu-job-waits
  cpu-job-durations
  gpu-job-sizes
  gpu-job-waits
  gpu-job-durations
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
    qhist-report resource --machine derecho --start-date 2025-11-01 --end-date 2025-11-30 --group-by day ${cmd}
    qhist-report resource --machine derecho --start-date 2024-12-01 --end-date 2025-11-30 --group-by month ${cmd}
done
