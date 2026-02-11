#!/usr/bin/env python3

import pbsparse
from pbsparse import get_pbs_records

job_queues = get_pbs_records("./data/sample_pbs_logs/derecho/20260129", type_filter = "Q")
job_starts = get_pbs_records("./data/sample_pbs_logs/derecho/20260129", type_filter = "S")
job_ends = get_pbs_records("./data/sample_pbs_logs/derecho/20260129", type_filter = "E")
job_requeus = get_pbs_records("./data/sample_pbs_logs/derecho/20260129", type_filter = "R")
jobs = get_pbs_records("./data/sample_pbs_logs/derecho/20260129")

cnt=0
for job in job_ends:
    print(job)
    print(job.short_id)
    for k,v in job.__dict__.items():
        print(f"\t{k} = {v}")
    #print(job.__dict__)
    cnt=cnt+1
    if cnt > 10: break
