all="id,short_id,account,avgcpu,count,cpupercent,cputime,cputype,elapsed,eligible,end,gputype,memory,mpiprocs,name,numcpus,numgpus,numnodes,ompthreads,ptargets,queue,reqmem,resources,start,status,submit,user,vmemory,walltime"

period="--period 20251121"

for machine in derecho casper; do

    ssh ${machine}  \
        qhist ${period} \
        --json | head -n 100

    ssh ${machine} \
        qhist ${period} \
        --csv \
        --format="${all}" | head -n 50

    ssh ${machine} \
        qhist ${period} \
        --list \
        --format="${all}" | head -n 100
done

ssh derecho \
    qhist \
    --help

ssh casper \
    qhist \
    --format=help
