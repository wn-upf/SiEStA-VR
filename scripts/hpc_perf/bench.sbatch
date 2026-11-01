#!/bin/bash
#SBATCH -J pp.bench
#SBATCH --partition=high
#SBATCH --constraint=x86_64&bdw
#SBATCH --nodes=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=06:00:00
# Interleaved timing (scripts/perf_bench.sh) of the binaries in $B/bin named by $BINS, on the paper's command line:
# SLO 4 users, MLO 8 users and random walk (MLO) 8 users, CBR 100 Mb/s, $STOP simulated seconds, $ROUNDS rounds. One run at
# a time on the job's 2 CPUs (the simulation thread and the Parquet writers), outputs on node-local /tmp.
set -uo pipefail
B=${B:?B=<hpc dir>}; BINS=${BINS:-"base.release base.optimized stageD.release final.release final.optimized"}
args=""; for b in $BINS; do args="$args $b=$B/bin/$b"; done
echo "node $(hostname -s), $(grep -m1 'model name' /proc/cpuinfo)"
BENCH_STOP=${STOP:-180} BENCH_ROUNDS=${ROUNDS:-3} BENCH_WORK=/tmp/pp_bench.$SLURM_JOB_ID \
    $B/snaps/final/scripts/perf_bench.sh $B/bench_${SLURM_JOB_ID}.csv $args
rm -rf /tmp/pp_bench.$SLURM_JOB_ID
echo BENCH-DONE
