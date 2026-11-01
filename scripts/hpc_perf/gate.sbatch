#!/bin/bash
#SBATCH -J pp.gate
#SBATCH --partition=high
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=02:00:00
# Byte-exact gate on the HPC: records the manifest of $REF (a binary in $B/bin), then checks every binary of $BINS
# against it, and runs the sanity mode (each scenario twice, the second on one CPU) for $SANITY.
set -uo pipefail
B=${B:?B=<hpc dir>}; REF=${REF:-stageD.release}; BINS=${BINS:-"stageD.optimized final.release final.optimized"}
SANITY=${SANITY:-"final.release"}
G=$B/snaps/final/scripts/byteexact_gate.sh; W=/tmp/pp_gate.$SLURM_JOB_ID; export GATE_JOBS=${SLURM_CPUS_PER_TASK:-8}
GATE_WORK=$W GATE_BIN=$B/bin/$REF $G record $B/gate_$REF.manifest
for b in $BINS; do echo "== $b"; GATE_WORK=$W GATE_BIN=$B/bin/$b $G check $B/gate_$REF.manifest; done
for b in $SANITY; do echo "== sanity $b"; GATE_WORK=$W GATE_BIN=$B/bin/$b GATE_ALT="taskset -c $(taskset -cp $$ | sed 's/.*: //; s/[,-].*//')" $G sanity | grep -v '^    '; done
echo "== laptop manifest vs HPC"; diff <(grep -v '^#' $B/snaps/final/gate_data/baseline.manifest) <(grep -v '^#' $B/gate_$REF.manifest) | grep '^[<>]' | awk '{print $1, $2, $4}' | sort | uniq -c | head -40
rm -rf $W
echo GATE-DONE
