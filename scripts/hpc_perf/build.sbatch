#!/bin/bash
#SBATCH -J pp.build
#SBATCH --partition=high
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=03:00:00
# Builds XR_sim (--release and --profile optimized) of every snapshot in $B/snaps (or of $LABELS), one after another
# into one shared target dir, and copies the binaries to $B/bin/<label>.release / <label>.optimized.
set -uo pipefail
set +u; source ~/.bashrc || true; set -u
export PATH=$HOME/.cargo/bin:$PATH
B=${B:?B=<hpc dir>}
export CARGO_TARGET_DIR=$B/target CARGO_BUILD_JOBS=${SLURM_CPUS_PER_TASK:-8}
mkdir -p $B/bin
for L in ${LABELS:-$(ls $B/snaps)}; do
  cd $B/snaps/$L/asynchronix
  for P in release optimized; do
    [ -x $B/bin/$L.$P ] && { echo "skip $L.$P (built)"; continue; }
    t0=$(date +%s)
    if cargo build --profile $P --example XR_sim >$B/bin/$L.$P.log 2>&1; then
      cp $CARGO_TARGET_DIR/$P/examples/XR_sim $B/bin/$L.$P; echo "built $L.$P in $(( $(date +%s)-t0 )) s"
    else echo "BUILD FAILED $L.$P"; grep -E "^error" -A6 $B/bin/$L.$P.log | head -20; fi
  done
done
echo BUILD-ALL-DONE
