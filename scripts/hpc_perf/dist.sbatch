#!/bin/bash
#SBATCH -J pp.dist
#SBATCH --partition=high
#SBATCH --nodes=1
#SBATCH --cpus-per-task=60
#SBATCH --mem=64G
#SBATCH --time=08:00:00
# Distribution comparison of two builds on the paper's heavy points: SLO 4-6 users (every ABR) and MLO 8 / 10 users
# (the MLO sweep's ABRs), SEEDS seeds, STOP simulated seconds, the paper's command line (NADA with XRSIM_NADA_FIX=2, as
# in the revision). Results in $B/dist/<label>/Results_rerun_<SLO|MLO>/sim_*, then extract_runs.py per label.
set -uo pipefail
set +u; source ~/.bashrc || true; set -u
B=${B:?B=<hpc dir>}; LABELS=${LABELS:-"base stageD"}; SEEDS=${SEEDS:-"1 2 3 4 5 6 7 8 9 10"}; STOP=${STOP:-180.0}
J=${SLURM_CPUS_PER_TASK:-60}
CMDS=$B/dist/cmds.txt; mkdir -p $B/dist; : >$CMDS
for L in $LABELS; do
  for seed in $SEEDS; do
    for pt in "SLO 4 SLO80" "SLO 5 SLO80" "SLO 6 SLO80" "MLO 8 MLO80-80" "MLO 10 MLO80-80"; do
      set -- $pt; sw=$1; nxr=$2; mlo=$3
      abrs="0 1 2 4 5 9"; [ $sw = MLO ] && abrs="0 1 2 5 9"
      for abr in $abrs; do
        env=""; [ $abr = 5 ] && env="XRSIM_NADA_FIX=2"
        out=$B/dist/$L/Results_rerun_$sw
        echo "cd $B/snaps/$L && env $env $B/bin/$L.release $STOP 12000.0 5.0 100.0 0.1 $nxr 0 5000 0 STD snow_short 90.0 0 1.5 $seed 30 1 0 1 $abr 1 0 1 1 0 1.0 $mlo 0 1 64 HEVC $out 0 0 1 >/dev/null 2>&1 || echo FAILED $L $sw $nxr $abr $seed" >>$CMDS
      done
    done
  done
done
for L in $LABELS; do mkdir -p $B/dist/$L/Results_rerun_SLO $B/dist/$L/Results_rerun_MLO; done
# heaviest first (MLO 10 users), so the tail of the job is short runs
sort -t' ' -k1,1 $CMDS | awk '{print ($0 ~ /MLO80-80/ ? 0 : 1), $0}' | sort -s -k1,1n | cut -d' ' -f2- >$CMDS.sorted
echo "$(wc -l <$CMDS) runs on $J CPUs"; t0=$(date +%s)
parallel -j $J <$CMDS.sorted
echo "runs done in $(( $(date +%s)-t0 )) s"
set +u; conda activate oracle_bw_cuda; set -u
for L in $LABELS; do python $B/extract_runs.py $B/dist/$L $B/dist/runs_$L.parquet $J; done
echo DIST-DONE
