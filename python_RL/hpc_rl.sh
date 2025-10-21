#!/bin/bash
#SBATCH --job-name=rl_xr              # Job name
#SBATCH -p high                    # short, medium, high, high-cpu
#SBATCH --nodes=1                      # Request 1 node
#SBATCH --ntasks=1                     # Single task
#SBATCH --cpus-per-task=5             # CPUs per task        ----default 10 cores
#SBATCH --mem=16G                       # Memory allocation   ----default 16G
#SBATCH -o logs_hpc/%x_%j.out
#SBATCH -e logs_hpc/%x_%j.err



source ~/.bashrc

module load CUDA
module load x265
module load x264
PROJECT_DIR="$SLURM_SUBMIT_DIR"

LOG_DIR="$PROJECT_DIR/logs_hpc"
SWEEP_ID="wn-upf/asynchronix-python_RL/abv10stl"



wandb agent "$SWEEP_ID" > "${LOG_DIR}/RL_agent${SLURM_JOB_ID}.log" 2>&1 &
