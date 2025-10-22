#!/bin/bash
#SBATCH --job-name=rl_xr              # Job name
#SBATCH -p high                    # short, medium, high, high-cpu
#SBATCH --nodes=1                      # Request 1 node
#SBATCH --ntasks=1                     # Single task
#SBATCH --cpus-per-task=5             # CPUs per task        ----default 10 cores
#SBATCH --mem=16G                       # Memory allocation   ----default 16G
#SBATCH -o ../logs_hpc/%x_%j.out
#SBATCH -e ../logs_hpc/%x_%j.err


source ~/.bashrc

PROJECT_DIR="$SLURM_SUBMIT_DIR"
LOG_DIR="$PROJECT_DIR/../logs_hpc"
SWEEP_ID="wn-upf/asynchronix-python_RL/abv10stl"

# --- go to python_RL folder inside project ---
cd "$PROJECT_DIR" || { echo "❌ Cannot cd to $PROJECT_DIR/python_RL"; exit 1; }

wandb agent "$SWEEP_ID" > "${LOG_DIR}/RL_agent_${SLURM_JOB_ID}.log" 2>&1 &
wait