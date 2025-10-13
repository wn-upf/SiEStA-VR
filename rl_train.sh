#!/bin/bash
#SBATCH --export=ALL
#SBATCH -J xr_sims               # job name
#SBATCH --partition=high         # partition
#SBATCH --nodes=1                # number of nodes
####### SBATCH --gres=gpu:1
#SBATCH --constraint=nvenc
###### #SBATCH --exclusive
#SBATCH --mem=128G               # memory
#SBATCH --time=48:00:00          # max walltime (adjust!)
#SBATCH --cpus-per-task=64        # example
#SBATCH -o logs_hpc/%x_%j.out
#SBATCH -e logs_hpc/%x_%j.err

source ~/.bashrc

module load CUDA
module load x265
module load x264


SWEEP_ID="wn-upf/asynchronix-python_RL/0uk46bio"


export PATH=$HOME/.local/bin:$PATH


NUMBER_OF_JOBS=2
SERIAL_EXECUTION=1
# initial_bitrate_mbps=( 100.0 )

TEST_TYPE=("STD") # Can be "BW", "JI", "PL", "RANDOM", or "STD" for different emulated tests (or none)

simTime=150.0
k_queue=10000
mean_length_BG=12000.0     ## BG traffic length 
rate_bps_src_BG=20E6;   ## BG traffic arrival rate

distance_list=( 1.5 )
distance_close_users=( 1.5 )  ## to have heterogeneous distances
num_close_users=( 0 )     ## number of users with alternate distance
N_XR=( 1 2 3 4) 
PL=0.1

fps_list=( 90.0 )
initial_bitrate_mbps=( 10.0 20.0 40.0 ) 

# ABR_ENABLED=( 0 1 2 )  ## 0 => CBR , 1 => Nest-VR, 2 => Everest,  3 => RL approach, 4=> GCC, 5 => NADA (TODO)

ABR_ENABLED=( 3 )
nest_profiles=( 1 ) ## balanced and that's it                                  2 => {NestVrProfile::Anxious},
RANDOM_SEEDS=({1..500})
# RANDOM_SEEDS=( 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 )
# video_samples=("garp4k" "snow" "assemble" "cut_video" "furbo" "randomVid")
video_samples=("snow")
N_BGs=(0)
IS_UL_BG=(0)

intrarefresh_choice=( 1 ) ## let's always assume intra-refresh
GoP_sizes=(90)

everest_tests=1

temp_file=$(mktemp)
SHUFFLED_CMDS=$(mktemp)

SIM_COUNT=0                 # counter of simulations, not an input arg
# N_STEPS_RL=7_500_000        ## Counter of simulations to iterate through for an RL training, needs to be synced (admittedly manually) with the python script.   


# ID for the W&B sweep you want the agent to join.
CONDA_ENVV="vr_sim"
script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR="$SLURM_SUBMIT_DIR"

RUN_ID="${SLURM_JOB_ID:-$$}_$RANDOM"   # or add your SIM_COUNT etc.


export ZMQ_ACTION_EP="ipc:///tmp/xr_${RUN_ID}_action"
export ZMQ_STEP_EP="ipc:///tmp/xr_${RUN_ID}_step"
export ZMQ_TRAINER_EP="ipc:///tmp/xr_${RUN_ID}_trainer"

if [[ "${USER:-}" == "fmaura" ]]; then
    # --- HPC (SLURM) Mode ---
    echo "🚀 Detected HPC environment (User: $USER). Using srun."
    IS_HPC=1
    LOG_DIR="logs_hpc"
    mkdir -p "$LOG_DIR" # Ensure log directory exists

    # Set library path for Conda
    export LD_LIBRARY_PATH="$CONDA_PREFIX/lib:$LD_LIBRARY_PATH"

    # 1. Launch ZMQ Server in the background on the allocated node
    ZMQ_LOG_FILE="${LOG_DIR}/zmq_server_${SLURM_JOB_ID}.log"
    echo "🔹 Launching ZMQ Server... Log: $ZMQ_LOG_FILE"
    srun --ntasks=1 --cpus-per-task=8 --exclusive --cpu-bind=cores \
    /bin/bash -c "
      source ~/miniconda3/etc/profile.d/conda.sh
      conda activate $CONDA_ENVV
      python $PROJECT_DIR/python_RL/zmq_server.py
    " > "$ZMQ_LOG_FILE" 2>&1 &
    ZMQ_SERVER_PID=$!
    echo "  -> ZMQ Server started with PID: $ZMQ_SERVER_PID"

    # Give the server a moment to start up
    sleep 3

    # 2. Launch Python trainer (W&B Agent) in the background on the same node
    AGENT_LOG_FILE="${LOG_DIR}/wandb_agent_${SLURM_JOB_ID}.log"
    echo "🔹 Launching W&B Agent... Log: $AGENT_LOG_FILE"
    srun --ntasks=1 ---cpus-per-task=8 -exclusive --cpu-bind=cores  \
        /bin/bash -c "
        source ~/miniconda3/etc/profile.d/conda.sh
        conda activate $CONDA_ENVV
        cd $PROJECT_DIR/python_RL  # <-- Add this line
        wandb agent $SWEEP_ID
        " > "$AGENT_LOG_FILE" 2>&1 &
    WANDB_AGENT_PID=$!
    echo "  -> W&B Agent started with PID: $WANDB_AGENT_PID"

else
    # --- Non-HPC (Local Desktop) Mode ---
    echo "🖥️  Detected non-HPC environment. Using gnome-terminal."
    IS_HPC=0
    PROJECT_DIR="/home/boris/Desktop/Rust_MG1/asynchronix"
    # 1. Launch ZMQ Server in a new terminal
    echo "🔹 Launching ZMQ Server in a new terminal..."
    gnome-terminal --disable-factory -- bash -c "
      echo '--- ZMQ Server Terminal ---'
      cd '$PROJECT_DIR/python_RL/'
      python zmq_server.py
      exec bash" &
    ZMQ_SERVER_PID=$!
    echo "  -> ZMQ Server terminal process started with PID: $ZMQ_SERVER_PID"

    # Give the server a moment to start up
    sleep 2

    # 2. Launch W&B Agent in another new terminal
    echo "🔹 Launching W&B Agent in a new terminal..."
    gnome-terminal --disable-factory -- bash -c "
      echo '--- W&B Agent Terminal ---'
      cd '$PROJECT_DIR/python_RL/'
      wandb agent '$SWEEP_ID'
      exec bash" &
    WANDB_AGENT_PID=$!
    echo "  -> W&B Agent terminal process started with PID: $WANDB_AGENT_PID"
fi



# Define the function to execute on Ctrl+C
handle_interrupt() {
    echo "Simulation interrupted."
    
    # kill -9 -$(ps -o pgid= $PY_TERM_PID | grep -o '[0-9]*') 2>/dev/null
    exit 1
}
# Set up the trap for SIGINT (Ctrl+C)
trap handle_interrupt SIGINT
cargo build --release --example XR_sim

# sleep 1
# for ((i=0; i<5; i++)); do
    # echo "SSSSIM i = $i"


    for test in "${TEST_TYPE[@]}"; do 
        for nbg in "${N_BGs[@]}"; do
            for nxr in "${N_XR[@]}"; do
                for is_ul in "${IS_UL_BG[@]}"; do
                    for bitrate in "${initial_bitrate_mbps[@]}"; do 
                        for video_sample in "${video_samples[@]}"; do 
                            for FPS in "${fps_list[@]}"; do 
                                for close_users in "${num_close_users[@]}"; do 
                                    for close_distance in "${distance_close_users[@]}"; do 
                                    for seed in "${RANDOM_SEEDS[@]}"; do 
                                            for distance in "${distance_list[@]}"; do 
                                                for gop in "${GoP_sizes[@]}"; do 
                                                    for intrarefresh in "${intrarefresh_choice[@]}"; do 
                                                        for ABR in "${ABR_ENABLED[@]}"; do 
                                                            for nest_profile in "${nest_profiles[@]}"; do 

                                                                # Create the folder for results saving
                                                                name_folder=$(printf "sim_T%.0f_D%.0f_Br%.1f_PL%.1f_NXR%.0f_NBG%.0f_UL%.0f_%s_%s_FPS%.0f_Nclose%d_dclose%.1f_S%.0f_GoP%.0f_IR%.0f_ABR%.0f_nest%.0f" \
                                                                            "$simTime" "$distance" "$bitrate" "$PL" "$nxr" "$nbg" "$is_ul" "$test" "$video_sample" "$FPS" "$close_users" "$close_distance" "$seed" "$gop" "$intrarefresh" "$ABR" "$nest_profile")
                                                        
                                                                (( SIM_COUNT++ ))  # ← increment
                                                                # mkdir -p "Results/$name_folder"

                                                                # if [ "$SERIAL_EXECUTION" -eq 0 ]; then  ## Parallel execution
                                                                    # echo "RUNNING SIM: $name_folder"

                                                                    # echo "./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_bps_src_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile $everest_tests $SIM_COUNT > sim.log 2>&1" >> "$temp_file"
                                                                    echo "./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_bps_src_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile $everest_tests $SIM_COUNT 2>&1 | tee Results/sim.log" >> "$temp_file"
                                                                # else                                    ## Serial execution
                                                                # script -c "./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_bps_src_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile $everest_tests $SIM_COUNT" "out_log.ans"
                                                                # sleep 5
                                                                #         rm out_log.ans
                                                                
                                                                # fi
                                                            done
                                                        done 
                                                    done
                                                done    
                                            done
                                        done
                                    done
                                done
                            done
                        done
                    done
                done 
            done
        done
    done
# done

# After writing to temp file
echo "Contents of temp file:"
# cat "$temp_file"
echo " --- Number of simulations: $SIM_COUNT --- \n"


#########################################################################################################
### Sort temp_file lines by the NXR value (field with "_NXR<val>_") # - Extract NXR using sed/grep, then sort numerically in reverse (largest first) Then shuffle within each group of equal NXR.

# sorted_file=$(mktemp)

# awk '{print $7, $0}' "$temp_file" \
#   | sort -k1,1 -n -r \
#   | cut -d' ' -f2- \
#   > "$sorted_file"

# echo "Ordered (largest NXR first, still randomized within each group):"
# cat "$sorted_file"
# echo " --- Number of simulations: $SIM_COUNT ---"

# parallel -j "$NUMBER_OF_JOBS" < "$sorted_file"

##########################################################################################################
# shuf "$temp_file" | parallel -j "$NUMBER_OF_JOBS" 



# Shuffle the collected commands into a new temporary file
shuf "$temp_file" > "$SHUFFLED_CMDS"
rm "$temp_file" # Clean up the un-shuffled file

RL_STEPS=20 ## should be enough for training 1M steps  
for RL_ITERATION in $(seq 1 $RL_REPETITIONS); do
    if [ "$SERIAL_EXECUTION" -eq 1 ]; then
        echo "Starting randomized SERIAL execution of $SIM_COUNT simulations..."
        # Execute commands one by one, using a subshell for execution to ensure $cmd is treated correctly
        while IFS= read -r cmd; do
            echo "Executing: $cmd"
            /bin/bash -c "$cmd"
            sleep 5

        done < "$SHUFFLED_CMDS"
        
    elif [ "$SERIAL_EXECUTION" -eq 0 ]; then
        echo "Starting randomized PARALLEL execution of $SIM_COUNT simulations with $NUMBER_OF_JOBS threads..."
        # Use GNU parallel on the shuffled list
        parallel -j "$NUMBER_OF_JOBS" < "$SHUFFLED_CMDS"
    fi
done 


rm "$temp_file"
rm "$sorted_file"

echo "ALL JOBS FINISHED!!!"

# cargo run --release --example two_bitrates_tests

echo "XRUN FINALLY FINISHED!!!"
