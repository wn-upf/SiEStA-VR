#!/bin/bash
#SBATCH --export=ALL
#SBATCH -J xr_sims               # job name
#SBATCH --partition=high         # partition
#SBATCH --nodes=1                # number of nodes

#SBATCH --mem=64G               # memory
#SBATCH --time=48:00:00          # max walltime (adjust!)
#SBATCH --cpus-per-task=48        # example
#SBATCH -o logs_hpc/%x_%j.out
#SBATCH -e logs_hpc/%x_%j.err

source ~/.bashrc

module load CUDA
module load x265
module load x264

export PATH=$HOME/.local/bin:$PATH

NUMBER_OF_JOBS=8
SERIAL_EXECUTION=0
#############################################################################
# RL params: 
observation_type=1 ## 0-> Raw unscaled obs, 1 -> Scaled in expected bounds, 2-> Running Normalization. 
reward_mode=0
T_ABR=0.3
#############################################################################
# initial_bitrate_mbps=( 100.0 )

TEST_TYPE=("STD") # Can be "BW", "JI", "PL", "RANDOM", or "STD" for different emulated tests (or none)

simTime=10.0
k_queue=1000
# RANDOM_SEEDS=({1..3})
RANDOM_SEEDS=(666)
mean_length_BG=12000.0     ## BG traffic length 
# rate_bps_src_BG=50E6;   ## BG traffic arrival rate

# rates_bps_BGtraffic=( 100E6 200E6 300E6 400E6 600E6 )
rates_bps_BGtraffic=( 100E6 )

N_XR=( 5 ) 
MLO_CONFIGS=("MLO3") ## MLO0: SLO -> 80 Mhz, MLO1 -> MLO 80_80 MHz , MLO2 -> MLO 80_160 MH< , MLO3 -> MLO 80_320 MHz channels 
IS_UL_BG=( 0 ) ## 0 -> DL, 1-> UL, 2 -> DL + UL 

N_BGs=( 1 )
distance_list=( 8.0 ) ## Distance to AP of users
num_close_users=( 0 )     ## number of users with alternate AP distance
distance_close_users=( 1.5 )  ## to have heterogeneous distances, (only if num_close_users > 0)
everest_tests=0             ## Everest tests randomizes all VR STA distances, makes them move in 1 m radius. 

PL=0.1
fps_list=( 90.0 )  # VR Only
initial_bitrate_mbps=( 100.0 ) # VR Only
# IS_UL_BG=(  2 )

ABR_ENABLED=( 0 )
nest_profiles=( 1 ) ## balanced and that's it                                  2 => {NestVrProfile::Anxious},

video_samples=("snow")

intrarefresh_choice=( 1 ) ## let's always assume intra-refresh
GoP_sizes=(90)

temp_file=$(mktemp)
SHUFFLED_CMDS=$(mktemp)

SIM_COUNT=0                 # counter of simulations, not an input arg
# N_STEPS_RL=7_500_000        ## Counter of simulations to iterate through for an RL training, needs to be synced (admittedly manually) with the python script.   

# ID for the W&B sweep you want the agent to join.
SWEEP_ID="wn-upf/asynchronix-python_RL/i9igunmc"
CONDA_ENVV="vr_sim"
script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR="$SLURM_SUBMIT_DIR"

RUN_ID="${SLURM_JOB_ID:-$$}_$RANDOM"   # or add your SIM_COUNT etc.



# Define the function to execute on Ctrl+C
handle_interrupt() {
    echo "Simulation interrupted."
    
    # kill -9 -$(ps -o pgid= $PY_TERM_PID | grep -o '[0-9]*') 2>/dev/null
    exit 1
}
# Set up the trap for SIGINT (Ctrl+C)
trap handle_interrupt SIGINT

# cargo build --release --example XR_sim
cargo build --release --example XR_sim
sleep 5 ## for being able to see if there were any errors before sims start

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
                                                            for MLO_config in "${MLO_CONFIGS[@]}"; do 
                                                                for rate_BG in "${rates_bps_BGtraffic[@]}"; do 

                                                                    NAME_ABR="ABR_${ABR}"
                                                                    # Create the folder for results saving
                                                                    name_folder=$(printf "sim_T%.0f_D%.0f_Br%.1f_PL%.1f_NXR%.0f_NBG%.0f_BGThr%.2f_UL%.0f_%s_%s_FPS%.0f_Nclose%d_dclose%.1f_S%.0f_GoP%.0f_IR%.0f_ABR%.0f_nest%.0f_obs%.0f_Tabr%.3f_%s_%s" \
                                                                                "$simTime" "$distance" "$bitrate" "$PL" "$nxr" "$nbg" "$rate_BG" "$is_ul" "$test" "$video_sample" "$FPS" "$close_users" "$close_distance" "$seed" "$gop" "$intrarefresh" "$ABR" "$nest_profile" "$observation_type" "$T_ABR" "$NAME_ABR" "$MLO_config")
                                                            
                                                                    (( SIM_COUNT++ ))  # ← increment
                                                                    # mkdir -p "Results/$name_folder"

                                                                    # echo "./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile $everest_tests $SIM_COUNT $observation_type $reward_mode $T_ABR $NAME_ABR $MLO_config 2>&1 | tee Results/$name_folder/sim.log" >> "$temp_file"
                                                                    # # else                                    ## Serial execution
                                                                    # rm out_log.ans
                                                                    # script -c "./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile $everest_tests $SIM_COUNT $observation_type $reward_mode $T_ABR $NAME_ABR $MLO_config" "out_log.ans"
                                                                    # sleep 5
                                                                    # # fi



                                                                    # --- Temporary Profiling Block using samply ---
                                                                    # if DEBUG_PROFILE_FLAMEGRAPH
                                                                    echo "--- Starting Profiling Run for XR_sim with samply ---"

                                                                    # Define the output file
                                                                    PROFILE_HTML_FILE="XR_sim_profile.html"

                                                                    # Run samply against your binary and arguments. 
                                                                    # The -o flag tells samply where to save the profile.
                                                                    samply record -o $PROFILE_HTML_FILE -- \
                                                                        ./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile $everest_tests $SIM_COUNT $observation_type $reward_mode $T_ABR $NAME_ABR $MLO_config

                                                                    echo "--- Interactive profile saved to $PROFILE_HTML_FILE ---"
                                                                    exit 0 # Exit the job after generating the profile
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
    done
done

# After writing to temp file
echo "Contents of temp file:"
# cat "$temp_file"
echo " --- Number of simulations: $SIM_COUNT --- \n"


# Shuffle the collected commands into a new temporary file
shuf "$temp_file" > "$SHUFFLED_CMDS"
rm "$temp_file" # Clean up the un-shuffled file




# RL_STEPS=1 ## should be enough for training 10 agents 
# for RL_ITERATION in $(seq 1 $RL_REPETITIONS); do
if [ "$SERIAL_EXECUTION" -eq 1 ]; then
    echo "Starting randomized SERIAL execution of $SIM_COUNT simulations..."
    # Execute commands one by one, using a subshell for execution to ensure $cmd is treated correctly
    while IFS= read -r cmd; do
        echo "Executing: $cmd"
        /bin/bash -c "$cmd"
        # sleep 5

    done < "$SHUFFLED_CMDS"
    
elif [ "$SERIAL_EXECUTION" -eq 0 ]; then
    echo "Starting randomized PARALLEL execution of $SIM_COUNT simulations with $NUMBER_OF_JOBS threads..."
    # Use GNU parallel on the shuffled list
    parallel -j "$NUMBER_OF_JOBS" < "$SHUFFLED_CMDS"
    fi
# done 


rm "$temp_file"
rm "$sorted_file"

echo "ALL JOBS FINISHED!!!"

# cargo run --release --example two_bitrates_tests

echo "XRUN FINALLY FINISHED!!!"
