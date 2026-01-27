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

NUMBER_OF_JOBS=12
SERIAL_EXECUTION=0

DEBUG_PROFILE_FLAMEGRAPH=0
DEBUG_LOGS=0

#############################################################################

simTime=25.0

EMU_TEST_TYPE=("STD") #  emulated link tests: Can be "BW", "JI", "PL", "RANDOM", or "STD" for different effects. (STD does nothing)
k_queue=5000  ## Leaves room for UL traffic (Per-sta). DL traffic queue at AP is constant set at 1K packets
# RANDOM_SEEDS=(1)
MLO_policies=(1) ## 0 => PrimaryFirst, 1 => Opportunistic, 2 => LyapunovBackpressure. 

RANDOM_SEEDS=({1..3})
# MLO_policies=(0 1 2) ## 0 => PrimaryFirst, 1 => Opportunistic, 2 => LyapunovBackpressure. 

############################################################################# <- BG Traffic
N_BGs=( 0 )                ## Nº of BG STAs
mean_length_BG=12000.0         ## BG traffic length (bits) 
# rates_bps_BGtraffic=( 20000 50000 80000 )  ## Packets per second 
rates_bps_BGtraffic=(20000)  ## Packets per second 

IS_UL_BG=( 0 )             ## 0 -> DL, 1-> UL, 2 -> DL + UL 
############################################################################# <- 802.11 Parameters

EDCA_BE_MODE=(0) ## Set to 1 if we want all traffic in EDCA_BE category. 
MLO_CONFIGS=( "MLO0" "MLO1") ## MLO0: SLO -> 80 Mhz, MLO1 -> MLO 80_80 MHz , MLO2 -> MLO 80_160 MH< , MLO3 -> MLO 80_320 MHz channels 
everest_tests=1              ## Randomizes all VR STA distances, makes them move in 1 m radius, 5 m/s speed random walk. 
distance_list=( 2.5)         ## Distance to AP of users                                     (ignored when everest_tests==1)
num_close_users=( 0 )        ## number of users with alternate AP distance (to the one configured before)
distance_close_users=( 1.5 ) ## to have heterogeneous distances            (if num_close_users > 0)
PL=0.1
packs_per_ampdu=( 64 )
############################################################################# <- VR streaming Parameters
CODEC_CHOICES=("HEVC" "AV1") ## can be "HEVC" or "AV1"
N_XR=( 1 2 3 4 5 6 ) 
initial_bitrate_mbps=( 20.0 ) # VR Only
fps_list=( 90.0 )              # VR Only
ABR_ENABLED=( 0 1 2 4 5 ) ## 0 -> CBR, 1 -> NeSt-VR, 2-> Everest, 3-> ReinforcementLearner, 4-> GCC, 5-> NADA, 6-> FoVOptix 
T_ABR=1.0         ## Time between updates of ABR, also affects RL mode. 
nest_profiles=( 1 ) ## balanced and that's it 
# video_samples=("swordsmith" )
video_samples=("snow")
intrarefresh_choice=( 0 ) ## intra-refresh enabled if true
GoP_sizes=(30)            ## Make sure GoP size is always less than (T_abr·FPS), and a common divisor 
############################################################################# <- RL training Parameters

observation_type=1 ## 0-> Raw unscaled obs, 1 -> Scaled in 'expected'/hardcoded bounds, 2-> Running Normalization. 
reward_mode=0
temp_file=$(mktemp)
SHUFFLED_CMDS=$(mktemp)
# N_STEPS_RL=7_500_000        ## Counter of simulations to iterate through for an RL training, needs to be synced with the python script.   

#########################################################################################################################
SWEEP_ID="wn-upf/asynchronix-python_RL/i9igunmc" # ID for the W&B sweep for the agent.
CONDA_ENVV="vr_sim"
script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR="$SLURM_SUBMIT_DIR"

RUN_ID="${SLURM_JOB_ID:-$$}_$RANDOM"   
SIM_COUNT=0                            # counter of simulations, not an input arg
#########################################################################################################################
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
sleep 4 ## for being able to see if there were any errors before sims start


for test in "${EMU_TEST_TYPE[@]}"; do 
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
                                                                    for edca_be in "${EDCA_BE_MODE[@]}"; do 
                                                                        for MLO_policy in "${MLO_policies[@]}"; do 
                                                                            for ampdu_packs in "${packs_per_ampdu[@]}"; do 
                                                                                for codec in "${CODEC_CHOICES[@]}"; do 

                                                                                    NAME_ABR="ABR_${ABR}"
                                                                                    (( SIM_COUNT++ ))  # ← increment

                                                                                    echo "./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile $everest_tests $SIM_COUNT $observation_type $reward_mode $T_ABR $NAME_ABR $MLO_config $edca_be $MLO_policy $ampdu_packs $codec 2>&1 | tee Results/$name_folder/sim.log" >> "$temp_file"
                                                                                                                                                                
                                                                                    if [ "$DEBUG_LOGS" = 1 ] || [ "$SERIAL_EXECUTION" = 1 ]; then
                                                                                        rm out_log.ans
                                                                                        script -c "./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile $everest_tests $SIM_COUNT $observation_type $reward_mode $T_ABR $NAME_ABR $MLO_config $edca_be $MLO_policy $ampdu_packs $codec" "out_log.ans"
                                                                                        sleep 5
                                                                                    fi

                                                                                    # --- Profiling Block using samply ---
                                                                                    if [ "$DEBUG_PROFILE_FLAMEGRAPH" = 1 ]; then
                                                                                        echo "--- Starting Profiling Run for XR_sim with samply ---"

                                                                                        # Define the output file
                                                                                        PROFILE_HTML_FILE="XR_sim_profile.html"

                                                                                        # Run samply against your binary and arguments. 
                                                                                        # The -o flag tells samply where to save the profile.
                                                                                        samply record -o $PROFILE_HTML_FILE -- \
                                                                                            ./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile $everest_tests $SIM_COUNT $observation_type $reward_mode $T_ABR $NAME_ABR $MLO_config $edca_be $MLO_policy $ampdu_packs $codec 

                                                                                        echo "--- Interactive profile saved to $PROFILE_HTML_FILE ---"
                                                                                        exit 0 # Exit the job after generating the profile
                                                                                    fi 
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
