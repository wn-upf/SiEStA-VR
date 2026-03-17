#!/bin/bash
#SBATCH --export=ALL
#SBATCH -J ABR_5m            # job name
#SBATCH --partition=high         # partition
#SBATCH --nodes=1                # Nodes PER TASK (Always 1 for arrays)
#SBATCH --array=0-1             # <--- INPUT: Run n nodes total (indices 0,1,2,3). Change to 0-9 for 10 nodes, etc.
#SBATCH --mem=64G                # memory
#SBATCH --time=96:00:00          # max walltime
#SBATCH --cpus-per-task=24       # CPUs per node
#SBATCH -o logs_hpc/%x_%A_%a.out # %A=Job ID, %a=Array Index (Log separation)
#SBATCH -e logs_hpc/%x_%A_%a.err

source ~/.bashrc

module load CUDA
module load x265
module load x264

export PATH=$HOME/.local/bin:$PATH

NUMBER_OF_JOBS=8
SERIAL_EXECUTION=1

DEBUG_PROFILE_FLAMEGRAPH=0
DEBUG_LOGS=0

#############################################################################

results_path_name="Results_MLO_ABR_10seeds_5m"
simTime=45.0
EMU_TEST_TYPE=("STD")   #  emulated link tests: Can be "BW", "JI", "PL", "RANDOM", or "STD" for different effects. (STD does nothing)
k_queue=5000            ## Leaves room for UL traffic (Per-sta). DL traffic queue at AP is constant set at 1K packets
# RANDOM_SEEDS=(1)
MLO_policies=(1)        ## 0 => PrimaryFirst, 1 => Opportunistic, 2 => LyapunovBackpressure. 
                        ## (Is ignored if the const STR_PLUS_MODE_MLO is set to true)
RANDOM_SEEDS=({1..10})
############################################################################# <- BG Traffic
N_BGs=( 0 )                    ## Nº of BG STAs
mean_length_BG=12000.0         ## BG traffic length (bits) 
rates_bps_BGtraffic=( 5000 )    ## Packets per second 
IS_UL_BG=( 0 )                 ## 0 -> DL, 1-> UL, 2 -> DL + UL 
############################################################################# <- 802.11 Parameters
EDCA_BE_MODE=(0) ## Set to 1 if we want all traffic in EDCA_BE category. 
MLO_CONFIGS=( "SLO80" "MLO80-80") ## Regex-based: e.g. SLO80 -> SLO with 80 Mhz, MLO80-80 -> MLO with two 80_80 MHz channels, MLO80-320 for 80_320 MHz channels, etc. 
# MLO_CONFIGS=( "SLO80"  )                         ## Regex-based: e.g. SLO80 -> SLO with 80 Mhz, MLO80-80 -> MLO with two 80_80 MHz channels, MLO80-320 for 80_320 MHz channels, etc. 

RANDOMWALK_TEST=0            ## If == 1: Randomizes all VR STA distances, makes them move in 1 m radius, 5 m/s speed random walk. 
distance_list=( 5.0 )         ## Distance to AP of users                                     (ignored when RANDOMWALK_TEST==1)
num_close_users=( 0 )        ## number of users with alternate AP distance (to the one configured before)
distance_close_users=( 1.5 ) ## to have heterogeneous distances            (if num_close_users > 0)
PL=0.1
packs_per_ampdu=( 64 )

############################################################################# <- VR streaming Parameters
# CODEC_CHOICES=("AV1" "HEVC")    ## can be "HEVC" or "AV1"
CODEC_CHOICES=( "HEVC" )
N_XR=( 9 10) 
# N_XR=( 1 ) 

initial_bitrate_mbps=( 100.0 )  
fps_list=( 90.0 )    
ABR_ENABLED=( 1 2 )             ## 0 -> CBR, 1 -> NeSt-VR, 2-> Everest, 3-> ReinforcementLearner, 4-> GCC, 5-> NADA, 6-> FoVOptix 
T_ABR=1.0                       ## Time between updates of ABR, also affects RL mode. 
nest_profiles=( 1 )             ## specific setting for Nest-vr
video_samples=("snow_short")    ## snow (HEVC only for now), swordsmith (AV1/HEVC)
intrarefresh_choice=( 1 )       ## Only if USE_FFMPEG_DEMO enabled: intra-refresh enabled if true
GoP_sizes=(30)                  ## Only if USE_FFMPEG_DEMO enabled:  Make sure GoP size is always less than (T_abr·FPS), and a common divisor to them
############################################################################# <- RL training Parameters
observation_type=1              ## 0-> Raw unscaled obs, 1 -> Scaled in 'expected'/hardcoded bounds, 2-> Running Normalization. 
reward_mode=0
temp_file=$(mktemp)
SHUFFLED_CMDS=$(mktemp)
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
    exit 1
}
# Set up the trap for SIGINT (Ctrl+C)
trap handle_interrupt SIGINT

# cargo build --release --example XR_sim
cargo build --release --example XR_sim
sleep 4 ## for being able to see if there were any errors before sims start, else it would use the last best compiled code

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

                                                                                    echo "./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile $RANDOMWALK_TEST $SIM_COUNT $observation_type $reward_mode $T_ABR $MLO_config $edca_be $MLO_policy $ampdu_packs $codec $results_path_name" >> "$temp_file"
                                                                                                                                                                
                                                                                    if [ "$DEBUG_LOGS" = 1 ] || [ "$SERIAL_EXECUTION" = 1 ]; then
                                                                                        rm out_log.ans
                                                                                        script -c "./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile $RANDOMWALK_TEST $SIM_COUNT $observation_type $reward_mode $T_ABR $MLO_config $edca_be $MLO_policy $ampdu_packs $codec $results_path_name" "out_log.ans"
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
                                                                                            ./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile $RANDOMWALK_TEST $SIM_COUNT $observation_type $reward_mode $T_ABR $NAME_ABR $MLO_config $edca_be $MLO_policy $ampdu_packs $codec $results_path_name

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



# --- PREPARE LIST ---
# 1. Create the initial list

JOB_ID=${SLURM_ARRAY_JOB_ID:-$$}
RAW_FILE="all_cmds_raw_${JOB_ID}.txt"
WEIGHTED_FILE="weighted_tasks_${JOB_ID}.txt"



cat "$temp_file" > "$RAW_FILE"
rm "$temp_file"

# 2. WEIGHTED SORTING (The "NXR" Fix)
# In your echo command, $nxr is the 7th argument.
# This sorts the file so the heaviest simulations (highest NXR) are at the top.
# awk '{print $8, $0}' "$RAW_FILE" | sort -rn | cut -d' ' -f2- > "$WEIGHTED_FILE"

awk '{
    mlo_weight = 1; # Default weight
    
    if ($27 == "MLO80-320") { mlo_weight = 2 }
    else if ($27 == "MLO80-80") { mlo_weight = 2 }
    else if ($27 == "SLO80") { mlo_weight = 1 }
    else {mlo_weight = 2}
    
    # Prepend the NXR and the MLO weight to the line
    print $8, mlo_weight, $0
}' "$RAW_FILE" | sort -k1,1rn -k2,2rn | cut -d' ' -f3- > "$WEIGHTED_FILE"


# 3. Get node info
TOTAL_TASKS=$(wc -l < "$WEIGHTED_FILE")
NODE_ID=${SLURM_ARRAY_TASK_ID:-0}           
TOTAL_NODES=${SLURM_ARRAY_TASK_COUNT:-1}    

# 4. INTERLEAVED SELECTION (The "Deck of Cards" Fix)
# This ensures Node 0 doesn't get ALL the heavy tasks. 
# It takes 1 heavy, then 1 light, etc.
NODE_TASKS_FILE="tasks_node_${JOB_ID}_${NODE_ID}.txt"
awk -v id="$NODE_ID" -v tot="$TOTAL_NODES" \
'((NR-1) % tot) == id { print $0 }' "$WEIGHTED_FILE" > "$NODE_TASKS_FILE"


TASKS_IN_THIS_NODE=$(wc -l < "$NODE_TASKS_FILE")

# 5. LOGGING (Cleaned up)
echo "----------------------------------------------------------------"
echo "Job Array ID: $NODE_ID / $((TOTAL_NODES - 1))"
echo "Strategy: Weighted Interleaving (NXR Balanced)"
echo "Tasks assigned to this node: $TASKS_IN_THIS_NODE / $TOTAL_TASKS"
echo "----------------------------------------------------------------"
echo "FULL LIST OF TASKS FOR THIS NODE:"
cat "$NODE_TASKS_FILE"
echo "----------------------------------------------------------------"

if [ "$TASKS_IN_THIS_NODE" -eq 0 ]; then
    echo "ERROR: No tasks assigned to Node $NODE_ID. Check TOTAL_NODES."
    exit 1
fi

# --- EXECUTION ---

if [ "$SERIAL_EXECUTION" -eq 1 ]; then
    echo "Starting SERIAL execution on Node $NODE_ID..."
    while IFS= read -r cmd; do
        eval "$cmd"  # Using eval to handle the 'tee' and redirects correctly
    done < "$NODE_TASKS_FILE"
    
else
    echo "Starting PARALLEL execution on Node $NODE_ID with $NUMBER_OF_JOBS threads..."
    # We use --halt now,1 to stop if a job fails
    parallel -j "$NUMBER_OF_JOBS" < "$NODE_TASKS_FILE"
fi

# --- CLEANUP ---
# Only delete the global files if this is the last node, 
# otherwise nodes might delete files while others are reading them.
rm "$NODE_TASKS_FILE"

if [ "$NODE_ID" -eq 0 ]; then ## Only Node 0 handles deleting the shared global lists
    sleep 15 
    rm -f "$RAW_FILE" "$WEIGHTED_FILE"
fi
echo "ALL JOBS FINISHED ON NODE $NODE_ID!!!"
