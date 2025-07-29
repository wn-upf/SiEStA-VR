NUMBER_OF_JOBS=4
SERIAL_EXECUTION=1

# initial_bitrate_mbps=( 100.0 )

TEST_TYPE=("STD") # Can be "BW", "JI", "PL", "RANDOM", or "STD" for different emulated tests (or none)

simTime=200.0
k_queue=10000
mean_length_BG=12000.0     ## BG traffic length 
rate_bps_src_BG=20E6;   ## BG traffic arrival rate

distance_list=( 1.5 )
distance_close_users=( 1.5 )  ## to have heterogeneous distances
num_close_users=( 0 )     ## number of users with alternate distance
N_XR=( 1 ) 
PL=0.1

fps_list=( 60.0 90.0 )
initial_bitrate_mbps=( 100.0 )

# ABR_ENABLED=( 0 1 2 )  ## 0 => CBR , 1 => Nest-VR, 2 => Everest

ABR_ENABLED=( 2 )
nest_profiles=( 1 )

# nest_profiles=( 0 1 2 ) ## Nest-VR profiles:       0 => {NestVrProfile::Speedy},
#                                                  1 => {NestVrProfile::Balanced},
#                                                  2 => {NestVrProfile::Anxious},
RANDOM_SEEDS=( 1 )
# RANDOM_SEEDS=( 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 )
# video_samples=("garp4k" "snow" "assemble" "cut_video" "furbo" "randomVid")
video_samples=("snow")
N_BGs=(0)
IS_UL_BG=(0)

intrarefresh_choice=(0 1 )
GoP_sizes=(90)
# Define the function to execute on Ctrl+C
handle_interrupt() {
    echo "Simulation interrupted."
    exit 1;
}

# Set up the trap for SIGINT (Ctrl+C)
trap handle_interrupt SIGINT


temp_file=$(mktemp)
SIM_COUNT=0                 # counter of simulations, not an input arg

cargo build --release --example XR_sim
sleep 2
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
                                                            name_folder=$(printf "sim_T%.0f_D%.0f_Br%.1f_PL%.03f_NXR%.0f_NBG%.0f_UL%.0f_%s_%s_FPS%.0f_Nclose%d_dclose%.1f_S%.0f_GoP%.0f_IR%.0f_ABR%.0f_nest%.0f" \
                                                                        "$simTime" "$distance" "$bitrate" "$PL" "$nxr" "$nbg" "$is_ul" "$test" "$video_sample" "$FPS" "$close_users" "$close_distance" "$seed" "$gop" "$intrarefresh" "$ABR" "$nest_profile")
                                                    
                                                            (( SIM_COUNT++ ))  # ← increment
                                                            mkdir -p "Results/$name_folder"

                                                            if [ "$SERIAL_EXECUTION" -eq 0 ]; then  ## Parallel execution
                                                                echo "RUNNING SIM: $name_folder"
                                                                echo           ./target/release/examples/XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_bps_src_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile >> "$temp_file"

                                                            else                                    ## Serial execution
                                                                script -c "cargo run --release --example XR_sim $simTime $mean_length_BG $k_queue $distance $bitrate $PL $nxr $nbg $rate_bps_src_BG $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile" "out_log.ans"
                                                                sleep 1
                                                                # rm out_log.ans
                                                                rm -rf Video_Sink/*
                                                            
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


# After writing to temp file
echo "Contents of temp file:"
cat "$temp_file"
echo " --- Number of simulations: $SIM_COUNT --- \n"

shuf "$temp_file" | parallel -j "$NUMBER_OF_JOBS"
# parallel -j "$NUMBER_OF_JOBS" < "$temp_file"
rm "$temp_file"

echo "ALL JOBS FINISHED!!!"

# cargo run --release --example two_bitrates_tests

echo "XRUN FINALLY FINISHED!!!"
