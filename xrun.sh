NUMBER_OF_JOBS=3
SERIAL_EXECUTION=1
initial_bitrate_mbps=( 100.0 )
# initial_bitrate_mbps=( 100.0 )

TEST_TYPE=("BW") # Can be "BW", "JI", "PL", "RANDOM", or "STD" for different emulated tests (or none)
simTime=80.0
k_queue=10000
mean_length=12000.0  ## TODO: DELETE THESE
rate_bps_src=20E6;   ## TODO: DELETE THESE
rate_bps_queue=6E5 ## does nothing theoretically 

distance_list=( 1.5 11.0 )
distance_close_users=(1.5 4.0 7.0 9.0 11.0 )  ## to have heterogeneous distances
num_close_users=(0 1 2)     ## number of users with alternate distance

PL=0.1
fps_list=(90.0)
N_XR=(1)
# distance_close_users=(1.0 1.8 2.0 4.0 5.0 7.0 8.0 11.0)

ABR_ENABLED=( 1 )
nest_profiles=(0 1 2) ## Nest-VR profiles: 

RANDOM_SEEDS=(1)
# video_samples=("garp4k" "snow" "assemble" "cut_video" "furbo" "randomVid")
video_samples=("snow")
N_BGs=(0)
IS_UL_BG=(0)

intrarefresh_choice=(1)
GoP_sizes=(90)
# Define the function to execute on Ctrl+C
handle_interrupt() {
    echo "Simulation interrupted."
    exit 1;
}

# Set up the trap for SIGINT (Ctrl+C)
trap handle_interrupt SIGINT
rm -rf Video_Sink/*

temp_file=$(mktemp)
SIM_COUNT=0                 # counter of simulations, not an input arg

cargo build --release --example XR_sim

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
                                                                echo "RUNNING SIM: $name_folder\n"
                                                                echo           ./target/release/examples/XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $bitrate $PL $nxr $nbg $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile >> "$temp_file"

                                                            else                                    ## Serial execution
                                                                script -c "cargo run --release --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $bitrate $PL $nxr $nbg $is_ul $test $video_sample $FPS $close_users $close_distance $seed $gop $intrarefresh $ABR $nest_profile" "out_log.ans"
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
