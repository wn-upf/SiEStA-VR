NUMBER_OF_JOBS=2
SERIAL_EXECUTION=1
N_XR=(3)
initial_bitrate_mbps=(100.0)
TEST_TYPE=("STD")

# Can be "BW", "JI", "PL", or "STD" for different emulated tests (or none)
 ## "TEST_TYPE = "RANDOM" ## for random PL BW and JI effects spread over the whole simulation

simTime=50.0
k_queue=10000
mean_length=12000.0
rate_bps_src=20E6; 
# rate_bps_src=6.5E8 ## loads the queue
rate_bps_queue=6E5 ## does nothing theoretically 

distance=15.0
PL=0.1
fps_list=(60.0 90.0)

num_close_users=(2)
distance_close_users=(15.0)

RANDOM_SEEDS=(1 2 3 4 5)

# video_samples=("garp4k" "snow" "assemble" "cut_video" "furbo" "randomVid")
video_samples=("swordsmith")

# VMAF_ANALYSIS=0

N_BGs=(0)
IS_UL_BG=(0)

# Define the function to execute on Ctrl+C
handle_interrupt() {
    echo "Simulation interrupted."
    exit 1;
}

# Set up the trap for SIGINT (Ctrl+C)
trap handle_interrupt SIGINT
rm -rf Video_Sink/*

temp_file=$(mktemp)

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
                                    # Create the folder for results saving
                                        name_folder=$(printf "sim_T%.0f_D%.0f_Br%.1f_PL%.03f_NXR%.0f_NBG%.0f_UL%.0f_%s_%s_FPS%.0f_Nclose%d_dclose%.1f_S%.0f" \
                                                    "$simTime" "$distance" "$bitrate" "$PL" "$nxr" "$nbg" "$is_ul" "$test" "$video_sample" "$FPS" "$close_users" "$close_distance" "$seed")
                                        
                                        
                                        mkdir -p "Results/$name_folder"

                                        if [ "$SERIAL_EXECUTION" -eq 0 ]; then  ## Parallel execution
                                            echo "RUNNING SIM: $name_folder\n"
                                            echo           ./target/release/examples/XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $bitrate $PL $nxr $nbg $is_ul $test $video_sample $FPS $close_users $close_distance $seed>> "$temp_file"

                                        else                                    ## Serial execution
                                            script -c "cargo run --release --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $bitrate $PL $nxr $nbg $is_ul $test $video_sample $FPS $close_users $close_distance $seed" "out_log.ans"
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
# After writing to temp file
echo "Contents of temp file:"
cat "$temp_file"

shuf "$temp_file" | parallel -j "$NUMBER_OF_JOBS"
# parallel -j "$NUMBER_OF_JOBS" < "$temp_file"
rm "$temp_file"

echo "ALL JOBS FINISHED!!!"

# cargo run --release --example two_bitrates_tests



echo "XRUN FINALLY FINISHED!!!"
