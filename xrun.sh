NUMBER_OF_JOBS=4
SERIAL_EXECUTION=0
N_XR=( 1)
initial_bitrate_mbps=(10.0 20.0 50.0 100.0 )
TEST_TYPE=("RANDOM")  # Can be "BW", "JI", "PL", or "STD" for different emulated tests (or none)
 ## "TEST_TYPE = "RANDOM" ## for random PL BW and JI effects spread over the whole simulation

simTime=25.0
k_queue=10000
mean_length=12000.0
rate_bps_src=20E6; 
# rate_bps_src=6.5E8 ## loads the queue
rate_bps_queue=6E5 ## does nothing theoretically 

distance=2.0
PL=0.1


video_samples=("garp4k" "snow" "assemble" "cut_video" "furbo" "randomVid")

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
                    
                        # Create the folder for results saving
                        name_folder=$(printf "sim_T%.0f_D%.0f_Br%.1f_PL%.03f_NXR%.0f_NBG%.0f_UL%.0f_%s_%s" \
                                    "$simTime" "$distance" "$bitrate" "$PL" "$nxr" "$nbg" "$is_ul" "$test" "$video_sample")
                        
                        
                        mkdir -p "Results/$name_folder"

                        if [ "$SERIAL_EXECUTION" -eq 0 ]; then  ## Parallel execution
                            echo "RUNNING SIM: $name_folder\n"
                            echo ./target/release/examples/XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $bitrate $PL $nxr $nbg $is_ul $test $video_sample>> "$temp_file"

                        else                                    ## Serial execution
                            script -c "cargo run --release --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $bitrate $PL $nxr $nbg $is_ul $test $video_sample" "out_log.ans"
                            sleep 1
                            # rm out_log.ans
                            rm -rf Video_Sink/*
                        
                        fi
                    done
                done
                # cargo run --release --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $initial_bitrate_mbps $PL $nxr $nbg
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

cargo run --release --example two_bitrates_tests



echo "XRUN FINALLY FINISHED!!!"
