NUMBER_OF_JOBS=5
SERIAL_EXECUTION=0



simTime=70.0
k_queue=10000
mean_length=12000.0
rate_bps_src=20E6; 
# rate_bps_src=6.5E8 ## loads the queue
rate_bps_queue=6E5 ## does nothing theoretically 

distance=2.0
PL=0.1


# VMAF_ANALYSIS=0

N_BGs=(0)
IS_UL_BG=(0)
# N_XR=(1 2 3 4 5)
# N_XR=(1 2 3 4 5)
# initial_bitrate_mbps=(0.1 1 2 3 5 10 20 40 60 80 100)


N_XR=(1)
initial_bitrate_mbps=(100.0)

TEST_TYPE=("BW" "JI" "PL")  # Can be "BW", "JI", "PL", or "STD" for different emulated tests (or none)

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
                    
                    # Create the folder for results saving
                    name_folder=$(printf "sim_T%.0f_D%.0f_Br%.1f_PL%.03f_NXR%.0f_NBG%.0f_UL%.0f_%s" \
                                "$simTime" "$distance" "$bitrate" "$PL" "$nxr" "$nbg" "$is_ul" "$test")
                    
                    
                    mkdir -p "Results/$name_folder"

                    if [ "$SERIAL_EXECUTION" -eq 0 ]; then  ## Parallel execution
                        echo "RUNNING SIM: $name_folder\n"
                        echo ./target/release/examples/XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $bitrate $PL $nxr $nbg $is_ul $test >> "$temp_file"

                    else                                    ## Serial execution
                        script -c "cargo run --release --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $bitrate $PL $nxr $nbg $is_ul $test" "out_log.ans"
                        sleep 1
                        # rm out_log.ans
                        rm -rf Video_Sink/*
                    
                    fi
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


# echo "All simulations completed."
# if [ "$VMAF_ANALYSIS" -eq 1 ]; then

#     cd simu_decode_samples
#     echo "EXTRACTING VMAF"

#     ffmpeg -y -hwaccel cuda -f concat -safe 0 -i <(for f in $(ls encoded_frame*.hevc | sort -V); do echo "file '$PWD/$f'"; done) -c:v hevc -preset fast -crf 23 output_video.mp4

#     cp -r output_video.mp4 vmaf_comparison/output_video.mp4  

#     cd vmaf_comparison

#     offset_video=360.0
#     # Extract duration of output_video.mp4
#     duration=$(ffprobe -v error -select_streams v:0 -show_entries format=duration -of csv=p=0 output_video.mp4)

#     ffmpeg -y -hwaccel cuda -ss $offset_video -i bbb_1080p60fps.mp4 -t $duration -c:v hevc_nvenc -an -b:v 20M output_sample.mp4

#     ffmpeg -y -hwaccel cuda -i output_sample.mp4 -i output_video.mp4 \
#         -filter_complex "[0:v][1:v]libvmaf=log_fmt=json:log_path=vmaf.json" \
#         -filter_complex "[0:v][1:v]psnr=stats_file=psnr.log" \
#         -filter_complex "[0:v][1:v]ssim=stats_file=ssim.log" \
#         -f null -
# fi


echo "ALL JOBS FINISHED!!!"