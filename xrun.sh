
simTime=75.0
k_queue=10000
mean_length=12000.0
rate_bps_src=20E6; 
# rate_bps_src=6.5E8 ## loads the queue
rate_bps_queue=6E5 ## does nothing theoretically 

distance=10.0
PL=0.00

initial_bitrate_mbps=10

VMAF_ANALYSIS=0

N_BGs=(0 1 2)
IS_UL_BG=(0 1)
N_XR=(1 2 3 4 5)

# Define folder name based on the same logic in Rust
name_folder=$(printf "sim_T%.0f_Plen%.0f_K%d_Rq%.0f_D%.0f_Br%.0f_PL%.06f" \
    "$simTime" "$mean_length" "$k_queue" "$rate_bps_queue" "$distance" "$initial_bitrate_mbps" "$PL")

# Create the folder
mkdir -p "Results/$name_folder"

# cargo build --release --example XR_sim

# Set up a trap to catch SIGINT (Ctrl+C) and print the folder location
# trap 'echo -e "\nSimulation stopped. Output folder location: $Results/$name_folder"; exit' SIGINT

# rm out_log.ans
# cargo run --release --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $initial_bitrate_mbps $PL

N_BG_one_time=2
N_XR_one_time=3
IS_UL=0

script -q -c "cargo run --release --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $initial_bitrate_mbps $PL $N_XR_one_time $N_BG_one_time " out_log.ans
# $N_XR_one_time $IS_UL" out_log.ans




# samply record cargo run --release --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $initial_bitrate_mbps $PL 

# ./target/release/examples/XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $initial_bitrate_mbps $PL | tee "Results/$name_folder/out_log.ans" ## windows option dbg output

# code Results/$name_folder/out_log.ans

# trap 'echo -e "\nSimulation stopped. Killing all simulations..."; kill 0; exit' SIGINT
# # Example: Run multiple simulations in parallel
# for i in {1..10}; do
#   echo "Starting simulation $i..."
#   ./target/release/examples/XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $initial_bitrate_mbps $PL &
# done

# # Wait for all background processes to finish
# wait

echo "All simulations completed."

cd simu_decode_samples




if [ "$VMAF_ANALYSIS" -eq 1 ]; then

    echo "EXTRACTING VMAF"

    ffmpeg -y -hwaccel cuda -f concat -safe 0 -i <(for f in $(ls encoded_frame*.hevc | sort -V); do echo "file '$PWD/$f'"; done) -c:v hevc -preset fast -crf 23 output_video.mp4

    cp -r output_video.mp4 vmaf_comparison/output_video.mp4  

    cd vmaf_comparison

    offset_video=360.0
    # Extract duration of output_video.mp4
    duration=$(ffprobe -v error -select_streams v:0 -show_entries format=duration -of csv=p=0 output_video.mp4)

    ffmpeg -y -hwaccel cuda -ss $offset_video -i bbb_1080p60fps.mp4 -t $duration -c:v hevc_nvenc -an -b:v 20M output_sample.mp4

    ffmpeg -y -hwaccel cuda -i output_sample.mp4 -i output_video.mp4 \
        -filter_complex "[0:v][1:v]libvmaf=log_fmt=json:log_path=vmaf.json" \
        -filter_complex "[0:v][1:v]psnr=stats_file=psnr.log" \
        -filter_complex "[0:v][1:v]ssim=stats_file=ssim.log" \
        -f null -
fi


echo "ALL JOBS FINISHED!!!"