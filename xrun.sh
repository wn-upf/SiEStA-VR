
simTime=20
k_queue=10000
mean_length=12000.0
rate_bps_src=3E6; 
# rate_bps_src=6.5E8 ## loads the queue
rate_bps_queue=6E5 ## does nothing theoretically 

distance=10.0
PL=0.001

initial_bitrate_mbps=50.0

# Define folder name based on the same logic in Rust
name_folder=$(printf "sim_T%.0f_Plen%.0f_K%d_Rq%.0f_D%.0f_Br%.0f_PL%.06f" \
    "$simTime" "$mean_length" "$k_queue" "$rate_bps_queue" "$distance" "$initial_bitrate_mbps" "$PL")

# Create the folder
mkdir -p "Results/$name_folder"

cargo build --release --example XR_sim

# Set up a trap to catch SIGINT (Ctrl+C) and print the folder location
trap 'echo -e "\nSimulation stopped. Output folder location: $Results/$name_folder"; exit' SIGINT


# rm out_log.ans
# script -q -c "cargo run --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance" out_log.ans
./target/release/examples/XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $initial_bitrate_mbps $PL | tee "Results/$name_folder/out_log.ans"

# code Results/$name_folder/out_log.ans
# samply record cargo run --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance # for Tracing syscalls

