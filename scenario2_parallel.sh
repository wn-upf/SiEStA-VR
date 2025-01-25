#!/bin/bash

NUMBER_OF_JOBS=15
simTime=100
k_queue=10000
mean_length=12000.0
rate_bps_queue=1 ## does nothing theoretically
rate_bps_in=100 ## does nothing theoretically 

# start_bandwidth=10E6
# end_bandwidth=80E6
# step_bandwidth=10E6

alt_bandwidths=(10E6 20E6 30E6 40E6 50E6 60E6 70E6 80E6 90E6 100E6)
N_BG=(2 3 4)
IS_UL=(0 1)

distance=20.0


# Create an array of bandwidth values
temp_file=$(mktemp)
cargo build --release --example mm1k_sim

# Define the function to execute on Ctrl+C
handle_interrupt() {
    echo "Simulation interrupted."
    exit 1;
}

# Set up the trap for SIGINT (Ctrl+C)
trap handle_interrupt SIGINT
for is_ul in "${IS_UL[@]}"; do 
    for num_stas in "${N_BG[@]}"; do 
        # for bandwidth_STA in $(seq $start_bandwidth $step_bandwidth $end_bandwidth); do
        for bandwidth_STA in "${alt_bandwidths[@]}"; do
            echo "IS_UL = $is_ul, N_BG = $num_stas"
            echo ./target/release/examples/mm1k_sim $simTime $mean_length $k_queue $rate_bps_in $distance $bandwidth_STA $is_ul $num_stas>> "$temp_file"
            # echo cargo run --release --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_in $distance $bandwidth_STA $is_ul $num_stas>> "$temp_file"
        done
    done
done
parallel -j "$NUMBER_OF_JOBS" < "$temp_file"
rm "$temp_file"



echo "ALL SIMS FINISHED!!\n"
