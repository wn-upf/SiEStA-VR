#!/bin/bash

NUMBER_OF_JOBS=25
simTime=1000
k_queue=1000
mean_length=12000.0
rate_bps_queue=1 ## does nothing theoretically
rate_bps_in=100 ## does nothing theoretically 

start_bandwidth=2.5E6
end_bandwidth=40E6
step_bandwidth=2.5E6

distance=30.0

# Number of parallel jobs to run
num_jobs=10  # You can change this value to the desired number of parallel jobs

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


for bandwidth_STA in $(seq $start_bandwidth $step_bandwidth $end_bandwidth); do
    echo ./target/release/examples/mm1k_sim $simTime $mean_length $k_queue $rate_bps_in $distance $bandwidth_STA >> "$temp_file"
done

parallel -j "$NUMBER_OF_JOBS" < "$temp_file"
rm "$temp_file"



echo "ALL SIMS FINISHED!!\n"
