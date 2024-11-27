#!/bin/bash

simTime=1000
k_queue=1000
mean_length=12000.0
rate_bps_queue=1 ## does nothing theoretically

start_bandwidth=2.5E6
end_bandwidth=40E6
step_bandwidth=2.5E6

distance=30.0

# Number of parallel jobs to run
num_jobs=10  # You can change this value to the desired number of parallel jobs

# Compile the project once
cargo build --release

# Create an array of bandwidth values
bandwidth_values=()
for bandwidth_STA in $(seq $start_bandwidth $step_bandwidth $end_bandwidth); do
    bandwidth_values+=($bandwidth_STA)
done

# Export variables for parallel to access them
export simTime mean_length k_queue rate_bps_queue distance

# Run the executable in parallel using GNU Parallel
echo "${bandwidth_values[@]}" | tr ' ' '\n' | parallel -j $num_jobs \
  './target/release/examples/mm1k_sim $simTime $mean_length $k_queue {} $rate_bps_queue $distance'

echo "All simulations completed."
