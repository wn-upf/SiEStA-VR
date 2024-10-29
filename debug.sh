#!/bin/bash

script -q -c "cargo run --example works_fast" out_log.ans

# cargo run --example works_fast 




# trace with samply:
# samply record cargo run --example works_fast $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance # for Tracing syscalls
# samply record cargo run --example works_slow_ampdu $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance # for Tracing syscalls



# # Define the timing log file
# TIMING_LOG="timing_log.txt"

# # Function to run command with real-time feedback and log its time
# run_and_time() {
#   local cmd="$1"
#   local description="$2"
  
#   # Display a brief message in the terminal
#   echo "$description"
  
#   # Log the description in the timing log
#   echo "$description" >> "$TIMING_LOG"
  
#   # Run the command with time measurement, redirecting only the timing output to the log file
#   { time eval "$cmd"; } 2>> "$TIMING_LOG"
  
#   echo "------------------------------------------------" >> "$TIMING_LOG"
# }

# # Run each command with timing and description
# run_and_time "cargo run --example works_fast" "Starting works_fast example..."
# run_and_time "cargo run --example works_slow_ampdu" "Starting ampdu_version example..."
# run_and_time "cd /home/boris/Desktop/cost_MG1/MG1_COST && ./run.sh" "Running MG1_COST script..."

# echo "starting fast example"
# time cargo run --example works_fast

# echo "Starting ampdu_version example"
# time cargo run --example ampdu_version



# echo "Running MG1_COST script"
# cd /home/boris/Desktop/cost_MG1/MG1_COST
# time ./run.sh
