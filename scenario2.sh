#!/bin/bash


simTime=100
k_queue=10000
mean_length=12000.0
# rate_bps_src=3E6; 
# rate_bps_src=6.5E8 ## loads the queue
rate_bps_in=1E2 ## does nothing theoretically, to be deleted if MM1K stats not used. 

handle_interrupt() {
    echo "Simulation interrupted."
    exit 1;
}

# Set up the trap for SIGINT (Ctrl+C)
trap handle_interrupt SIGINT


start_bandwidth=10E6
end_bandwidth=65E6
step_bandwidth=2.5E6


distance=10.0

is_ul=1
n_bg=5

cargo build --release --example mm1k_sim
for bandwidth_STA in $(seq $start_bandwidth $step_bandwidth $end_bandwidth); do
        echo -e "\n\n********************************** RUST results for bandwidth_STA = $bandwidth_STA **********************************\n"

        script -c "./target/release/examples/mm1k_sim $simTime $mean_length $k_queue $rate_bps_in $distance $bandwidth_STA $is_ul $n_bg $distance" "out_log.ans"

        # Create a directory named after the current bandwidth_STA value with reduced decimals
        folder_name=$(echo "$bandwidth_STA" | awk '{printf "%.1fMbps\n", $1/1E6}')
        sleep 1

done 

