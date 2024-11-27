#!/bin/bash


simTime=1000
k_queue=1000
mean_length=12000.0
# rate_bps_src=3E6; 
# rate_bps_src=6.5E8 ## loads the queue
rate_bps_queue=1 ## does nothing theoretically 



start_bandwidth=2.5E6
end_bandwidth=40E6
step_bandwidth=2.5E6


distance=30.0


for bandwidth_STA in $(seq $start_bandwidth $step_bandwidth $end_bandwidth); do
        echo -e "\n\n********************************** RUST results for bandwidth_STA = $bandwidth_STA **********************************\n"

        cargo run --release --example  mm1k_sim $simTime $mean_length $k_queue $bandwidth_STA $rate_bps_queue $distance

        # Create a directory named after the current bandwidth_STA value with reduced decimals
        folder_name=$(echo "$bandwidth_STA" | awk '{printf "%.1fMbps\n", $1/1E6}')
        

done 



# script -q -c "cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance" out_log.ans

# cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance


# cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance # for no DBG output file


# trace with samply:
# samply record cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance # for Tracing syscalls
