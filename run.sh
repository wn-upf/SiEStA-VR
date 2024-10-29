#!/bin/bash


simTime=1E3
k_queue=100
mean_length=12000.0
rate_bps_src=8E4; 
# rate_bps_src=6.5E8 ## loads the queue
rate_bps_queue=6E5 ## does nothing theoretically 


distance=10.0


rm out_log.ans
script -q -c "cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance" out_log.ans

# cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance


# cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance # for no DBG output file


# trace with samply:
# samply record cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance # for Tracing syscalls
