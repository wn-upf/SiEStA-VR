#!/bin/bash


simTime=100

k_queue=100
mean_length=1000.0


rate_bps_src=2000.0
rate_bps_queue=20000.0


distance=1.0


# rm out_log.ans

# script -q -c "cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance" out_log.ans

cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance # for no DBG output file


# trace with samply:
# samply record cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance # for Tracing syscalls
