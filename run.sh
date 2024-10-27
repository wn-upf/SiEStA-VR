#!/bin/bash


simTime=1000.0
mean_length=1000.0
k_queue=100
rate_bps_src=2000.0
rate_bps_queue=20000.0


rm out_log.ans

# script -q -c "cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue" out_log.ans

cargo run --example mm1k_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue # for no DBG output file
