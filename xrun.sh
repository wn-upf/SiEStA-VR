
simTime=20
k_queue=10000
mean_length=12000.0
rate_bps_src=3E6; 
# rate_bps_src=6.5E8 ## loads the queue
rate_bps_queue=6E5 ## does nothing theoretically 


distance=10.0


rm out_log.ans
script -q -c "cargo run --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance" out_log.ans
