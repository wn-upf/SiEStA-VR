# Define ranges for each variable
simTime=100
mean_length=12000.0
rate_bps_src=3E6        # Explore different source rates

# distance_values=(10.0 30.0)
# initial_bitrate_values=(20.0 100.0)

rate_bps_queue_values=(10000.0 100000.0 1000000.0)         # Explore different queue rates
distance_values=(2.5 10.0 25.0 30.0)         # Different distances
initial_bitrate_values=(5.0 10.0 50.0 75.0 100.0 150.0)  # Different initial bitrates
k_queue_values=(1000 10000)

# Trap exit signals to kill background jobs
trap 'kill $(jobs -p)' EXIT

# Loop through all combinations
  for rate_bps_queue in "${rate_bps_queue_values[@]}"; do
    for distance in "${distance_values[@]}"; do
      for initial_bitrate in "${initial_bitrate_values[@]}"; do
        for k_queue in "${k_queue_values[@]}"; do
          cargo run --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $initial_bitrate &
          # log_file="Results/log_rate_src_${rate_bps_src}_rate_queue_${rate_bps_queue}_distance_${distance}_bitrate_${initial_bitrate}.txt"
          # cargo run --example XR_sim $simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $initial_bitrate | tee "$log_file" &
        done
      done
    done
  done


# Wait for all instances to complete
wait
