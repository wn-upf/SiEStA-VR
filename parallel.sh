BATCH_SIZE=1000 # Set to the desired level of parallelism

cargo build --release --example XR_sim

# Define ranges for each variable
simTime=100
mean_length=12000.0
rate_bps_src=3E6

rate_bps_queue_values=(10000.0 100000.0 1000000.0)
distance_values=(1.0 5.0 7.5 10.0 12.5 15.0 17.5 25.0 27.5 30.0 40.0)
initial_bitrate_values=(10.0 20.0 30.0 40.0 50.0 60.0 70.0 80.0 90.0 100.0 150.0 200.0)
k_queue_values=(1000 10000)

PL_probs=(0.0 0.1 0.05 0.02 0.01 0.001)

# Calculate total iterations
num_rate_bps_queue=${#rate_bps_queue_values[@]}
num_distance=${#distance_values[@]}
num_initial_bitrate=${#initial_bitrate_values[@]}
num_k_queue=${#k_queue_values[@]}
num_pls=${#PL_probs[@]}

total_iterations=$((num_rate_bps_queue * num_distance * num_initial_bitrate * num_k_queue * num_pls))



# # Define folder name based on the same logic in Rust
# name_folder=$(printf "sim_T%.0f_Plen%.0f_K%d_Rq%.0f_D%.0f_Br%.0f" \
#     "$simTime" "$mean_length" "$k_queue" "$rate_bps_queue" "$distance" "$initial_bitrate_mbps")

# # Create the folder
# echo "Creating Results/$name_folder"
# mkdir -p "Results/$name_folder"



# Define the batch size for parallel jobs


echo "Total number of iterations: $total_iterations"
echo "Batch size: $BATCH_SIZE"
sleep 5


# Trap exit signals to kill background jobs
trap 'kill $(jobs -p)' EXIT

#  ---------------- version without log output --------------------
for rate_bps_queue in "${rate_bps_queue_values[@]}"; do
  for distance in "${distance_values[@]}"; do
    for initial_bitrate in "${initial_bitrate_values[@]}"; do
      for k_queue in "${k_queue_values[@]}"; do
        for PL in "${PL_probs[@]}"; do
          echo "$simTime $mean_length $k_queue $rate_bps_src $rate_bps_queue $distance $initial_bitrate"
        done
      done
    done
  done
done | xargs -n 7 -P "$BATCH_SIZE" bash -c './target/release/examples/XR_sim "$@"' _
