#!/bin/bash

RESULTS_DIR="/home/boris/Desktop/Rust_MG1/asynchronix/Results"
INTERVAL_MINUTES=1  # how often to clean (every 30 minutes)

while true; do
    echo "[$(date)] Cleaning up $RESULTS_DIR ..."
    rm -rf "${RESULTS_DIR:?}/"*   # delete all contents safely
    echo "[$(date)] Cleanup done. Next cleanup in $INTERVAL_MINUTES minutes."
    sleep $((INTERVAL_MINUTES * 60))
done
