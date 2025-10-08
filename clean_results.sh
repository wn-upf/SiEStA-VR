#!/bin/bash

RESULTS_DIR="/path/to/Results"
INTERVAL_MINUTES=4  # how often to clean (every 30 minutes)

while true; do
    echo "[$(date)] Cleaning up $RESULTS_DIR ..."
    rm -rf "${RESULTS_DIR:?}/"*   # delete all contents safely
    echo "[$(date)] Cleanup done. Next cleanup in $INTERVAL_MINUTES minutes."
    sleep $((INTERVAL_MINUTES * 60))
done
