#!/bin/bash
# Run Descriptive Statistics Analysis on Full Year Traffic Data

echo "Starting Descriptive Statistics Analysis..."
echo "Data source: traffic_data_partitioned"
echo "Output: traffic_statistics/"
echo ""

# Run the PySpark script
# Using multiple executors and cores to maximize cluster utilization
spark-submit \
    --master local[*] \
    --driver-memory 8g \
    --executor-memory 8g \
    descriptive_statistics.py

echo ""
echo "Analysis complete! Check traffic_statistics/ for results."
