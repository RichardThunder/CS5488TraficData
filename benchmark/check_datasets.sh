#!/bin/bash

# Check if all datasets exist in HDFS before running benchmarks

echo "========================================"
echo "Checking HDFS Datasets for Benchmarks"
echo "========================================"
echo ""

datasets=(
    "hdfs://traffic_data_partitioned/202508"
    "hdfs://traffic_data_partitioned/202507"
    "hdfs://traffic_data_partitioned/202505"
    "hdfs://traffic_data_partitioned/202504"
    "hdfs://traffic_data_partitioned/202503"
    "hdfs://traffic_data_partitioned/202502"
)

all_exist=true

for dataset in "${datasets[@]}"; do
    echo -n "Checking $dataset ... "
    if hdfs dfs -test -d "$dataset" 2>/dev/null; then
        size=$(hdfs dfs -du -s -h "$dataset" 2>/dev/null | awk '{print $1, $2}')
        echo "✓ EXISTS (Size: $size)"
    else
        echo "✗ NOT FOUND"
        all_exist=false
    fi
done

echo ""
echo "========================================"
if [ "$all_exist" = true ]; then
    echo "✓ All datasets are available!"
    echo "You can run the benchmark with:"
    echo "  spark-submit benchmark_congestion.py"
else
    echo "✗ Some datasets are missing!"
    echo "Please verify HDFS paths or update DATASETS in benchmark_congestion.py"
fi
echo "========================================"
