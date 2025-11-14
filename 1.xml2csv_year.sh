#!/bin/zsh
# This script runs the 1_xml_to_csv.py script to convert XML files to parquet format.
# It also handles the output directories and prints the locations of the saved files.

basedir=$(dirname "$0")
script="${basedir}/1_xml_to_csv.py"
if [ ! -f "$script" ]; then
    echo "Error: Script $script not found!"
    exit 1
fi

for dir in "$basedir"/dataset/*; do
    echo "Processing directory: $dir"
    full_input_path=$(readlink -f "$dir")
    spark-submit \
    --master "local[14]" \
    --driver-memory 4g \
    --executor-memory 16g \
    --executor-cores 4 \
    --num-executors 3 \
    --packages com.databricks:spark-xml_2.12:0.17.0 \
    "$script" file://"$full_input_path"
    echo "=== Finished: $dir ==="
done