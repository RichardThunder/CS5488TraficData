#!/bin/zsh
# This script runs the xml_to_csv_memory_efficient.py script to convert XML files to CSV format.
# It also handles the output directories and prints the locations of the saved files.

basedir=$(dirname "$0")
script="${basedir}/xml_to_csv_memory_efficient.py"
if [ ! -f "$script" ]; then
    echo "Error: Script $script not found!"
    exit 1
fi

output_dir="traffic_data_partitioned"
for dir in "$basedir"/dataset/*; do
    echo "Processing directory: $dir"
    python3 "$script" "$dir" "$output_dir"
done