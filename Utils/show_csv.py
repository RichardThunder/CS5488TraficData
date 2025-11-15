import pandas as pd
import os

# --- Configuration ---

# 1. Path to the local Parquet directory you downloaded
# (This is the parent directory containing the part-files)
local_parquet_path = "/home/richard/project/BDA/pyspark/benchmark/timing"

# 2. Path for your final, single CSV file
local_csv_output_file = "/home/richard/project/BDA/pyspark/benchmark/all_test.csv"

# --- Conversion ---

try:
    print(f"Reading Parquet dataset from: {local_parquet_path}")
    
    # Pandas can read a directory of Parquet files as one DataFrame
    df = pd.read_parquet(local_parquet_path)

    print(f"Successfully read {len(df)} rows.")

    # Save the entire DataFrame to a single CSV file
    # index=False prevents writing the pandas row index as a column
    df.to_csv(local_csv_output_file, index=False)

    print(f"Successfully converted to CSV at: {local_csv_output_file}")

except Exception as e:
    print(f"An error occurred: {e}")