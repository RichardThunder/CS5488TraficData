# Benchmark Enhancements Summary

## Overview
Enhanced the benchmark.py script with detailed timing breakdowns, Pandas serial computation comparison, and CSV export functionality.

## Key Enhancements

### 1. Detailed Timing Tracking
Each benchmark now records granular timing information for:

#### PySpark Phases:
- Data Read
- Cache & Count
- Morning Analysis (6:00-10:00)
- Evening Analysis (17:00-21:00)
- Unpersist
- Total Execution

#### Hive Phases:
- Create View
- Morning Query (6:00-10:00)
- Evening Query (17:00-21:00)
- Total Execution

#### Pandas Phases:
- Read Parquet (via Spark)
- Convert to Pandas
- Preprocess (extract hour)
- Morning Analysis (6:00-10:00)
- Evening Analysis (17:00-21:00)
- Total Execution

#### Setup Phase:
- SparkSession Initialization
- Total Program Execution

### 2. Pandas Serial Computation Benchmark
Added `benchmark_pandas()` function that:
- Reads data via Spark then converts to Pandas DataFrame
- Performs serial (single-threaded) computation
- Uses native Pandas groupby and aggregation operations
- Provides baseline for comparing parallel vs serial performance

### 3. CSV Export Functionality
New `export_timing_to_csv()` function that:
- Creates `benchmark_logs/` directory automatically
- Exports all timing data to `benchmark_timing_results.csv`
- CSV includes columns: timestamp, tool, phase, duration_seconds, details
- Ready for visualization in Excel, Python (matplotlib/seaborn), or other tools

### 4. Enhanced Summary Report
The benchmark summary now shows:
- Execution time for all three tools (PySpark, Hive, Pandas)
- Identifies the fastest tool
- Calculates speedup ratios (e.g., "PySpark is 2.5x faster than Pandas")

## Usage

Run the benchmark as before:
```bash
spark-submit benchmark.py
```

## Output

The script will generate:
1. **Console Output**: Real-time logging with detailed phase timing
2. **CSV File**: `benchmark_logs/benchmark_timing_results.csv` with all timing data

### Sample CSV Structure:
```csv
timestamp,tool,phase,duration_seconds,details
2025-11-14T10:30:45.123,Setup,SparkSession Initialization,2.156,
2025-11-14T10:30:47.279,PySpark,Data Read,5.432,from hdfs:///traffic_data_partitioned/202508
2025-11-14T10:30:52.711,PySpark,Cache & Count,8.234,1,234,567 records
2025-11-14T10:31:00.945,PySpark,Morning Analysis,3.567,6:00-10:00
...
```

## Visualization Recommendations

The CSV file can be used to create:
1. **Bar charts**: Compare total execution time across tools
2. **Stacked bar charts**: Show phase breakdown for each tool
3. **Speedup charts**: Visualize parallel vs serial performance gains
4. **Timeline charts**: Show execution phases over time

### Example Python Visualization Code:
```python
import pandas as pd
import matplotlib.pyplot as plt

# Read timing data
df = pd.read_csv('benchmark_logs/benchmark_timing_results.csv')

# Filter total execution times
totals = df[df['phase'] == 'Total Execution']

# Create bar chart
plt.figure(figsize=(10, 6))
plt.bar(totals['tool'], totals['duration_seconds'])
plt.xlabel('Tool')
plt.ylabel('Execution Time (seconds)')
plt.title('Benchmark Performance Comparison')
plt.savefig('benchmark_comparison.png')
```

## Performance Insights

The enhanced benchmark helps answer:
- **Where is time spent?**: Identify bottlenecks in each tool
- **Read vs Compute**: Distinguish I/O time from computation time
- **Caching impact**: See the cost and benefit of data caching
- **Parallel speedup**: Compare PySpark/Hive against serial Pandas

## Notes

- Pandas benchmark includes Spark read + conversion time for fair comparison
- All tools use the same data source: `hdfs:///traffic_data_partitioned/202508`
- Timing uses Python's `time.time()` for wall-clock measurements
- CSV file is overwritten on each run (consider timestamped filenames for multiple runs)
