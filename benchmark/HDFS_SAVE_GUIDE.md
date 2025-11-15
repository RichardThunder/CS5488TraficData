# Saving Timing Results to HDFS

The benchmark framework now supports saving timing results directly to HDFS in both Parquet and CSV formats.

## Features

### 1. Save Individual Test Results to HDFS

Each `BaseAnalysisTest` instance has two methods to save its timing results to HDFS:

```python
# Save as Parquet (recommended for large datasets)
test.save_timing_results_to_hdfs("hdfs:///benchmark_results/my_test")

# Save as CSV (better for human readability)
test.save_timing_results_to_csv_hdfs("hdfs:///benchmark_results/my_test.csv")
```

### 2. Automatic Saving in `run_benchmarks.py`

The harness script can automatically save all timing results to HDFS. This is configured via constants at the top of the file.

## Configuration

Edit `run_benchmarks.py`:

```python
# HDFS output paths
HDFS_TIMING_OUTPUT = "hdfs:///benchmark_results/timing"  # Parquet format
HDFS_TIMING_CSV_OUTPUT = "hdfs:///benchmark_results/timing_csv"  # CSV format

# Enable/disable HDFS saving
SAVE_TO_HDFS = True  # Set to False to disable
```

## Usage Examples

### Example 1: Save Single Test Results

```python
from pyspark.sql import SparkSession
from benchmark.concrete_tests import SparkBusyRoadTest

# Initialize Spark
spark = SparkSession.builder.appName("Test").getOrCreate()

# Create and run test
test = SparkBusyRoadTest(
    name="PySpark-BusyRoad",
    data_paths=['hdfs:///traffic_data_partitioned/202508'],
    spark=spark
)

result = test.run()

# Save results to HDFS
if result['success']:
    # Save as Parquet
    test.save_timing_results_to_hdfs("hdfs:///my_results/pyspark_test")

    # Or save as CSV
    test.save_timing_results_to_csv_hdfs("hdfs:///my_results/pyspark_test_csv")

spark.stop()
```

### Example 2: Save with Different Modes

```python
# Overwrite existing data
test.save_timing_results_to_hdfs("hdfs:///results/test1", mode="overwrite")

# Append to existing data
test.save_timing_results_to_hdfs("hdfs:///results/test1", mode="append")

# Error if path already exists
test.save_timing_results_to_hdfs("hdfs:///results/test1", mode="error")
```

### Example 3: Run Harness with HDFS Saving

```bash
# Make sure SAVE_TO_HDFS = True in run_benchmarks.py
spark-submit --master yarn benchmark/run_benchmarks.py
```

This will:
1. Run all configured tests
2. Save results to local CSV: `benchmark_logs/benchmark_timing_results.csv`
3. Save results to HDFS Parquet: `hdfs:///benchmark_results/timing`
4. Save results to HDFS CSV: `hdfs:///benchmark_results/timing_csv`

## Reading Results from HDFS

### Read Parquet Results

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadResults").getOrCreate()

# Read timing results
df = spark.read.parquet("hdfs:///benchmark_results/timing")

# Show all results
df.show(truncate=False)

# Filter by tool
df.filter(df.tool == "PySpark-BusyRoad").show()

# Get total execution times
df.filter(df.phase == "Total Execution").select("tool", "duration_seconds").show()

# Calculate average execution time by tool
df.filter(df.phase == "Total Execution") \
  .groupBy("tool") \
  .avg("duration_seconds") \
  .show()
```

### Read CSV Results

```python
# Read CSV results
df_csv = spark.read.option("header", "true").csv("hdfs:///benchmark_results/timing_csv")

# Note: CSV columns are strings by default, cast if needed
from pyspark.sql.functions import col
df_csv = df_csv.withColumn("duration_seconds", col("duration_seconds").cast("double"))

df_csv.show()
```

### Download Results from HDFS

```bash
# Download Parquet results
hdfs dfs -get /benchmark_results/timing ./local_results/

# Download CSV results
hdfs dfs -get /benchmark_results/timing_csv ./local_results_csv/

# View CSV directly
hdfs dfs -cat /benchmark_results/timing_csv/part-*.csv | head -20
```

## Result Schema

Both Parquet and CSV results have the same schema:

| Column | Type | Description |
|--------|------|-------------|
| timestamp | string | ISO timestamp when the phase completed |
| total_records | long | Total number of records in the dataset |
| tool | string | Name of the test (e.g., "PySpark-BusyRoad") |
| phase | string | Phase name (e.g., "Read Data", "Execute Analysis") |
| duration_seconds | double | Duration of the phase in seconds |
| records_processed | long | Number of records processed in this phase |
| details | string | Additional details about the phase |

## Analysis Queries

### Compare Tools Performance

```python
# Compare total execution times
spark.read.parquet("hdfs:///benchmark_results/timing") \
    .filter(col("phase") == "Total Execution") \
    .groupBy("tool") \
    .avg("duration_seconds") \
    .orderBy("avg(duration_seconds)") \
    .show()
```

### Phase Breakdown Analysis

```python
# Analyze time spent in each phase
df = spark.read.parquet("hdfs:///benchmark_results/timing")

df.filter(col("phase") != "Total Execution") \
  .groupBy("tool", "phase") \
  .avg("duration_seconds") \
  .orderBy("tool", "phase") \
  .show(truncate=False)
```

### Historical Comparison

```python
# If you've been saving results over time with timestamps
df = spark.read.parquet("hdfs:///benchmark_results/timing")

# Get results from last 7 days
from pyspark.sql.functions import col, to_date, current_date, date_sub

df.filter(
    (col("phase") == "Total Execution") &
    (to_date(col("timestamp")) >= date_sub(current_date(), 7))
) \
.select("timestamp", "tool", "duration_seconds") \
.orderBy("timestamp") \
.show(100)
```

## Best Practices

1. **Use Parquet for Large Datasets**: Parquet is columnar and compressed, better for analytics
2. **Use CSV for Sharing**: CSV is more portable and easier to import into other tools
3. **Append Mode for History**: Use `mode="append"` to build historical data
4. **Partition by Date**: For long-term storage, consider partitioning by date:
   ```python
   df.write.partitionBy("date").parquet("hdfs:///results/by_date/")
   ```
5. **Clean Old Results**: Periodically clean up old benchmark results to save space

## Troubleshooting

### Permission Denied

```
PermissionDeniedException: Permission denied: user=..., access=WRITE
```

**Solution**: Ensure you have write permissions to the HDFS path:
```bash
hdfs dfs -chmod -R 777 /benchmark_results
# Or create the directory with proper permissions
hdfs dfs -mkdir -p /benchmark_results
hdfs dfs -chown $USER /benchmark_results
```

### Path Already Exists

```
AnalysisException: path hdfs:///benchmark_results/timing already exists
```

**Solution**: Use `mode="overwrite"` or delete the existing path:
```bash
hdfs dfs -rm -r /benchmark_results/timing
```

### HDFS Not Accessible

```
IOError: Failed to save timing results to HDFS
```

**Solution**:
1. Check HDFS is running: `hdfs dfsadmin -report`
2. Verify path format: Must start with `hdfs://` or `hdfs:///`
3. Test connectivity: `hdfs dfs -ls /`

## Integration with Analysis Tools

### Export to pandas for Local Analysis

```python
# Read from HDFS and convert to pandas
df_spark = spark.read.parquet("hdfs:///benchmark_results/timing")
df_pandas = df_spark.toPandas()

# Use pandas for analysis
import pandas as pd
import matplotlib.pyplot as plt

# Plot execution times
pivot = df_pandas[df_pandas['phase'] == 'Total Execution'].pivot(
    columns='tool',
    values='duration_seconds'
)
pivot.plot(kind='bar')
plt.show()
```

### Export to Excel/CSV for Reporting

```python
# Download and convert to Excel
df_spark = spark.read.parquet("hdfs:///benchmark_results/timing")
df_pandas = df_spark.toPandas()

# Save to Excel
df_pandas.to_excel("benchmark_report.xlsx", index=False)

# Or CSV
df_pandas.to_csv("benchmark_report.csv", index=False)
```

## Summary

The HDFS save functionality provides:
- ✅ Automatic saving of timing results to HDFS
- ✅ Support for both Parquet (efficient) and CSV (readable) formats
- ✅ Flexible save modes (append, overwrite, error)
- ✅ Easy integration with Spark SQL for analysis
- ✅ Historical tracking of benchmark performance
