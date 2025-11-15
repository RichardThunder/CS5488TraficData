# Multi-Size Dataset Benchmark

## Overview

Enhanced benchmark suite that tests PySpark, Hive, and Pandas performance across **5 different dataset sizes** ranging from 1 month to 12 months of Hong Kong traffic data (202409-202508).

## Key Enhancements

### 1. **Multiple Dataset Sizes**

Tests 5 different data sizes to analyze scalability:

| Dataset Size | Months | HDFS Paths | Approx. Size |
|--------------|--------|------------|--------------|
| 1_month_202508 | 1 | 202508 | ~1GB |
| 2_months | 2 | 202508, 202507 | ~2GB |
| 3_months | 3 | 202508, 202507, 202506 | ~3GB |
| 6_months | 6 | 202508-202503 | ~6GB |
| 12_months | 12 | 202508-202409 | ~12GB |

### 2. **Hive SQL in Standalone Files**

SQL queries extracted to separate `.hql` files for better maintainability:

- `benchmark/queries/morning_rush.hql` - Morning rush hour query (6:00-10:00)
- `benchmark/queries/evening_rush.hql` - Evening rush hour query (17:00-21:00)

**Benefits:**
- Easy to modify queries without touching Python code
- Can run queries independently using `hive -f`
- Better version control and code organization

### 3. **Null Value Filtering**

All three methods now filter out records where `Road_EN` is null:

- **PySpark**: `df.filter(col("Road_EN").isNotNull())`
- **Hive**: `WHERE Road_EN IS NOT NULL` in SQL queries
- **Pandas**: Filters before converting to Pandas DataFrame

### 4. **Enhanced Timing and Metrics**

CSV export now includes:
- `dataset_size`: Which dataset was benchmarked
- `records_processed`: Number of records in each phase
- All other timing details per phase

**CSV Structure:**
```csv
timestamp,dataset_size,tool,phase,duration_seconds,records_processed,details
2025-11-15T10:30:45.123,1_month_202508,PySpark,Data Read,5.432,0,from 1 path(s)
2025-11-15T10:30:50.555,1_month_202508,PySpark,Clean Nulls,1.234,0,drop Road_EN nulls
2025-11-15T10:30:51.789,1_month_202508,PySpark,Cache & Count,8.234,1234567,
...
```

## Usage

### Run the Complete Benchmark

Tests all 5 dataset sizes sequentially:

```bash
spark-submit benchmark/benchmark.py
```

### For Large Datasets (6+ months)

Increase memory allocation:

```bash
spark-submit \
  --driver-memory 8G \
  --executor-memory 8G \
  --conf spark.driver.maxResultSize=4G \
  --conf spark.executor.memoryOverhead=2G \
  benchmark/benchmark.py
```

### Run Hive Queries Independently

You can test the Hive queries standalone:

```bash
# Create temporary table first
hive -e "
  CREATE TEMPORARY VIEW traffic_data
  USING parquet
  OPTIONS (path 'hdfs:///traffic_data_partitioned/202508')
"

# Run morning query
hive -f benchmark/queries/morning_rush.hql

# Run evening query
hive -f benchmark/queries/evening_rush.hql
```

## Output

### Console Output

For each dataset size, you'll see:

1. **Per-tool benchmarks** with detailed phase timing
2. **Top 10 busiest roads** for morning and evening rush hours
3. **Dataset summary** comparing all three tools
4. **Overall summary** showing performance across all dataset sizes

### CSV Export

All timing data saved to: `benchmark_logs/benchmark_timing_results.csv`

## Example Output

```
====================================================================================================
BENCHMARKING DATASET: 1_month_202508 (1 month(s))
====================================================================================================

[1_month_202508][PySpark] Data Read: 5.432s | Records: 0 | from 1 path(s)
[1_month_202508][PySpark] Clean Nulls: 1.234s | Records: 0 | drop Road_EN nulls
[1_month_202508][PySpark] Cache & Count: 8.234s | Records: 1,234,567 |
...

--------------------------------------------------------------------------------
SUMMARY FOR 1_month_202508
--------------------------------------------------------------------------------
PySpark: 25.43s
Hive: 28.12s
Pandas: 45.67s
Fastest: PySpark (25.43s)

====================================================================================================
OVERALL BENCHMARK SUMMARY
====================================================================================================

1_month_202508:
  PySpark  :    25.43s
  Hive     :    28.12s
  Pandas   :    45.67s
  Fastest: PySpark
    PySpark is 1.11x faster than Hive
    PySpark is 1.80x faster than Pandas

2_months:
  PySpark  :    42.15s
  Hive     :    48.32s
  Pandas   :    89.45s
  Fastest: PySpark
    PySpark is 1.15x faster than Hive
    PySpark is 2.12x faster than Pandas

...
```

## Scalability Analysis

Use the CSV output to create visualizations:

### 1. Performance vs Data Size

```python
import pandas as pd
import matplotlib.pyplot as plt

# Read results
df = pd.read_csv('benchmark_logs/benchmark_timing_results.csv')

# Filter total execution times
totals = df[df['phase'] == 'Total Execution']

# Create scalability chart
for tool in ['PySpark', 'Hive', 'Pandas']:
    tool_data = totals[totals['tool'] == tool]
    plt.plot(tool_data['dataset_size'], tool_data['duration_seconds'],
             marker='o', label=tool)

plt.xlabel('Dataset Size')
plt.ylabel('Execution Time (seconds)')
plt.title('Scalability: Execution Time vs Data Size')
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('scalability_chart.png')
```

### 2. Speedup Analysis

```python
# Calculate speedup of PySpark vs Pandas
pyspark_times = totals[totals['tool'] == 'PySpark'].set_index('dataset_size')
pandas_times = totals[totals['tool'] == 'Pandas'].set_index('dataset_size')

speedup = pandas_times['duration_seconds'] / pyspark_times['duration_seconds']

plt.bar(speedup.index, speedup.values)
plt.xlabel('Dataset Size')
plt.ylabel('Speedup (Pandas time / PySpark time)')
plt.title('PySpark Speedup Over Pandas')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('speedup_chart.png')
```

## Customization

### Add More Dataset Sizes

Edit the `DATASETS` dictionary in `benchmark.py`:

```python
DATASETS = {
    '1_month_202508': ['hdfs:///traffic_data_partitioned/202508'],
    # Add your custom size
    '4_months': ['hdfs:///traffic_data_partitioned/202508',
                 'hdfs:///traffic_data_partitioned/202507',
                 'hdfs:///traffic_data_partitioned/202506',
                 'hdfs:///traffic_data_partitioned/202505'],
}
```

### Modify Hive Queries

Edit the `.hql` files in `benchmark/queries/`:

- Change time periods
- Add more aggregations
- Modify filtering criteria

### Change Top N Results

Edit the `.hql` files:

```sql
-- Change LIMIT 10 to LIMIT 20 for top 20 roads
LIMIT 20
```

## Expected Performance Patterns

### Small Datasets (1-2 months)
- **Pandas** may be competitive due to low overhead
- **PySpark** has initialization cost but fast execution
- **Hive** slower due to query optimization overhead

### Medium Datasets (3-6 months)
- **PySpark** advantages become clear
- **Pandas** memory usage increases
- **Hive** performance depends on cluster

### Large Datasets (12 months)
- **PySpark** significantly faster with parallelization
- **Pandas** may hit memory limits
- **Hive** benefits from distributed processing

## File Structure

```
benchmark/
├── benchmark.py              # Main benchmark script (updated)
├── benchmark_congestion.py   # Congestion analysis benchmark
├── queries/
│   ├── morning_rush.hql     # Morning rush hour query
│   └── evening_rush.hql     # Evening rush hour query
├── benchmark_logs/
│   └── benchmark_timing_results.csv  # Timing output
├── BENCHMARK_README.md
├── MULTI_SIZE_BENCHMARK_README.md    # This file
└── CONGESTION_BENCHMARK_README.md
```

## Troubleshooting

### Out of Memory (Pandas)

For datasets > 4 months, Pandas may run out of memory:
```bash
# Increase driver memory
spark-submit --driver-memory 16G benchmark/benchmark.py
```

Or skip Pandas for large datasets by commenting out in `main()`.

### Slow Performance

- Check Spark UI: http://localhost:4040
- Verify HDFS data availability
- Adjust shuffle partitions in `initialize_spark()`

### SQL File Not Found

Ensure you're running from the correct directory:
```bash
cd /home/richard/project/BDA/pyspark
spark-submit benchmark/benchmark.py
```

## Next Steps

1. **Run the benchmark** on your cluster
2. **Analyze the CSV** to identify performance patterns
3. **Create visualizations** to present findings
4. **Optimize configurations** based on results
5. **Scale to larger datasets** if needed

## License

This benchmark suite is provided for educational and analytical purposes.
