# Road Congestion Analysis Benchmark

## Overview

Comprehensive benchmark suite for analyzing Hong Kong traffic congestion patterns using three different processing frameworks: **PySpark**, **Hive**, and **Pandas**. Tests performance across multiple data sizes (1GB to 6+ months of data).

## Key Features

### 1. **Congestion Analysis Metrics**

The benchmark calculates a **congestion score** based on three factors:

- **Occupancy** (40% weight): Higher occupancy indicates more vehicles on the road
- **Speed** (30% weight): Lower speed indicates slower traffic flow
- **Severe Congestion Events** (30% weight): Instances where occupancy > 50% AND speed < 40 km/h

**Congestion Score Formula:**
```
congestion_score = (avg_occupancy × 0.4) + ((100 - avg_speed) × 0.3) + (severe_events_ratio × 100 × 0.3)
```

**Severity Levels:**
- **Severe**: Occupancy > 50% AND Speed < 40 km/h
- **Moderate**: Occupancy > 30% AND Speed < 60 km/h
- **Light**: Occupancy > 20%

### 2. **Multi-Size Dataset Testing**

Tests performance on 6 different data sizes:

| Dataset | Size | Paths | Approx. Size |
|---------|------|-------|--------------|
| 1_month_202508 | 1 month | 202508 | ~1GB |
| 2_months | 2 months | 202508, 202507 | ~2GB |
| 3_months | 3 months | 202508, 202507, 202505 | ~3GB |
| 4_months | 4 months | 202508, 202507, 202505, 202504 | ~4GB |
| 5_months | 5 months | 202508, 202507, 202505, 202504, 202503 | ~5GB |
| 6_months | 6 months | All above + 202502 | ~6GB |

### 3. **Null Value Handling**

Automatically filters out records where `Road_EN` is null:
- **PySpark**: `df.filter(col("Road_EN").isNotNull())`
- **Hive**: Same filtering in view creation
- **Pandas**: Filtered before conversion from Spark DataFrame

### 4. **Detailed Phase Timing**

Each benchmark records granular timing for every phase:

#### PySpark Phases:
1. Data Read
2. Clean Nulls
3. Cache & Count
4. Morning Congestion Analysis (6:00-10:00)
5. Evening Congestion Analysis (17:00-21:00)
6. Total Execution

#### Hive Phases:
1. Create View & Clean
2. Morning Congestion Query (6:00-10:00)
3. Evening Congestion Query (17:00-21:00)
4. Total Execution

#### Pandas Phases:
1. Read Parquet (via Spark)
2. Clean Nulls (Spark)
3. Convert to Pandas
4. Preprocess (extract hour)
5. Morning Congestion Analysis (6:00-10:00)
6. Evening Congestion Analysis (17:00-21:00)
7. Total Execution

## Usage

### Run the Full Benchmark Suite

```bash
spark-submit benchmark_congestion.py
```

This will:
- Run congestion analysis on all 6 dataset sizes
- Test PySpark, Hive, and Pandas on each dataset
- Record detailed timing for every phase
- Export results to CSV for visualization

### Run with Memory Optimization (for larger datasets)

```bash
spark-submit \
  --driver-memory 8G \
  --executor-memory 8G \
  --conf spark.driver.maxResultSize=4G \
  --conf spark.executor.memoryOverhead=2G \
  benchmark_congestion.py
```

## Output

### Console Output

For each dataset size and tool, you'll see:

1. **Top 10 Most Congested Roads** for morning rush hour (6:00-10:00)
2. **Top 10 Most Congested Roads** for evening rush hour (17:00-21:00)
3. **Timing breakdown** for each phase
4. **Summary** comparing all three tools

Example output:
```
--- TOP 10 MOST CONGESTED ROADS (Morning 6:00-10:00) ---
Road_EN                                    District  total_volume  avg_occupancy  avg_speed  congestion_score
Aberdeen Praya Road near Abba House        Southern  15000        65.5           35.2       72.3
...
```

### CSV Export

All timing data is exported to: `benchmark_logs/congestion_benchmark_results.csv`

**CSV Structure:**
```csv
timestamp,dataset_size,tool,phase,duration_seconds,records_processed,details
2025-11-15T10:30:45.123,1_month_202508,PySpark,Data Read,5.432,0,from 1 path(s)
2025-11-15T10:30:50.555,1_month_202508,PySpark,Clean Nulls,1.234,1234567,
2025-11-15T10:30:51.789,1_month_202508,PySpark,Cache & Count,8.234,1234567,
...
```

## Performance Analysis

### Expected Results

**Small Datasets (1-2 months):**
- Pandas may be competitive due to low overhead
- PySpark has initialization overhead but fast execution

**Medium Datasets (3-4 months):**
- PySpark starts showing advantages
- Pandas memory usage increases

**Large Datasets (5-6 months):**
- PySpark significantly faster due to parallelization
- Pandas may face memory constraints
- Hive performance depends on cluster configuration

### Scalability Insights

The multi-size testing helps identify:
1. **Crossover point**: When does parallel processing beat serial?
2. **Memory limits**: When does Pandas run out of memory?
3. **Scaling efficiency**: How does performance scale with data size?

## Congestion Analysis Insights

### What the Results Tell You

1. **Congestion Score**: Higher score = more congested road
   - Score > 70: Severe congestion
   - Score 50-70: Moderate congestion
   - Score < 50: Light congestion

2. **Severe Congestion Count**: Number of time periods with critical congestion
   - High count indicates persistent problems

3. **Average Speed vs Occupancy**:
   - High occupancy + Low speed = Stop-and-go traffic
   - High occupancy + Moderate speed = Heavy but flowing traffic

### Use Cases

- **Traffic Planning**: Identify roads needing infrastructure improvements
- **Route Optimization**: Avoid highly congested roads during rush hours
- **Temporal Patterns**: Compare morning vs evening congestion
- **District Analysis**: Identify congestion hotspots by district

## Customization

### Change Congestion Criteria

Edit the congestion thresholds in the code:

```python
# Current: Severe congestion
(col("occupancy") > 50) & (col("speed") < 40)

# Example: More strict criteria
(col("occupancy") > 60) & (col("speed") < 30)
```

### Adjust Congestion Score Weights

Modify the formula in `analyze_congestion_pyspark()`:

```python
# Current weights: occupancy 40%, speed 30%, severe_events 30%
.withColumn("congestion_score",
            col("avg_occupancy") * 0.4 +
            (100 - col("avg_speed")) * 0.3 +
            (col("severe_congestion_count") / col("observation_count") * 100) * 0.3)

# Example: Prioritize speed more
.withColumn("congestion_score",
            col("avg_occupancy") * 0.3 +
            (100 - col("avg_speed")) * 0.5 +
            (col("severe_congestion_count") / col("observation_count") * 100) * 0.2)
```

### Change Time Periods

Modify the hour ranges in benchmark functions:

```python
# Current: Morning 6:00-10:00, Evening 17:00-21:00

# Example: Extend morning rush hour
analyze_congestion_pyspark(df, 5, 11, "Morning")  # 5:00-11:00
```

### Add More Datasets

Add entries to the `DATASETS` dictionary:

```python
DATASETS = {
    '1_month_202508': ['hdfs:///traffic_data_partitioned/202508'],
    # Add custom dataset
    '1_month_202501': ['hdfs:///traffic_data_partitioned/202501'],
    # Or custom combination
    'q2_2025': ['hdfs:///traffic_data_partitioned/202504',
                'hdfs:///traffic_data_partitioned/202505',
                'hdfs:///traffic_data_partitioned/202506']
}
```

## Visualization Recommendations

Use the CSV output to create:

### 1. Performance Comparison Charts

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('benchmark_logs/congestion_benchmark_results.csv')

# Total execution time by dataset size and tool
totals = df[df['phase'] == 'Total Execution']
pivot = totals.pivot(index='dataset_size', columns='tool', values='duration_seconds')

pivot.plot(kind='bar', figsize=(12, 6))
plt.xlabel('Dataset Size')
plt.ylabel('Execution Time (seconds)')
plt.title('Congestion Analysis Performance Comparison')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('congestion_performance.png')
```

### 2. Scalability Analysis

```python
# How does execution time scale with data size?
import numpy as np

# Extract number of months
totals['months'] = totals['dataset_size'].str.extract('(\d+)').astype(int)

for tool in ['PySpark', 'Hive', 'Pandas']:
    tool_data = totals[totals['tool'] == tool]
    plt.plot(tool_data['months'], tool_data['duration_seconds'], marker='o', label=tool)

plt.xlabel('Number of Months')
plt.ylabel('Execution Time (seconds)')
plt.title('Scalability Analysis: Time vs Data Size')
plt.legend()
plt.grid(True)
plt.savefig('scalability_analysis.png')
```

### 3. Phase Breakdown

```python
# Stacked bar chart showing time spent in each phase
phase_data = df[df['dataset_size'] == '6_months']
pivot_phases = phase_data.pivot(index='tool', columns='phase', values='duration_seconds')

pivot_phases.plot(kind='barh', stacked=True, figsize=(12, 6))
plt.xlabel('Time (seconds)')
plt.title('Phase Breakdown for 6-Month Dataset')
plt.tight_layout()
plt.savefig('phase_breakdown.png')
```

### 4. Speedup Chart

```python
# Calculate and visualize speedup ratios
for dataset in df['dataset_size'].unique():
    dataset_times = totals[totals['dataset_size'] == dataset]
    baseline = dataset_times[dataset_times['tool'] == 'Pandas']['duration_seconds'].values[0]

    for tool in ['PySpark', 'Hive']:
        tool_time = dataset_times[dataset_times['tool'] == tool]['duration_seconds'].values[0]
        speedup = baseline / tool_time
        print(f"{dataset} - {tool} speedup over Pandas: {speedup:.2f}x")
```

## Troubleshooting

### Out of Memory Errors (Pandas)

For large datasets (>4 months), Pandas may run out of memory:
- Reduce dataset size
- Increase driver memory
- Use PySpark or Hive instead

### HDFS Path Not Found

Verify data exists:
```bash
hdfs dfs -ls hdfs://traffic_data_partitioned/
```

### Slow Performance

- Check cluster resources (CPU, memory)
- Verify Spark configuration (shuffle partitions, parallelism)
- Monitor Spark UI: http://localhost:4040

## Contact & Support

For issues or questions:
- Check logs: `benchmark_logs/`
- Review Spark UI for job details
- Verify HDFS data availability

## License

This benchmark suite is provided for educational and analytical purposes.
