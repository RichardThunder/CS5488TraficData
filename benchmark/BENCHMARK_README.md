# Hong Kong Traffic Data Benchmark (August 2025)

This benchmark suite analyzes traffic data from Hong Kong for August 2025, finding the top 10 busiest roads during morning and evening rush hours using three different big data processing tools: PySpark, Hive, Pig Lati and Pandas.

## Overview

The benchmark performs the following analyses:
- **Morning Rush Hour Analysis** (6:00 - 10:00): Top 10 busiest roads
- **Evening Rush Hour Analysis** (17:00 - 21:00): Top 10 busiest roads
- **Comparative Analysis**: Traffic pattern differences between morning and evening

**Busiest** is determined by total traffic volume (number of vehicles) aggregated by road name.

## Data Schema

The traffic data includes the following fields:
```
detector_id     : Traffic detector identifier
direction       : Traffic direction
lane_id         : Lane identifier (Fast Lane, Middle Lane, Slow Lane)
occupancy       : Lane occupancy percentage
speed           : Vehicle speed
volume          : Number of vehicles
standard_deviation : Speed standard deviation
period_from     : Time period start
period_to       : Time period end
date            : Date of observation
District        : Hong Kong district
Road_EN         : Road name in English
Rotation        : Detector rotation angle
GeometryEasting : Eastern coordinate
GeometryNorthing: Northern coordinate
```

## Files in This Benchmark Suite

1. **benchmark.py** - PySpark implementation with built-in Hive support
2. **benchmark_hive.hql** - Pure HiveQL query script
3. **benchmark_pig.pig** - Pig Latin dataflow script
4. **run_all_benchmarks.sh** - Shell script to run all benchmarks and compare performance
5. **BENCHMARK_README.md** - This documentation file

## Prerequisites

Ensure you have the following installed and configured:
- Apache Spark (with PySpark)
- Apache Hive
- Apache Pig
- Hadoop HDFS (with data at `hdfs://traffic_partitioned_data/202508`)
- Python 3.7+

## Running the Benchmarks

### Option 1: Run All Benchmarks at Once (Recommended)

```bash
./run_all_benchmarks.sh
```

This will:
- Run all three benchmarks sequentially
- Time each execution
- Generate a performance comparison summary
- Save detailed logs to `./benchmark_logs/`
- Create a performance chart (if gnuplot is available)

### Option 2: Run Individual Benchmarks

#### PySpark Benchmark
```bash
spark-submit benchmark.py
```

Features:
- Uses Spark DataFrame API and SQL
- Includes both PySpark and Hive implementations
- Caches data for faster queries
- Adaptive query execution enabled

#### Hive Benchmark
```bash
hive -f benchmark_hive.hql
```

Features:
- Pure HiveQL queries
- Vectorized execution enabled
- Parallel execution enabled
- Includes comparative analysis query

#### Pig Latin Benchmark
```bash
pig -f benchmark_pig.pig
```

Features:
- Dataflow-style processing
- Multi-query optimization enabled
- Stores results to HDFS
- Includes comparative analysis

## Expected Output

Each benchmark will display:

1. **Morning Top 10 Busiest Roads**
   ```
   Road_EN                                    | District  | total_volume | total_occupancy | avg_speed
   -------------------------------------------|-----------|--------------|-----------------|----------
   Aberdeen Praya Road near Abba House        | Southern  | 15000        | 4500            | 65.5
   ...
   ```

2. **Evening Top 10 Busiest Roads**
   ```
   Similar format as morning results
   ```

3. **Performance Metrics**
   - Total execution time
   - Number of records processed
   - Comparative analysis between tools

## Benchmark Results Location

- **PySpark**: Console output + Spark UI (http://localhost:4040)
- **Hive**: Console output + HDFS logs
- **Pig Latin**:
  - Results saved to `hdfs://benchmark_results/morning_top10`
  - Results saved to `hdfs://benchmark_results/evening_top10`
  - Comparison saved to `hdfs://benchmark_results/comparison_analysis`

## Performance Tuning

### PySpark Tuning (in benchmark.py)
```python
.config("spark.sql.shuffle.partitions", "12")
.config("spark.default.parallelism", "12")
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.files.maxPartitionBytes", "128MB")
.config("spark.memory.fraction", "0.8")  # 80% of heap for execution and storage
.config("spark.memory.storageFraction", "0.3")  # 30% of that for caching
.config("spark.sql.inMemoryColumnarStorage.compressed", "true")  # Compress cached data
```

The benchmark uses `MEMORY_AND_DISK` storage level, which automatically spills to disk when memory is full, preventing OutOfMemoryErrors.

### Hive Tuning (in benchmark_hive.hql)
```sql
SET hive.exec.parallel=true;
SET hive.vectorized.execution.enabled=true;
```

### Pig Tuning (in benchmark_pig.pig)
```pig
SET default_parallel 12;
SET opt.multiquery true;
```

Adjust these parameters based on your cluster configuration and data size.

## Interpreting Results

### Traffic Volume
Higher total volume indicates busier roads. Look for:
- Roads with consistently high volume in both periods
- Roads with significant differences between morning and evening

### Occupancy
Higher occupancy indicates congestion. Compare with speed:
- High occupancy + Low speed = Severe congestion
- High occupancy + High speed = Heavy but flowing traffic

### Speed
Average speed patterns:
- Morning: Typically 50-70 km/h
- Evening: May be slower (40-60 km/h) due to heavier congestion

## Troubleshooting

### HDFS Path Issues
If data cannot be found, verify:
```bash
hdfs dfs -ls hdfs://traffic_partitioned_data/202508
```

### Memory Issues (PySpark)
If you encounter OutOfMemoryError, increase executor and driver memory:
```bash
spark-submit \
  --driver-memory 4G \
  --executor-memory 4G \
  --conf spark.driver.maxResultSize=2G \
  --conf spark.executor.memoryOverhead=1G \
  benchmark.py
```

For very large datasets (>10GB), increase further:
```bash
spark-submit \
  --driver-memory 8G \
  --executor-memory 8G \
  --conf spark.driver.maxResultSize=4G \
  --conf spark.executor.memoryOverhead=2G \
  benchmark.py
```

### Hive Metastore Issues
Restart metastore if table creation fails:
```bash
hive --service metastore &
```

### Pig UDF Issues
Ensure piggybank.jar is registered for datetime functions:
```bash
pig -Dpig.additional.jars=/path/to/piggybank.jar -f benchmark_pig.pig
```

## Customization

### Changing Time Periods
Edit the hour ranges in each script:

**PySpark/Hive:**
```python
# Morning: Change start_hour and end_hour
topBusyRoads(df, 7, 11, "Morning")  # 7:00-11:00 instead of 6:00-10:00
```

**Pig:**
```pig
-- Change filter conditions
morning_traffic = FILTER traffic_data BY (hour >= 7 AND hour < 11);
```

### Changing Top N Results
Modify the `LIMIT` clause:
```sql
LIMIT 20  -- Get top 20 instead of top 10
```

## Performance Comparison

Expected relative performance (actual results will vary based on cluster configuration):
1. **PySpark**: Fastest for iterative queries (caching advantage)
2. **Hive**: Good for ad-hoc SQL-style analysis
3. **Pig**: Good for ETL pipelines, may be slower for aggregations

## Contact & Support

For issues or questions about this benchmark:
- Check Spark logs: `./spark-logs/`
- Check Hive logs: `/tmp/hive.log`
- Check Pig logs: `./pig_*.log`

## License

This benchmark suite is provided as-is for educational and analytical purposes.
