from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, hour, sum as spark_sum, desc
from pyspark import StorageLevel
import logging
from typing import Optional, Tuple, Dict, List
import time
import pandas as pd
import csv
from datetime import datetime
import os

'''
Benchmark on multiple dataset sizes, compare performance of different tools:
- PySpark
- Hive
- Pandas (Serial)

Find top 10 busiest roads in Hong Kong during morning (6:00-10:00) and evening (17:00-21:00) rush hours

Tests data sizes from 1 month to 12 months (202409-202508)
'''

# Dataset configurations - testing different sizes
DATASETS = {
    '1_month_202508': ['hdfs:///traffic_data_partitioned/202508'],
    '2_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507'],
    '3_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
                 'hdfs:///traffic_data_partitioned/202506'],
    # '4_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
    #              'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505'],
    # '5_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
    #              'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505',
    #              'hdfs:///traffic_data_partitioned/202504'],
    # '6_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
    #              'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505',
    #              'hdfs:///traffic_data_partitioned/202504', 'hdfs:///traffic_data_partitioned/202503'],
    # '7_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
    #              'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505',
    #              'hdfs:///traffic_data_partitioned/202504', 'hdfs:///traffic_data_partitioned/202503',
    #              'hdfs:///traffic_data_partitioned/202502'],
    # '8_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
    #              'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505',
    #              'hdfs:///traffic_data_partitioned/202504', 'hdfs:///traffic_data_partitioned/202503',
    #              'hdfs:///traffic_data_partitioned/202502', 'hdfs:///traffic_data_partitioned/202501'],
    # '9_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
    #              'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505',
    #              'hdfs:///traffic_data_partitioned/202504', 'hdfs:///traffic_data_partitioned/202503',
    #              'hdfs:///traffic_data_partitioned/202502', 'hdfs:///traffic_data_partitioned/202501',
    #              'hdfs:///traffic_data_partitioned/202412'],
    # '10_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
    #               'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505',
    #               'hdfs:///traffic_data_partitioned/202504', 'hdfs:///traffic_data_partitioned/202503',
    #               'hdfs:///traffic_data_partitioned/202502', 'hdfs:///traffic_data_partitioned/202501',
    #               'hdfs:///traffic_data_partitioned/202412', 'hdfs:///traffic_data_partitioned/202411'],
    # '11_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
    #               'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505',
    #               'hdfs:///traffic_data_partitioned/202504', 'hdfs:///traffic_data_partitioned/202503',
    #               'hdfs:///traffic_data_partitioned/202502', 'hdfs:///traffic_data_partitioned/202501',
    #               'hdfs:///traffic_data_partitioned/202412', 'hdfs:///traffic_data_partitioned/202411',
    #               'hdfs:///traffic_data_partitioned/202410'],
    # '12_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
    #               'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505',
    #               'hdfs:///traffic_data_partitioned/202504', 'hdfs:///traffic_data_partitioned/202503',
    #               'hdfs:///traffic_data_partitioned/202502', 'hdfs:///traffic_data_partitioned/202501',
    #               'hdfs:///traffic_data_partitioned/202412', 'hdfs:///traffic_data_partitioned/202411',
    #               'hdfs:///traffic_data_partitioned/202410', 'hdfs:///traffic_data_partitioned/202409'],
}

# Skip Pandas for datasets larger than this threshold (to avoid OOM)
PANDAS_SKIP_THRESHOLD = 2  # Skip Pandas if dataset has more than 2 months

# SQL query files directory
SQL_DIR = "benchmark/queries"

# Timing results storage
timing_results: List[Dict[str, any]] = []

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def load_sql_query(filename: str) -> str:
    """Load SQL query from file"""
    filepath = os.path.join(SQL_DIR, filename)
    with open(filepath, 'r') as f:
        return f.read()

def get_hdfs_size_bytes(spark: SparkSession, data_paths: List[str]) -> int:
    """Get the total size of HDFS data paths in bytes."""
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    total_size = 0
    for path_str in data_paths:
        path = sc._jvm.org.apache.hadoop.fs.Path(path_str)
        if fs.exists(path):
            total_size += fs.getContentSummary(path).getLength()
    return total_size

def record_timing(dataset_size: str, tool: str, phase: str, duration: float,
                  records: int = 0, details: str = "",
                  data_size_bytes: int = 0, total_records: int = 0):
    """Record timing information for later CSV export"""
    timing_results.append({
        'timestamp': datetime.now().isoformat(),
        'dataset_size': dataset_size,
        'data_size_bytes': data_size_bytes,
        'total_records': total_records,
        'tool': tool,
        'phase': phase,
        'duration_seconds': round(duration, 3),
        'records_processed': records,
        'details': details
    })
    logger.info(f"[{dataset_size}][{tool}] {phase}: {duration:.3f}s | Records: {records:,} | {details}")

def initialize_spark() -> SparkSession:
    return SparkSession.builder \
        .appName("Traffic Benchmark - Hong Kong") \
        .config("spark.sql.files.ignoreCorruptFiles", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "64MB") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.5") \
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "5000") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()

def topBusyRoads(df: DataFrame, start_hour: int, end_hour: int, period_name: str) -> Optional[DataFrame]:
    """
    Find top 10 busiest roads based on total traffic volume during specified hours

    Args:
        df: Input DataFrame with traffic data
        start_hour: Start hour of the period (0-23)
        end_hour: End hour of the period (0-23)
        period_name: Name of the period for logging (e.g., "Morning", "Evening")

    Returns:
        DataFrame with top 10 busiest roads
    """
    logger.info(f"Analyzing {period_name} rush hour ({start_hour}:00 - {end_hour}:00)")

    # Filter by hour and aggregate by road
    result = df.filter(
        (hour(col("period_from")) >= start_hour) &
        (hour(col("period_from")) < end_hour)
    ).groupBy("Road_EN", "District") \
     .agg(
         spark_sum("volume").alias("total_volume"),
         spark_sum("occupancy").alias("total_occupancy")
     ) \
     .orderBy(desc("total_volume")) \
     .limit(10)

    return result

def benchmark_pyspark(spark: SparkSession, dataset_size: str, data_paths: List[str],
                      data_size_bytes: int, total_records: int) -> float:
    """
    Run PySpark benchmark to find top 10 busiest roads in morning and evening

    Args:
        spark: SparkSession
        dataset_size: Name of the dataset size (e.g., "1_month_202508")
        data_paths: List of HDFS paths to read
        data_size_bytes: Size of the dataset in bytes
        total_records: Total number of records in the dataset
        
    Returns:
        Execution time in seconds
    """
    logger.info("=" * 80)
    logger.info(f"STARTING PYSPARK BENCHMARK - Dataset: {dataset_size}")
    logger.info("=" * 80)

    overall_start = time.time()

    # Read data
    logger.info(f"Reading data from {len(data_paths)} path(s)")
    read_start = time.time()
    df = spark.read.parquet(*data_paths)
    read_time = time.time() - read_start
    record_timing(dataset_size, "PySpark", "Data Read", read_time, 0, f"from {len(data_paths)} path(s)",
                  data_size_bytes=data_size_bytes, total_records=total_records)

    # Clean null values - drop records where Road_EN is null
    clean_start = time.time()
    df = df.filter(col("Road_EN").isNotNull())
    clean_time = time.time() - clean_start
    record_timing(dataset_size, "PySpark", "Clean Nulls", clean_time, 0, "drop Road_EN nulls",
                  data_size_bytes=data_size_bytes, total_records=total_records)

    # Cache data
    cache_start = time.time()
    df.persist(StorageLevel.MEMORY_AND_DISK)
    record_count_after_clean = df.count()
    cache_time = time.time() - cache_start
    record_timing(dataset_size, "PySpark", "Cache & Count", cache_time, record_count_after_clean,
                  data_size_bytes=data_size_bytes, total_records=total_records)

    # Morning rush hour: 6:00 - 10:00
    morning_start = time.time()
    morning_results = topBusyRoads(df, 6, 10, "Morning")
    morning_count = morning_results.count()
    logger.info("\n--- TOP 10 BUSIEST ROADS (Morning 6:00-10:00) ---")
    morning_results.show(truncate=False)
    morning_time = time.time() - morning_start
    record_timing(dataset_size, "PySpark", "Morning Analysis", morning_time, morning_count, "6:00-10:00",
                  data_size_bytes=data_size_bytes, total_records=total_records)

    # Evening rush hour: 17:00 - 21:00
    evening_start = time.time()
    evening_results = topBusyRoads(df, 17, 21, "Evening")
    evening_count = evening_results.count()
    logger.info("\n--- TOP 10 BUSIEST ROADS (Evening 17:00-21:00) ---")
    evening_results.show(truncate=False)
    evening_time = time.time() - evening_start
    record_timing(dataset_size, "PySpark", "Evening Analysis", evening_time, evening_count, "17:00-21:00",
                  data_size_bytes=data_size_bytes, total_records=total_records)

    # Unpersist
    df.unpersist()

    total_time = time.time() - overall_start
    record_timing(dataset_size, "PySpark", "Total Execution", total_time, record_count_after_clean,
                  data_size_bytes=data_size_bytes, total_records=total_records)
    logger.info(f"\nPySpark total execution time: {total_time:.2f} seconds")

    return total_time

def benchmark_hive(spark: SparkSession, dataset_size: str, data_paths: List[str],
                   data_size_bytes: int, total_records: int) -> float:
    """
    Run Hive benchmark using HiveQL queries loaded from files

    Args:
        spark: SparkSession
        dataset_size: Name of the dataset size
        data_paths: List of HDFS paths to read
        data_size_bytes: Size of the dataset in bytes
        total_records: Total number of records in the dataset

    Returns:
        Execution time in seconds
    """
    logger.info("=" * 80)
    logger.info(f"STARTING HIVE BENCHMARK - Dataset: {dataset_size}")
    logger.info("=" * 80)

    overall_start = time.time()

    # Create temporary Hive table
    view_start = time.time()

    # Read and clean data, then create temp view
    df_temp = spark.read.parquet(*data_paths)
    df_clean = df_temp.filter(col("Road_EN").isNotNull())
    df_clean.createOrReplaceTempView("traffic_data")

    view_time = time.time() - view_start
    record_timing(dataset_size, "Hive", "Create View & Clean", view_time, 0,
                  data_size_bytes=data_size_bytes, total_records=total_records)
    logger.info("Created temporary Hive view")

    # Load and execute morning query from file
    logger.info("\n--- HIVE: TOP 10 BUSIEST ROADS (Morning 6:00-10:00) ---")
    morning_start = time.time()
    morning_query = load_sql_query("morning_rush.hql")
    morning_results = spark.sql(morning_query)
    morning_count = morning_results.count()
    morning_results.show(truncate=False)
    morning_time = time.time() - morning_start
    record_timing(dataset_size, "Hive", "Morning Query", morning_time, morning_count, "6:00-10:00",
                  data_size_bytes=data_size_bytes, total_records=total_records)

    # Load and execute evening query from file
    logger.info("\n--- HIVE: TOP 10 BUSIEST ROADS (Evening 17:00-21:00) ---")
    evening_start = time.time()
    evening_query = load_sql_query("evening_rush.hql")
    evening_results = spark.sql(evening_query)
    evening_count = evening_results.count()
    evening_results.show(truncate=False)
    evening_time = time.time() - evening_start
    record_timing(dataset_size, "Hive", "Evening Query", evening_time, evening_count, "17:00-21:00",
                  data_size_bytes=data_size_bytes, total_records=total_records)

    total_time = time.time() - overall_start
    record_timing(dataset_size, "Hive", "Total Execution", total_time, 0,
                  data_size_bytes=data_size_bytes, total_records=total_records)
    logger.info(f"\nHive total execution time: {total_time:.2f} seconds")

    return total_time

def benchmark_pandas(spark: SparkSession, dataset_size: str, data_paths: List[str],
                     data_size_bytes: int, total_records: int) -> float:
    """
    Run Pandas benchmark (serial computation) for comparison

    Args:
        spark: SparkSession
        dataset_size: Name of the dataset size
        data_paths: List of HDFS paths to read
        data_size_bytes: Size of the dataset in bytes
        total_records: Total number of records in the dataset

    Returns:
        Execution time in seconds
    """
    logger.info("=" * 80)
    logger.info(f"STARTING PANDAS BENCHMARK - Dataset: {dataset_size}")
    logger.info("=" * 80)

    overall_start = time.time()

    # Read data using Spark first, then convert to Pandas
    read_start = time.time()
    df_spark = spark.read.parquet(*data_paths)
    read_time = time.time() - read_start
    record_timing(dataset_size, "Pandas", "Read Parquet (via Spark)", read_time, 0,
                  data_size_bytes=data_size_bytes, total_records=total_records)

    # Clean null values before conversion
    clean_start = time.time()
    df_spark = df_spark.filter(col("Road_EN").isNotNull())
    clean_time = time.time() - clean_start
    record_timing(dataset_size, "Pandas", "Clean Nulls (Spark)", clean_time, 0, "drop Road_EN nulls",
                  data_size_bytes=data_size_bytes, total_records=total_records)

    # Convert to Pandas
    convert_start = time.time()
    df = df_spark.toPandas()
    record_count = len(df)
    convert_time = time.time() - convert_start
    record_timing(dataset_size, "Pandas", "Convert to Pandas", convert_time, record_count,
                  data_size_bytes=data_size_bytes, total_records=total_records)

    # Extract hour from period_from
    preprocess_start = time.time()
    df['hour'] = pd.to_datetime(df['period_from']).dt.hour
    preprocess_time = time.time() - preprocess_start
    record_timing(dataset_size, "Pandas", "Preprocess (extract hour)", preprocess_time, record_count,
                  data_size_bytes=data_size_bytes, total_records=total_records)

    # Morning rush hour: 6:00 - 10:00
    morning_start = time.time()
    morning_df = df[(df['hour'] >= 6) & (df['hour'] < 10)]
    morning_results = morning_df.groupby(['Road_EN', 'District']).agg({
        'volume': 'sum',
        'occupancy': 'sum'
    }).reset_index()
    morning_results.columns = ['Road_EN', 'District', 'total_volume', 'total_occupancy']
    morning_results = morning_results.sort_values('total_volume', ascending=False).head(10)
    morning_time = time.time() - morning_start
    record_timing(dataset_size, "Pandas", "Morning Analysis", morning_time, len(morning_results), "6:00-10:00",
                  data_size_bytes=data_size_bytes, total_records=total_records)

    logger.info("\n--- PANDAS: TOP 10 BUSIEST ROADS (Morning 6:00-10:00) ---")
    logger.info(morning_results.to_string(index=False))

    # Evening rush hour: 17:00 - 21:00
    evening_start = time.time()
    evening_df = df[(df['hour'] >= 17) & (df['hour'] < 21)]
    evening_results = evening_df.groupby(['Road_EN', 'District']).agg({
        'volume': 'sum',
        'occupancy': 'sum'
    }).reset_index()
    evening_results.columns = ['Road_EN', 'District', 'total_volume', 'total_occupancy']
    evening_results = evening_results.sort_values('total_volume', ascending=False).head(10)
    evening_time = time.time() - evening_start
    record_timing(dataset_size, "Pandas", "Evening Analysis", evening_time, len(evening_results), "17:00-21:00",
                  data_size_bytes=data_size_bytes, total_records=total_records)

    logger.info("\n--- PANDAS: TOP 10 BUSIEST ROADS (Evening 17:00-21:00) ---")
    logger.info(evening_results.to_string(index=False))

    total_time = time.time() - overall_start
    record_timing(dataset_size, "Pandas", "Total Execution", total_time, record_count,
                  data_size_bytes=data_size_bytes, total_records=total_records)
    logger.info(f"\nPandas total execution time: {total_time:.2f} seconds")

    return total_time

def export_timing_to_csv(filename: str = "benchmark_timing_results.csv"):
    """Export all timing results to a CSV file"""
    output_dir = "benchmark_logs"
    os.makedirs(output_dir, exist_ok=True)

    filepath = os.path.join(output_dir, filename)

    with open(filepath, 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'dataset_size', 'data_size_bytes', 'total_records', 'tool', 'phase',
                     'duration_seconds', 'records_processed', 'details']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for result in timing_results:
            writer.writerow(result)

    logger.info(f"\nTiming results exported to: {filepath}")
    return filepath

def main():
    # Record overall start time
    program_start = time.time()

    # Initialize Spark
    spark_init_start = time.time()
    spark = initialize_spark()
    spark_init_time = time.time() - spark_init_start
    logger.info(f"SparkSession initialized in {spark_init_time:.3f}s")

    # Store results for summary
    all_results = {}

    try:
        # Run benchmarks for each dataset size
        for dataset_name, data_paths in DATASETS.items():
            logger.info("\n" + "=" * 100)
            logger.info(f"BENCHMARKING DATASET: {dataset_name} ({len(data_paths)} month(s))")
            logger.info("=" * 100)

            results = {}

            # Get dataset stats
            data_size_bytes = get_hdfs_size_bytes(spark, data_paths)
            df_for_stats = spark.read.parquet(*data_paths)
            total_records = df_for_stats.count()
            logger.info(f"Dataset: {dataset_name}, Size: {data_size_bytes / (1024**3):.2f} GB, Records: {total_records:,}")

            # PySpark benchmark
            try:
                pyspark_time = benchmark_pyspark(spark, dataset_name, data_paths, data_size_bytes, total_records)
                results['PySpark'] = pyspark_time
            except Exception as e:
                logger.error(f"PySpark benchmark for {dataset_name} failed.", exc_info=True)
                results['PySpark'] = None
                record_timing(dataset_name, "PySpark", "Total Execution", 0, 0, f"FAILED: {str(e)}",
                              data_size_bytes=data_size_bytes, total_records=total_records)

            # Hive benchmark
            try:
                hive_time = benchmark_hive(spark, dataset_name, data_paths, data_size_bytes, total_records)
                results['Hive'] = hive_time
            except Exception as e:
                logger.error(f"Hive benchmark for {dataset_name} failed.", exc_info=True)
                results['Hive'] = None
                record_timing(dataset_name, "Hive", "Total Execution", 0, 0, f"FAILED: {str(e)}",
                              data_size_bytes=data_size_bytes, total_records=total_records)

            # Pandas benchmark - skip for large datasets to avoid OOM
            num_months = len(data_paths)
            if num_months <= PANDAS_SKIP_THRESHOLD:
                try:
                    pandas_time = benchmark_pandas(spark, dataset_name, data_paths, data_size_bytes, total_records)
                    results['Pandas'] = pandas_time
                except Exception as e:
                    logger.error(f"Pandas benchmark for {dataset_name} failed.", exc_info=True)
                    results['Pandas'] = None
                    record_timing(dataset_name, "Pandas", "Total Execution", 0, 0, f"FAILED: {str(e)}",
                                  data_size_bytes=data_size_bytes, total_records=total_records)
            else:
                logger.info("\n" + "=" * 80)
                logger.info(f"SKIPPING PANDAS BENCHMARK for {dataset_name}")
                logger.info(f"Dataset too large ({num_months} months > {PANDAS_SKIP_THRESHOLD} threshold)")
                logger.info("Pandas would likely cause OutOfMemoryError")
                logger.info("=" * 80)
                results['Pandas'] = None

            all_results[dataset_name] = results

            # Dataset summary
            logger.info("\n" + "-" * 80)
            logger.info(f"SUMMARY FOR {dataset_name}")
            logger.info("-" * 80)
            if results.get('PySpark') is not None:
                logger.info(f"PySpark: {results['PySpark']:.2f}s")
            else:
                logger.info("PySpark: FAILED")

            if results.get('Hive') is not None:
                logger.info(f"Hive: {results['Hive']:.2f}s")
            else:
                logger.info("Hive: FAILED")

            if results.get('Pandas') is not None:
                logger.info(f"Pandas: {results['Pandas']:.2f}s")
            elif num_months > PANDAS_SKIP_THRESHOLD:
                logger.info("Pandas: SKIPPED (too large)")
            else:
                logger.info("Pandas: FAILED")

            # Find fastest among non-None results
            valid_results = {k: v for k, v in results.items() if v is not None}
            if valid_results:
                fastest = min(valid_results, key=valid_results.get)
                logger.info(f"Fastest: {fastest} ({valid_results[fastest]:.2f}s)")

        # Overall summary
        logger.info("\n" + "=" * 100)
        logger.info("OVERALL BENCHMARK SUMMARY")
        logger.info("=" * 100)

        for dataset_name, results in all_results.items():
            logger.info(f"\n{dataset_name}:")
            valid_results = {k: v for k, v in results.items() if v is not None}
            for tool, exec_time in results.items():
                if exec_time is not None:
                    logger.info(f"  {tool:10s}: {exec_time:8.2f}s")
                else:
                    logger.info(f"  {tool:10s}: FAILED/SKIPPED")

            if valid_results:
                fastest = min(valid_results, key=valid_results.get)
                logger.info(f"  Fastest: {fastest}")

                # Calculate speedups
                for tool, tool_time in valid_results.items():
                    if tool != fastest and tool_time is not None and valid_results[fastest] > 0:
                        speedup = tool_time / valid_results[fastest]
                        logger.info(f"    {fastest} is {speedup:.2f}x faster than {tool}")

        # Export to CSV
        csv_path = export_timing_to_csv()
        logger.info(f"\nAll timing results saved to: {csv_path}")

        total_program_time = time.time() - program_start
        logger.info(f"\nTotal benchmark time: {total_program_time:.2f}s")

    except Exception as e:
        logger.error(f"Error during benchmark: {str(e)}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
