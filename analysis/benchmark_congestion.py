from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, hour, sum as spark_sum, avg as spark_avg, desc, count, when
from pyspark import StorageLevel
import logging
from typing import Optional, Tuple, Dict, List
import time
import pandas as pd
import csv
from datetime import datetime
import os

'''
Congestion Analysis Benchmark - Hong Kong Traffic Data
Compare performance of PySpark, Hive, and Pandas for congestion analysis across different data sizes

Congestion Metrics:
- High occupancy (>50%) with low speed (<40 km/h) = Severe congestion
- Volume-to-capacity analysis
- Average speed during rush hours
- Congestion duration and patterns

Data Sizes:
- Testing 202409-202508 (12 months total)
- Configurations from 1 month to 12 months
'''

# Dataset configurations - testing different sizes (202409-202508)
DATASETS = {
    '1_month_202508': ['hdfs:///traffic_data_partitioned/202508'],
    '2_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507'],
    '3_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
                 'hdfs:///traffic_data_partitioned/202506'],
    '6_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
                 'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505',
                 'hdfs:///traffic_data_partitioned/202504', 'hdfs:///traffic_data_partitioned/202503'],
    '12_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
                  'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505',
                  'hdfs:///traffic_data_partitioned/202504', 'hdfs:///traffic_data_partitioned/202503',
                  'hdfs:///traffic_data_partitioned/202502', 'hdfs:///traffic_data_partitioned/202501',
                  'hdfs:///traffic_data_partitioned/202412', 'hdfs:///traffic_data_partitioned/202411',
                  'hdfs:///traffic_data_partitioned/202410', 'hdfs:///traffic_data_partitioned/202409']
}

# SQL query files directory
SQL_DIR = "benchmark/queries"

# Timing results storage
timing_results: List[Dict[str, any]] = []

def load_sql_query(filename: str) -> str:
    """Load SQL query from file"""
    filepath = os.path.join(SQL_DIR, filename)
    with open(filepath, 'r') as f:
        return f.read()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def record_timing(dataset_size: str, tool: str, phase: str, duration: float,
                  records: int = 0, details: str = ""):
    """Record timing information for later CSV export"""
    timing_results.append({
        'timestamp': datetime.now().isoformat(),
        'dataset_size': dataset_size,
        'tool': tool,
        'phase': phase,
        'duration_seconds': round(duration, 3),
        'records_processed': records,
        'details': details
    })
    logger.info(f"[{dataset_size}][{tool}] {phase}: {duration:.3f}s | Records: {records:,} | {details}")

def initialize_spark() -> SparkSession:
    return SparkSession.builder \
        .appName("Congestion Analysis Benchmark - Hong Kong Traffic") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000") \
        .enableHiveSupport() \
        .getOrCreate()

def analyze_congestion_pyspark(df: DataFrame, start_hour: int, end_hour: int,
                                period_name: str) -> DataFrame:
    """
    Analyze road congestion during specified hours

    Congestion criteria:
    - Severe: occupancy > 50 AND speed < 40
    - Moderate: occupancy > 30 AND speed < 60
    - Light: occupancy > 20

    Returns top 10 most congested roads with metrics
    """
    result = df.filter(
        (hour(col("period_from")) >= start_hour) &
        (hour(col("period_from")) < end_hour)
    ).groupBy("Road_EN", "District") \
     .agg(
         spark_sum("volume").alias("total_volume"),
         spark_avg("occupancy").alias("avg_occupancy"),
         spark_avg("speed").alias("avg_speed"),
         count("*").alias("observation_count"),
         # Congestion severity flags
         spark_sum(when((col("occupancy") > 50) & (col("speed") < 40), 1).otherwise(0)).alias("severe_congestion_count"),
         spark_sum(when((col("occupancy") > 30) & (col("speed") < 60), 1).otherwise(0)).alias("moderate_congestion_count")
     ) \
     .withColumn("congestion_score",
                 col("avg_occupancy") * 0.4 +
                 (100 - col("avg_speed")) * 0.3 +
                 (col("severe_congestion_count") / col("observation_count") * 100) * 0.3) \
     .orderBy(desc("congestion_score")) \
     .limit(10)

    return result

def benchmark_pyspark_congestion(spark: SparkSession, dataset_size: str,
                                  data_paths: List[str]) -> float:
    """
    Run PySpark congestion analysis benchmark
    """
    logger.info("=" * 80)
    logger.info(f"STARTING PYSPARK CONGESTION BENCHMARK - Dataset: {dataset_size}")
    logger.info("=" * 80)

    overall_start = time.time()

    # Read data
    read_start = time.time()
    df = spark.read.parquet(*data_paths)
    read_time = time.time() - read_start

    # Clean null values - drop records where Road_EN is null
    clean_start = time.time()
    df = df.filter(col("Road_EN").isNotNull())
    clean_time = time.time() - clean_start

    # Cache data
    cache_start = time.time()
    df.persist(StorageLevel.MEMORY_AND_DISK)
    record_count = df.count()
    cache_time = time.time() - cache_start
    record_timing(dataset_size, "PySpark", "Data Read", read_time, 0, f"from {len(data_paths)} path(s)")
    record_timing(dataset_size, "PySpark", "Clean Nulls", clean_time, record_count)
    record_timing(dataset_size, "PySpark", "Cache & Count", cache_time, record_count)

    # Morning congestion analysis: 6:00 - 10:00
    morning_start = time.time()
    morning_results = analyze_congestion_pyspark(df, 6, 10, "Morning")
    morning_count = morning_results.count()
    logger.info("\n--- TOP 10 MOST CONGESTED ROADS (Morning 6:00-10:00) ---")
    morning_results.show(truncate=False)
    morning_time = time.time() - morning_start
    record_timing(dataset_size, "PySpark", "Morning Congestion Analysis",
                  morning_time, morning_count, "6:00-10:00")

    # Evening congestion analysis: 17:00 - 21:00
    evening_start = time.time()
    evening_results = analyze_congestion_pyspark(df, 17, 21, "Evening")
    evening_count = evening_results.count()
    logger.info("\n--- TOP 10 MOST CONGESTED ROADS (Evening 17:00-21:00) ---")
    evening_results.show(truncate=False)
    evening_time = time.time() - evening_start
    record_timing(dataset_size, "PySpark", "Evening Congestion Analysis",
                  evening_time, evening_count, "17:00-21:00")

    # Unpersist
    df.unpersist()

    total_time = time.time() - overall_start
    record_timing(dataset_size, "PySpark", "Total Execution", total_time, record_count)

    return total_time

def benchmark_hive_congestion(spark: SparkSession, dataset_size: str,
                               data_paths: List[str]) -> float:
    """
    Run Hive congestion analysis benchmark
    """
    logger.info("=" * 80)
    logger.info(f"STARTING HIVE CONGESTION BENCHMARK - Dataset: {dataset_size}")
    logger.info("=" * 80)

    overall_start = time.time()

    # Create temporary view
    view_start = time.time()

    # Read and clean data
    df_temp = spark.read.parquet(*data_paths)
    df_clean = df_temp.filter(col("Road_EN").isNotNull())
    df_clean.createOrReplaceTempView("traffic_data")

    view_time = time.time() - view_start
    record_timing(dataset_size, "Hive", "Create View & Clean", view_time, 0)

    # Load and execute morning congestion query from file
    logger.info("\n--- HIVE: TOP 10 MOST CONGESTED ROADS (Morning 6:00-10:00) ---")
    morning_start = time.time()
    morning_query = load_sql_query("morning_congestion.hql")
    morning_results = spark.sql(morning_query)
    morning_count = morning_results.count()
    morning_results.show(truncate=False)
    morning_time = time.time() - morning_start
    record_timing(dataset_size, "Hive", "Morning Congestion Query",
                  morning_time, morning_count, "6:00-10:00")

    # Load and execute evening congestion query from file
    logger.info("\n--- HIVE: TOP 10 MOST CONGESTED ROADS (Evening 17:00-21:00) ---")
    evening_start = time.time()
    evening_query = load_sql_query("evening_congestion.hql")
    evening_results = spark.sql(evening_query)
    evening_count = evening_results.count()
    evening_results.show(truncate=False)
    evening_time = time.time() - evening_start
    record_timing(dataset_size, "Hive", "Evening Congestion Query",
                  evening_time, evening_count, "17:00-21:00")

    total_time = time.time() - overall_start
    record_timing(dataset_size, "Hive", "Total Execution", total_time, 0)

    return total_time

def benchmark_pandas_congestion(spark: SparkSession, dataset_size: str,
                                 data_paths: List[str]) -> float:
    """
    Run Pandas congestion analysis benchmark (serial computation)
    """
    logger.info("=" * 80)
    logger.info(f"STARTING PANDAS CONGESTION BENCHMARK - Dataset: {dataset_size}")
    logger.info("=" * 80)

    overall_start = time.time()

    # Read data
    read_start = time.time()
    df_spark = spark.read.parquet(*data_paths)
    read_time = time.time() - read_start
    record_timing(dataset_size, "Pandas", "Read Parquet (via Spark)", read_time, 0)

    # Clean nulls before conversion
    clean_start = time.time()
    df_spark = df_spark.filter(col("Road_EN").isNotNull())
    clean_time = time.time() - clean_start
    record_timing(dataset_size, "Pandas", "Clean Nulls (Spark)", clean_time, 0)

    # Convert to Pandas
    convert_start = time.time()
    df = df_spark.toPandas()
    record_count = len(df)
    convert_time = time.time() - convert_start
    record_timing(dataset_size, "Pandas", "Convert to Pandas", convert_time, record_count)

    # Preprocess
    preprocess_start = time.time()
    df['hour'] = pd.to_datetime(df['period_from']).dt.hour
    preprocess_time = time.time() - preprocess_start
    record_timing(dataset_size, "Pandas", "Preprocess (extract hour)", preprocess_time, record_count)

    # Morning congestion analysis
    morning_start = time.time()
    morning_df = df[(df['hour'] >= 6) & (df['hour'] < 10)].copy()

    # Calculate congestion flags
    morning_df['severe_congestion'] = ((morning_df['occupancy'] > 50) &
                                        (morning_df['speed'] < 40)).astype(int)
    morning_df['moderate_congestion'] = ((morning_df['occupancy'] > 30) &
                                          (morning_df['speed'] < 60)).astype(int)

    morning_grouped = morning_df.groupby(['Road_EN', 'District']).agg({
        'volume': 'sum',
        'occupancy': 'mean',
        'speed': 'mean',
        'detector_id': 'count',
        'severe_congestion': 'sum',
        'moderate_congestion': 'sum'
    }).reset_index()

    morning_grouped.columns = ['Road_EN', 'District', 'total_volume', 'avg_occupancy',
                               'avg_speed', 'observation_count',
                               'severe_congestion_count', 'moderate_congestion_count']

    # Calculate congestion score
    morning_grouped['congestion_score'] = (
        morning_grouped['avg_occupancy'] * 0.4 +
        (100 - morning_grouped['avg_speed']) * 0.3 +
        (morning_grouped['severe_congestion_count'] / morning_grouped['observation_count'] * 100) * 0.3
    )

    morning_results = morning_grouped.sort_values('congestion_score', ascending=False).head(10)
    morning_time = time.time() - morning_start
    record_timing(dataset_size, "Pandas", "Morning Congestion Analysis",
                  morning_time, len(morning_results), "6:00-10:00")

    logger.info("\n--- PANDAS: TOP 10 MOST CONGESTED ROADS (Morning 6:00-10:00) ---")
    logger.info(morning_results.to_string(index=False))

    # Evening congestion analysis
    evening_start = time.time()
    evening_df = df[(df['hour'] >= 17) & (df['hour'] < 21)].copy()

    evening_df['severe_congestion'] = ((evening_df['occupancy'] > 50) &
                                        (evening_df['speed'] < 40)).astype(int)
    evening_df['moderate_congestion'] = ((evening_df['occupancy'] > 30) &
                                          (evening_df['speed'] < 60)).astype(int)

    evening_grouped = evening_df.groupby(['Road_EN', 'District']).agg({
        'volume': 'sum',
        'occupancy': 'mean',
        'speed': 'mean',
        'detector_id': 'count',
        'severe_congestion': 'sum',
        'moderate_congestion': 'sum'
    }).reset_index()

    evening_grouped.columns = ['Road_EN', 'District', 'total_volume', 'avg_occupancy',
                               'avg_speed', 'observation_count',
                               'severe_congestion_count', 'moderate_congestion_count']

    evening_grouped['congestion_score'] = (
        evening_grouped['avg_occupancy'] * 0.4 +
        (100 - evening_grouped['avg_speed']) * 0.3 +
        (evening_grouped['severe_congestion_count'] / evening_grouped['observation_count'] * 100) * 0.3
    )

    evening_results = evening_grouped.sort_values('congestion_score', ascending=False).head(10)
    evening_time = time.time() - evening_start
    record_timing(dataset_size, "Pandas", "Evening Congestion Analysis",
                  evening_time, len(evening_results), "17:00-21:00")

    logger.info("\n--- PANDAS: TOP 10 MOST CONGESTED ROADS (Evening 17:00-21:00) ---")
    logger.info(evening_results.to_string(index=False))

    total_time = time.time() - overall_start
    record_timing(dataset_size, "Pandas", "Total Execution", total_time, record_count)

    return total_time

def export_timing_to_csv(filename: str = "congestion_benchmark_results.csv"):
    """Export all timing results to a CSV file"""
    output_dir = "benchmark_logs"
    os.makedirs(output_dir, exist_ok=True)

    filepath = os.path.join(output_dir, filename)

    with open(filepath, 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'dataset_size', 'tool', 'phase',
                     'duration_seconds', 'records_processed', 'details']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for result in timing_results:
            writer.writerow(result)

    logger.info(f"\nTiming results exported to: {filepath}")
    return filepath

def main():
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

            # PySpark benchmark
            pyspark_time = benchmark_pyspark_congestion(spark, dataset_name, data_paths)
            results['PySpark'] = pyspark_time

            # Hive benchmark
            hive_time = benchmark_hive_congestion(spark, dataset_name, data_paths)
            results['Hive'] = hive_time

            # Pandas benchmark
            pandas_time = benchmark_pandas_congestion(spark, dataset_name, data_paths)
            results['Pandas'] = pandas_time

            all_results[dataset_name] = results

            # Dataset summary
            logger.info("\n" + "-" * 80)
            logger.info(f"SUMMARY FOR {dataset_name}")
            logger.info("-" * 80)
            logger.info(f"PySpark: {pyspark_time:.2f}s")
            logger.info(f"Hive: {hive_time:.2f}s")
            logger.info(f"Pandas: {pandas_time:.2f}s")

            fastest = min(results, key=results.get)
            logger.info(f"Fastest: {fastest} ({results[fastest]:.2f}s)")

        # Overall summary
        logger.info("\n" + "=" * 100)
        logger.info("OVERALL BENCHMARK SUMMARY")
        logger.info("=" * 100)

        for dataset_name, results in all_results.items():
            logger.info(f"\n{dataset_name}:")
            for tool, exec_time in results.items():
                logger.info(f"  {tool:10s}: {exec_time:8.2f}s")

            fastest = min(results, key=results.get)
            logger.info(f"  Fastest: {fastest}")

            # Calculate speedups
            for tool, tool_time in results.items():
                if tool != fastest:
                    speedup = tool_time / results[fastest]
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
