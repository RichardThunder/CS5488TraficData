#!/usr/bin/env python3
"""
Benchmark Test Harness

This script runs multiple analysis tests across different engines (PySpark, Hive, Pandas)
and dataset sizes. It collects timing results and exports them to CSV for comparison.

Usage:
    python run_benchmarks.py
    OR
    spark-submit --master yarn benchmark/run_benchmarks.py

Configuration:
    - Modify DATASETS to change which datasets to test
    - Modify TEST_CLASSES to change which tests to run
    - Set PANDAS_SKIP_THRESHOLD to control when Pandas tests are skipped
"""

import sys
import os

# Add parent directory to path for imports when running with spark-submit
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from benchmark.concrete_tests import (
    SparkBusyRoadTest,
    HiveBusyRoadTest,
    PandasBusyRoadTest,
    # Uncomment if you want to use congestion tests
    # SparkCongestionTest,
    # HiveCongestionTest,
    # PandasCongestionTest
)
from benchmark.base_analysis_test import BaseAnalysisTest
import logging
import time
import csv
import os
from typing import List, Dict, Type, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Dataset configurations - testing different sizes
DATASETS = {
    '1_month_202508': ['hdfs:///traffic_data_partitioned/202508'],
    '2_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507'],
    '3_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
                 'hdfs:///traffic_data_partitioned/202506'],
    # Uncomment to test larger datasets
    # '4_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
    #              'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505'],
    # '5_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
    #              'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505',
    #              'hdfs:///traffic_data_partitioned/202504'],
    # '6_months': ['hdfs:///traffic_data_partitioned/202508', 'hdfs:///traffic_data_partitioned/202507',
    #              'hdfs:///traffic_data_partitioned/202506', 'hdfs:///traffic_data_partitioned/202505',
    #              'hdfs:///traffic_data_partitioned/202504', 'hdfs:///traffic_data_partitioned/202503'],
}

# Skip Pandas for datasets larger than this threshold (to avoid OOM)
PANDAS_SKIP_THRESHOLD = 2  # Skip Pandas if dataset has more than 2 months

# HDFS output path for timing results
HDFS_TIMING_OUTPUT = "hdfs:///benchmark_results/timing"  # Parquet format
HDFS_TIMING_CSV_OUTPUT = "hdfs:///benchmark_results/timing_csv"  # CSV format
SAVE_TO_HDFS = True  # Set to True to automatically save results to HDFS

# Test classes to run
# Format: (TestClass, "friendly_name", skip_for_large_datasets)
TEST_CLASSES: List[tuple[Type[BaseAnalysisTest], str, bool]] = [
    (SparkBusyRoadTest, "PySpark-BusyRoad", False),
    (HiveBusyRoadTest, "Hive-BusyRoad", False),
    (PandasBusyRoadTest, "Pandas-BusyRoad", True),  # True means skip for large datasets
    # Uncomment to run congestion tests
    # (SparkCongestionTest, "PySpark-Congestion", False),
    # (HiveCongestionTest, "Hive-Congestion", False),
    # (PandasCongestionTest, "Pandas-Congestion", True),
]


def initialize_spark() -> SparkSession:
    """Initialize and return a SparkSession with optimized settings."""
    return SparkSession.builder \
        .appName("Traffic Benchmark Framework - Hong Kong") \
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


def get_hdfs_size_bytes(spark: SparkSession, data_paths: List[str]) -> int:
    """
    Get the total size of HDFS data paths in bytes.

    Args:
        spark: SparkSession instance
        data_paths: List of HDFS paths

    Returns:
        Total size in bytes
    """
    if not data_paths:
        return 0

    sc = spark.sparkContext
    conf = sc._jsc.hadoopConfiguration()

    try:
        first_path_obj = sc._jvm.org.apache.hadoop.fs.Path(data_paths[0])
        fs = first_path_obj.getFileSystem(conf)
    except Exception as e:
        raise IOError(f"Failed to get FileSystem for path '{data_paths[0]}'. "
                      f"Check HDFS configuration and connectivity. Original error: {e}")

    total_size = 0
    for path_str in data_paths:
        path = sc._jvm.org.apache.hadoop.fs.Path(path_str)
        if fs.exists(path):
            total_size += fs.getContentSummary(path).getLength()
    return total_size


def export_timing_to_csv(all_timing_results: List[Dict[str, Any]],
                         filename: str = "benchmark_timing_results.csv") -> str:
    """
    Export all timing results to a CSV file.

    Args:
        all_timing_results: List of timing dictionaries
        filename: Output CSV filename

    Returns:
        Path to the created CSV file
    """
    output_dir = "benchmark_logs"
    os.makedirs(output_dir, exist_ok=True)

    filepath = os.path.join(output_dir, filename)

    with open(filepath, 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'dataset_size', 'data_size_bytes', 'total_records',
                     'tool', 'phase', 'duration_seconds', 'records_processed', 'details']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for result in all_timing_results:
            writer.writerow(result)

    logger.info(f"\nTiming results exported to: {filepath}")
    return filepath


def run_test(test_class: Type[BaseAnalysisTest], test_name: str,
             dataset_name: str, data_paths: List[str], spark: SparkSession,
             data_size_bytes: int, total_records: int) -> Dict[str, Any]:
    """
    Run a single test and return the results.

    Args:
        test_class: Class of the test to run
        test_name: Friendly name for the test
        dataset_name: Name of the dataset
        data_paths: Paths to the data
        spark: SparkSession instance
        data_size_bytes: Size of the dataset in bytes
        total_records: Total number of records

    Returns:
        Dictionary containing test results
    """
    try:
        # Instantiate the test
        test = test_class(
            name=test_name,
            dataset_size=dataset_name,
            data_paths=data_paths,
            spark=spark,
            data_size_bytes=data_size_bytes,
            total_records=total_records
        )

        # Run the test
        result = test.run()

        return {
            'test_name': test_name,
            'success': result['success'],
            'total_time': result['total_time'],
            'timing_results': result['timing_results'],
            'error': result['error']
        }

    except Exception as e:
        logger.error(f"{test_name} test instantiation/run failed: {e}", exc_info=True)
        return {
            'test_name': test_name,
            'success': False,
            'total_time': 0.0,
            'timing_results': [],
            'error': str(e)
        }


def main():
    """Main harness function that orchestrates all benchmark tests."""
    logger.info("=" * 100)
    logger.info("BENCHMARK TEST HARNESS STARTING")
    logger.info("=" * 100)

    program_start = time.time()

    # Initialize Spark
    spark_init_start = time.time()
    spark = initialize_spark()
    spark_init_time = time.time() - spark_init_start
    logger.info(f"SparkSession initialized in {spark_init_time:.3f}s")

    # Storage for all results
    all_timing_results = []
    summary_results = {}

    try:
        # Iterate over each dataset
        for dataset_name, data_paths in DATASETS.items():
            logger.info("\n" + "=" * 100)
            logger.info(f"BENCHMARKING DATASET: {dataset_name} ({len(data_paths)} month(s))")
            logger.info("=" * 100)

            dataset_results = {}

            # Get dataset statistics
            try:
                data_size_bytes = get_hdfs_size_bytes(spark, data_paths)
                df_for_stats = spark.read.parquet(*data_paths)
                total_records = df_for_stats.count()
                logger.info(f"Dataset: {dataset_name}")
                logger.info(f"  Size: {data_size_bytes / (1024**3):.2f} GB")
                logger.info(f"  Records: {total_records:,}")
            except Exception as e:
                logger.error(f"Failed to get dataset stats for {dataset_name}: {e}")
                data_size_bytes = 0
                total_records = 0

            # Run each test
            num_months = len(data_paths)

            for test_class, test_name, skip_large in TEST_CLASSES:
                # Skip Pandas-based tests for large datasets if configured
                if skip_large and num_months > PANDAS_SKIP_THRESHOLD:
                    logger.info("\n" + "=" * 80)
                    logger.info(f"SKIPPING {test_name} for {dataset_name}")
                    logger.info(f"Dataset too large ({num_months} months > {PANDAS_SKIP_THRESHOLD} threshold)")
                    logger.info("=" * 80)
                    dataset_results[test_name] = None
                    continue

                # Run the test
                result = run_test(
                    test_class=test_class,
                    test_name=test_name,
                    dataset_name=dataset_name,
                    data_paths=data_paths,
                    spark=spark,
                    data_size_bytes=data_size_bytes,
                    total_records=total_records
                )

                # Store results
                if result['success']:
                    dataset_results[test_name] = result['total_time']
                else:
                    dataset_results[test_name] = None

                # Collect timing results
                all_timing_results.extend(result['timing_results'])

            # Store dataset summary
            summary_results[dataset_name] = dataset_results

            # Print dataset summary
            logger.info("\n" + "-" * 80)
            logger.info(f"SUMMARY FOR {dataset_name}")
            logger.info("-" * 80)

            for test_name, exec_time in dataset_results.items():
                if exec_time is not None:
                    logger.info(f"{test_name:25s}: {exec_time:8.2f}s")
                elif num_months > PANDAS_SKIP_THRESHOLD and "Pandas" in test_name:
                    logger.info(f"{test_name:25s}: SKIPPED (too large)")
                else:
                    logger.info(f"{test_name:25s}: FAILED")

            # Find fastest
            valid_results = {k: v for k, v in dataset_results.items() if v is not None}
            if valid_results:
                fastest = min(valid_results, key=valid_results.get)
                logger.info(f"\nFastest: {fastest} ({valid_results[fastest]:.2f}s)")

                # Calculate speedups
                for test_name, test_time in valid_results.items():
                    if test_name != fastest and valid_results[fastest] > 0:
                        speedup = test_time / valid_results[fastest]
                        logger.info(f"  {fastest} is {speedup:.2f}x faster than {test_name}")

        # Overall summary
        logger.info("\n" + "=" * 100)
        logger.info("OVERALL BENCHMARK SUMMARY")
        logger.info("=" * 100)

        for dataset_name, results in summary_results.items():
            logger.info(f"\n{dataset_name}:")
            for test_name, exec_time in results.items():
                if exec_time is not None:
                    logger.info(f"  {test_name:25s}: {exec_time:8.2f}s")
                else:
                    logger.info(f"  {test_name:25s}: FAILED/SKIPPED")

            valid_results = {k: v for k, v in results.items() if v is not None}
            if valid_results:
                fastest = min(valid_results, key=valid_results.get)
                logger.info(f"  Fastest: {fastest}")

        # Export to CSV
        csv_path = export_timing_to_csv(all_timing_results)
        logger.info(f"\nAll timing results saved to: {csv_path}")

        total_program_time = time.time() - program_start
        logger.info(f"\nTotal benchmark time: {total_program_time:.2f}s")

    except Exception as e:
        logger.error(f"Error during benchmark execution: {str(e)}", exc_info=True)

    finally:
        logger.info("\nStopping SparkSession...")
        spark.stop()
        logger.info("SparkSession stopped. Benchmark complete.")


if __name__ == "__main__":
    main()