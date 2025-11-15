#!/usr/bin/env python3
"""
Example Usage of the Benchmark Framework

This file demonstrates different ways to use the benchmark framework:
1. Running a single test manually
2. Running multiple tests in sequence
3. Creating a custom test class
4. Accessing and analyzing results
"""

from pyspark.sql import SparkSession, DataFrame
from benchmark.base_analysis_test import BaseAnalysisTest
from benchmark.concrete_tests import SparkBusyRoadTest, HiveBusyRoadTest
from pyspark.sql.functions import col, count
from typing import Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# Example 1: Running a Single Test Manually
# ============================================================================

def example_1_single_test():
    """Run a single test manually with full control."""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: Running a Single Test Manually")
    print("=" * 80)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Example - Single Test") \
        .enableHiveSupport() \
        .getOrCreate()

    # Create test instance
    test = SparkBusyRoadTest(
        name="PySpark-BusyRoad",
        data_paths=['hdfs:///202508_subset_10pct'],
        spark=spark,
    )

    # Run the test
    result = test.run()

    # Access results
    print(f"\nTest completed successfully: {result['success']}")
    print(f"Total execution time: {result['total_time']:.2f} seconds")

    if result['success']:
        print("\nPhase-by-phase timing:")
        for timing in result['timing_results']:
            print(f"  {timing['phase']:20s}: {timing['duration_seconds']:6.3f}s - {timing['details']}")

        print("\nAnalysis results:")
        print(f"  Morning rush hour: {result['analysis_results']['morning']['count']} roads")
        print(f"  Evening rush hour: {result['analysis_results']['evening']['count']} roads")
    else:
        print(f"Test failed with error: {result['error']}")

    spark.stop()


# ============================================================================
# Example 2: Running Multiple Tests in Sequence
# ============================================================================

def example_2_multiple_tests():
    """Run multiple tests and compare results."""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Running Multiple Tests in Sequence")
    print("=" * 80)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Example - Multiple Tests") \
        .enableHiveSupport() \
        .getOrCreate()

    # Define test configuration
    dataset_config = {
        'data_paths': ['hdfs:///traffic_data_partitioned/202508'],
        'spark': spark,
    }

    # Tests to run
    tests = [
        (SparkBusyRoadTest, "PySpark-BusyRoad"),
        (HiveBusyRoadTest, "Hive-BusyRoad"),
    ]

    # Run each test and collect results
    results = {}
    for test_class, test_name in tests:
        test = test_class(name=test_name, **dataset_config)
        result = test.run()
        results[test_name] = result['total_time'] if result['success'] else None

    # Compare results
    print("\n" + "-" * 80)
    print("COMPARISON SUMMARY")
    print("-" * 80)

    valid_results = {k: v for k, v in results.items() if v is not None}
    if valid_results:
        fastest = min(valid_results, key=valid_results.get)

        for test_name, exec_time in results.items():
            if exec_time is not None:
                speedup = exec_time / valid_results[fastest] if fastest != test_name else 1.0
                print(f"{test_name:25s}: {exec_time:8.2f}s (x{speedup:.2f})")
            else:
                print(f"{test_name:25s}: FAILED")

        print(f"\nFastest: {fastest}")

    spark.stop()


# ============================================================================
# Example 3: Creating a Custom Test Class
# ============================================================================

class CustomDataQualityTest(BaseAnalysisTest):
    """
    Custom test that checks data quality metrics.

    This demonstrates how to create your own test class by inheriting
    from BaseAnalysisTest and implementing the required methods.
    """

    def initialize(self) -> None:
        """Initialize - verify SparkSession exists."""
        if self.spark is None:
            raise ValueError("SparkSession is required")
        logger.info("Data Quality test initialized")

    def read_data(self) -> DataFrame:
        """Read data from Parquet files."""
        logger.info(f"Reading data from {len(self.data_paths)} path(s)")
        return self.spark.read.parquet(*self.data_paths)

    def clean_data(self, data: DataFrame) -> DataFrame:
        """For quality checks, we don't clean - we analyze as-is."""
        logger.info("Skipping data cleaning for quality analysis")
        return data

    def execute_analysis(self, data: DataFrame) -> Dict[str, Any]:
        """
        Execute data quality checks.

        Checks:
        - Total record count
        - Null counts per column
        - Distinct road count
        - Date range coverage
        """
        logger.info("Executing data quality analysis")

        # Total records
        total_count = data.count()

        # Null counts for important columns
        null_counts = {}
        for col_name in ['Road_EN', 'District', 'volume', 'occupancy', 'period_from']:
            null_count = data.filter(col(col_name).isNull()).count()
            null_counts[col_name] = null_count

        # Distinct road count
        distinct_roads = data.select("Road_EN").distinct().count()

        # Distinct districts
        distinct_districts = data.select("District").distinct().count()

        results = {
            'null_counts': null_counts,
            'distinct_roads': distinct_roads,
            'distinct_districts': distinct_districts
        }

        # Log results
        logger.info("\n--- DATA QUALITY REPORT ---")
        logger.info(f"Total Records: {total_count:,}")
        logger.info(f"Distinct Roads: {distinct_roads:,}")
        logger.info(f"Distinct Districts: {distinct_districts:,}")
        logger.info("\nNull Counts:")
        for col_name, null_count in null_counts.items():
            pct = (null_count / total_count * 100) if total_count > 0 else 0
            logger.info(f"  {col_name:15s}: {null_count:8,} ({pct:5.2f}%)")

        return results


def example_3_custom_test():
    """Demonstrate custom test class."""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Running a Custom Test Class")
    print("=" * 80)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Example - Custom Test") \
        .getOrCreate()

    # Create and run custom test
    test = CustomDataQualityTest(
        name="DataQuality",
        data_paths=['hdfs:///traffic_data_partitioned/202508'],
        spark=spark,
    )

    result = test.run()

    # Access custom results
    if result['success']:
        print("\nData Quality Metrics:")
        analysis = result['analysis_results']
        print(f"  Distinct Roads: {analysis['distinct_roads']:,}")
        print(f"  Distinct Districts: {analysis['distinct_districts']:,}")
        print("\n  Null Percentages:")
        for col_name, null_count in analysis['null_counts'].items():
            print(f"    {col_name:15s}: {pct:5.2f}%")

    spark.stop()


# ============================================================================
# Example 4: Analyzing and Exporting Results
# ============================================================================

def example_4_analyze_results():
    """Run tests and export detailed timing analysis."""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: Analyzing and Exporting Results")
    print("=" * 80)

    import csv
    import os

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Example - Result Analysis") \
        .enableHiveSupport() \
        .getOrCreate()

    # Run a test
    test = SparkBusyRoadTest(
        name="PySpark-BusyRoad",
        data_paths=['hdfs:///traffic_data_partitioned/202508'],
        spark=spark,
    )

    result = test.run()

    # Analyze timing breakdown
    if result['success']:
        timing_results = result['timing_results']

        print("\nTiming Breakdown Analysis:")
        total_time = result['total_time']

        for timing in timing_results:
            if timing['phase'] != 'Total Execution':
                percentage = (timing['duration_seconds'] / total_time * 100)
                print(f"  {timing['phase']:20s}: {timing['duration_seconds']:6.3f}s ({percentage:5.1f}%)")

        # Export to CSV
        output_dir = "benchmark_logs"
        os.makedirs(output_dir, exist_ok=True)

        csv_file = os.path.join(output_dir, "example_results.csv")
        with open(csv_file, 'w', newline='') as f:
            fieldnames = ['phase', 'duration_seconds', 'percentage', 'details']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for timing in timing_results:
                if timing['phase'] != 'Total Execution':
                    percentage = (timing['duration_seconds'] / total_time * 100)
                    writer.writerow({
                        'phase': timing['phase'],
                        'duration_seconds': timing['duration_seconds'],
                        'percentage': round(percentage, 2),
                        'details': timing['details']
                    })

        print(f"\nResults exported to: {csv_file}")

    spark.stop()


# ============================================================================
# Main Function - Run All Examples
# ============================================================================

def main():
    """Run all examples."""
    print("\n" + "=" * 80)
    print("BENCHMARK FRAMEWORK - EXAMPLE USAGE")
    print("=" * 80)

    # Uncomment the examples you want to run
    # WARNING: These will attempt to connect to HDFS and run Spark jobs

    example_1_single_test()
    # example_2_multiple_tests()
    # example_3_custom_test()
    # example_4_analyze_results()

    print("\n" + "=" * 80)
    print("EXAMPLES COMPLETE")
    print("=" * 80)
    print("\nTo run examples:")
    print("  1. Uncomment the example functions in main()")
    print("  2. Ensure HDFS paths are correct")
    print("  3. Run: python benchmark/example_usage.py")
    print()


if __name__ == "__main__":
    main()
