#!/usr/bin/env python3
"""
Example Usage of the Benchmark Framework

This file demonstrates different ways to use the benchmark framework:
1. Running a single test manually
2. Running multiple tests in sequence
3. Creating a custom test class
4. Accessing and analyzing results
"""

import sys
import os

# Add parent directory to path for imports when running with spark-submit
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, DataFrame
from benchmark.base_analysis_test import BaseAnalysisTest
from benchmark.concrete_tests import SparkBusyRoadTest, HiveBusyRoadTest, PandasBusyRoadTest
from pyspark.sql.functions import col
from typing import Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# Example 1: Running a Single Test Manually
# ============================================================================
def example_sparkBusyRoadTest(testName:str, datasetPath: dir, spark: SparkSession):
    # Initialize Spa

    # Create test instance
    test = SparkBusyRoadTest(
        name="PySpark-BusyRoad",
        data_path=datasetPath,
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

    

def example_HiveBusyRoadTest(testName:str, datasetPath: dir, spark: SparkSession):

    # Create test instance
    test = HiveBusyRoadTest(
        name="Hive-BusyRoad",
        data_path=datasetPath,
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

    

def example_PandasBusyRoadTest(testName:str, datasetPath: dir,spark: SparkSession):


    # Create test instance
    test = PandasBusyRoadTest(
        name="Pandas-BusyRoad",
        data_path=datasetPath,
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

    

def example_1_single_test(spark: SparkSession):
    """Run a single test manually with full control."""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: Running a Single Test Manually")
    print("=" * 80)



    # Create test instance
    test = SparkBusyRoadTest(
        name="PySpark-BusyRoad",
        data_path=['hdfs:///202508_subset_10pct'],
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

    


# ============================================================================
# Example 2: Running Multiple Tests in Sequence
# ============================================================================

def example_2_multiple_tests(spark: SparkSession):
    """Run multiple tests and compare results."""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Running Multiple Tests in Sequence")
    print("=" * 80)



    # Define test configuration
    dataset_config = {
        'data_path': ['hdfs:///traffic_data_partitioned/202508'],
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

    

    


# ============================================================================
# Main Function - Run All Examples
# ============================================================================

def main():
    """Run all examples."""
    print("\n" + "=" * 80)
    print("BENCHMARK FRAMEWORK - EXAMPLE USAGE")
    print("=" * 80)

    # Ez test
    # sparkSession = SparkSession.builder.appName(f"TEST").getOrCreate()
    # example_sparkBusyRoadTest(testName=f"SparkBusyRoadTest_{10}pct", datasetPath=f'hdfs:///202508_subset_{10}pct',spark=sparkSession)
    # sparkSession.stop()



    ##run 3 times
    for j in range(1,4):
        print(f"\n" + "=" * 80)
        print(f"RUNNING EXAMPLE LOOP {j}")
        print("=" * 80)
        for i in range(10,100,10):
            sparkSession = SparkSession.builder.appName(f"TEST") \
                .config("spark.driver.maxResultSize", "4g") \
                .enableHiveSupport().getOrCreate()
            datasetPath = f'hdfs:///202508_subset_{i}pct'
            example_sparkBusyRoadTest(testName=f"SparkBusyRoadTest_{i}pct", datasetPath=datasetPath,spark=sparkSession)
            example_HiveBusyRoadTest(testName=f"HiveBusyRoadTest_{i}pct", datasetPath=datasetPath,spark=sparkSession)

            if i <= 30:
                try:
                    example_PandasBusyRoadTest(testName=f"PandasBusyRoadTest_{i}pct", datasetPath=datasetPath,spark=sparkSession)
                except Exception as e:
                    logger.exception(f"PandasBusyRoadTest_{i}pct failed with error: {e}")
            sparkSession.stop()
    print("\n" + "=" * 80)
    print("Tests COMPLETE")
    print("=" * 80)



if __name__ == "__main__":
    main()
