#!/usr/bin/env python3
"""
Example Usage of the Benchmark Framework

This file demonstrates different ways to use the benchmark framework:
1. Running a single test manually (BusyRoad or Congestion)
2. Running multiple tests in sequence and comparing results
3. Running comprehensive benchmarks across multiple dataset sizes
4. Accessing and analyzing results

Available test types:
- BusyRoad Analysis: Find top 10 busiest roads during rush hours
- Congestion Analysis: Find roads with high occupancy rates (>50%)

Available frameworks:
- PySpark: Distributed processing using PySpark DataFrames
- Hive: SQL-based analysis using HiveQL
- Pandas: Single-machine processing using Pandas DataFrames (limited to smaller datasets)
"""

import sys
import os

# Add parent directory to path for imports when running with spark-submit
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, DataFrame
from benchmark.base_analysis_test import BaseAnalysisTest
from benchmark.concrete_tests import (
    SparkBusyRoadTest, HiveBusyRoadTest, PandasBusyRoadTest,
    SparkCongestionTest, HiveCongestionTest, PandasCongestionTest
)
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



# ============================================================================
# Congestion Test Examples
# ============================================================================

def example_SparkCongestionTest(testName: str, datasetPath: str, spark: SparkSession):
    """Run PySpark congestion analysis test."""

    # Create test instance
    test = SparkCongestionTest(
        name="PySpark-Congestion",
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
        print(f"  Congested roads (>50% occupancy): {result['analysis_results']['count']} roads")
    else:
        print(f"Test failed with error: {result['error']}")


def example_HiveCongestionTest(testName: str, datasetPath: str, spark: SparkSession):
    """Run Hive congestion analysis test."""

    # Create test instance
    test = HiveCongestionTest(
        name="Hive-Congestion",
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
        print(f"  Congested roads (>50% occupancy): {result['analysis_results']['count']} roads")
    else:
        print(f"Test failed with error: {result['error']}")


def example_PandasCongestionTest(testName: str, datasetPath: str, spark: SparkSession):
    """Run Pandas congestion analysis test."""

    # Create test instance
    test = PandasCongestionTest(
        name="Pandas-Congestion",
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
        print(f"  Congested roads (>50% occupancy): {result['analysis_results']['count']} roads")
    else:
        print(f"Test failed with error: {result['error']}")




def example_1_single_test(spark: SparkSession):
    """Run a single test manually with full control."""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: Running a Single BusyRoad Test Manually")
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


def example_congestion_single_test(spark: SparkSession):
    """Run a single congestion test manually with full control."""
    print("\n" + "=" * 80)
    print("EXAMPLE: Running a Single Congestion Test Manually")
    print("=" * 80)

    # Create test instance
    test = SparkCongestionTest(
        name="PySpark-Congestion",
        data_path='hdfs:///202508_subset_10pct',
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
        print(f"  Congested roads (>50% occupancy): {result['analysis_results']['count']} roads")
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
        'data_path': 'hdfs:///202508_subset_10pct',
        'spark': spark,
    }

    # Tests to run - both BusyRoad and Congestion
    tests = [
        (SparkBusyRoadTest, "PySpark-BusyRoad"),
        (HiveBusyRoadTest, "Hive-BusyRoad"),
        (SparkCongestionTest, "PySpark-Congestion"),
        (HiveCongestionTest, "Hive-Congestion"),
    ]

    # Run each test and collect results
    results = {}
    for test_class, test_name in tests:
        print(f"\n{'='*80}")
        print(f"Running: {test_name}")
        print(f"{'='*80}")
        test = test_class(name=test_name, **dataset_config)
        result = test.run()
        results[test_name] = result['total_time'] if result['success'] else None

    # Compare results
    print("\n" + "=" * 80)
    print("COMPARISON SUMMARY")
    print("=" * 80)

    # Compare BusyRoad tests
    print("\nBusyRoad Analysis:")
    busyroad_results = {k: v for k, v in results.items() if 'BusyRoad' in k and v is not None}
    if busyroad_results:
        fastest_br = min(busyroad_results, key=busyroad_results.get)
        for test_name, exec_time in busyroad_results.items():
            speedup = exec_time / busyroad_results[fastest_br] if fastest_br != test_name else 1.0
            print(f"  {test_name:25s}: {exec_time:8.2f}s (x{speedup:.2f})")
        print(f"  Fastest: {fastest_br}")

    # Compare Congestion tests
    print("\nCongestion Analysis:")
    congestion_results = {k: v for k, v in results.items() if 'Congestion' in k and v is not None}
    if congestion_results:
        fastest_cg = min(congestion_results, key=congestion_results.get)
        for test_name, exec_time in congestion_results.items():
            speedup = exec_time / congestion_results[fastest_cg] if fastest_cg != test_name else 1.0
            print(f"  {test_name:25s}: {exec_time:8.2f}s (x{speedup:.2f})")
        print(f"  Fastest: {fastest_cg}")

    

    


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
            spark_builder = SparkSession.builder.appName(f"TEST") \
                .config("spark.driver.maxResultSize", "4g") \
                .enableHiveSupport()
            
            # Configure log4j
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            log4j_path = os.path.join(project_root, "log4j.properties")
            if os.path.exists(log4j_path):
                spark_builder.config("spark.files", log4j_path)
                spark_builder.config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
                # Note: driver properties are passed differently in client vs cluster mode.
                # This assumes client mode, where the driver runs locally.
                spark_builder.config("spark.driver.extraJavaOptions", f"-Dlog4j.configuration=file://{os.path.abspath(log4j_path)}")

            sparkSession = spark_builder.getOrCreate()
            datasetPath = f'hdfs:///202508_subset_{i}pct'

            # BusyRoad Tests
            example_sparkBusyRoadTest(testName=f"SparkBusyRoadTest_{i}pct", datasetPath=datasetPath, spark=sparkSession)
            example_HiveBusyRoadTest(testName=f"HiveBusyRoadTest_{i}pct", datasetPath=datasetPath, spark=sparkSession)

            # Congestion Tests
            example_SparkCongestionTest(testName=f"SparkCongestionTest_{i}pct", datasetPath=datasetPath, spark=sparkSession)
            example_HiveCongestionTest(testName=f"HiveCongestionTest_{i}pct", datasetPath=datasetPath, spark=sparkSession)

            # Pandas tests (only for smaller datasets due to memory constraints)
            if i <= 30:
                try:
                    example_PandasBusyRoadTest(testName=f"PandasBusyRoadTest_{i}pct", datasetPath=datasetPath, spark=sparkSession)
                    example_PandasCongestionTest(testName=f"PandasCongestionTest_{i}pct", datasetPath=datasetPath, spark=sparkSession)
                except Exception as e:
                    logger.exception(f"Pandas test for {i}pct failed with error: {e}")
            sparkSession.stop()
    print("\n" + "=" * 80)
    print("Tests COMPLETE")
    print("=" * 80)



if __name__ == "__main__":
    main()
