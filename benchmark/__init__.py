"""
Benchmark Test Framework

A flexible framework for benchmarking data processing tasks across different
engines (PySpark, Hive, Pandas) using the Template Method design pattern.

Main Components:
- BaseAnalysisTest: Abstract base class defining the test structure
- Concrete test classes: Specific implementations for different engines
- run_benchmarks.py: Test harness for running multiple tests

Quick Start:
    from benchmark.concrete_tests import SparkBusyRoadTest
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Test").getOrCreate()
    test = SparkBusyRoadTest(
        name="PySpark",
        dataset_size="1_month",
        data_paths=['hdfs:///data/202508'],
        spark=spark
    )
    result = test.run()

See README_FRAMEWORK.md for detailed documentation.
"""

from benchmark.base_analysis_test import BaseAnalysisTest
from benchmark.concrete_tests import (
    SparkBusyRoadTest,
    HiveBusyRoadTest,
    PandasBusyRoadTest,
    SparkCongestionTest,
    HiveCongestionTest,
    PandasCongestionTest,
)

__version__ = "1.0.0"
__all__ = [
    "BaseAnalysisTest",
    "SparkBusyRoadTest",
    "HiveBusyRoadTest",
    "PandasBusyRoadTest",
    "SparkCongestionTest",
    "HiveCongestionTest",
    "PandasCongestionTest",
]
