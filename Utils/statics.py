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
datasetPath = '/traffic_data_partitioned'
# Load data from all subdirectories under datasetPath
df = sparkSession.read.parquet(f"{datasetPath}/*")
print("Data loaded successfully.")
print(f"Total records count: {df.count()}")
print("Schema of the data:")
df.printSchema()