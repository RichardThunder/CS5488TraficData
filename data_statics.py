from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, regexp_extract, to_timestamp, concat, lit, when
import os
import logging
from typing import Optional

dir_path = "hdfs://hadoop-namenode:9000/traffic_data_partitioned/202506"


logging.basicConfig(
    level = logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def initialize_spark() -> SparkSession:
    return SparkSession.builder \
        .appName("Traffic XML to CSV - Memory Efficient") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .getOrCreate()

def main():
    spark = initialize_spark()
    logger.info("Spark Session initialized.")
    # Further processing can be added here  

    df = spark.read.parquet(dir_path)
    df.show(10,truncate=False)
    df.printSchema()

    #morning_df = df.filter(col("period_from") >= to_timestamp(lit("2024-10-22 07:00:00")) & col("period_to") <= to_timestamp(lit("2024-10-22 09:00:00"))).groupBy()
        
    





if __name__ == "__main__":
    main()