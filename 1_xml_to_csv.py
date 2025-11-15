from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, regexp_extract, to_timestamp, concat, lit, when, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
import logging
from typing import Optional

GEOLOCATION_PATH = "Locations_of_Traffic_Detectors.gdb_converted.csv"



logging.basicConfig(
    level = logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def initialize_spark() -> SparkSession:
    return SparkSession.builder \
        .appName("Traffic XML to CSV - Optimized") \
        .config("spark.sql.shuffle.partitions", "36") \
        .config("spark.default.parallelism", "36") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .getOrCreate()


# read xml files from local storage
def XML_reader(local_dir:str, spark: SparkSession) -> Optional[DataFrame]:
    try:
        return spark.read.format("xml")\
                .option("rowTag", "raw_speed_volume_list") \
            .option("inferSchema", "false") \
            .option("valueTag", "_VALUE") \
            .option("attributePrefix", "_") \
            .option("charset", "UTF-8") \
            .option("mode", "DROPMALFORMED") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .load(local_dir) 
    except Exception as e:
        logger.exception(f"{'#'*40} Error reading xml files. {e}")
        return None

def get_location_schema() -> StructType:
    """Defines the schema for the geolocation CSV file to avoid inferring it."""
    return StructType([
        StructField("AID_ID_Number", IntegerType(), True),
        StructField("Location", StringType(), True),
        StructField("Road_EN", StringType(), True),
        StructField("Road_TC", StringType(), True),
        StructField("Road_SC", StringType(), True),
        StructField("District", StringType(), True),
        StructField("Rotation", DoubleType(), True),
        StructField("GeometryEasting", DoubleType(), True),
        StructField("GeometryNorthing", DoubleType(), True)
    ])

def read_location_data(file_path: str, spark:SparkSession) -> Optional[DataFrame]:
    try:
        schema = get_location_schema()
        return spark.read.option("header", "true").schema(schema).csv(file_path)
    except Exception as e:
        logger.exception(f"{'#'*40} Error read location data. {e}")
        return None

def process_traffic_data(df: DataFrame) -> Optional[DataFrame]:
    """
    Chains all transformations to flatten the XML structure into a DataFrame.
    """
    try:
        return df.select(
            col("date"),
            explode(col("periods.period")).alias("period_details")
        ).select(
            col("date"),
            col("period_details.period_from").alias("time_from"),
            col("period_details.period_to").alias("time_to"),
            explode(col("period_details.detectors.detector")).alias("detector_details")
        ).withColumn("clean_date", regexp_extract(col("date"), r"(\d{4}-\d{2}-\d{2})", 1)) \
        .withColumn("clean_time_from", regexp_extract(col("time_from"), r"(\d{2}:\d{2}:\d{2})", 0)) \
        .withColumn("clean_time_to", regexp_extract(col("time_to"), r"(\d{2}:\d{2}:\d{2})", 0)) \
        .withColumn("period_from", to_timestamp(concat(col("clean_date"), lit(" "), col("clean_time_from")))) \
        .withColumn("period_to", to_timestamp(concat(col("clean_date"), lit(" "), col("clean_time_to")))) \
        .select(
            col("date"),
            col("period_from"),
            col("period_to"),
            col("detector_details.detector_id"),
            col("detector_details.direction"),
            explode(col("detector_details.lanes.lane")).alias("lane_details")
        ).select(
            col("detector_id"),
            col("direction"),
            col("lane_details.lane_id"),
            col("lane_details.occupancy"),
            col("lane_details.speed"),
            col("lane_details.valid"),
            col("lane_details.volume"),
            col("lane_details.`s.d.`").alias("standard_deviation"),
            col("period_from"),
            col("period_to"),
            col("date")
        ).filter(col("valid") == 'Y').drop("valid")
    except Exception as e:
        logger.exception(f"{'#'*40} Error processing traffic data. {e}")
        return None

    
def main(dir:str = "202508", geoLocationPath: str = GEOLOCATION_PATH):
    HDFS_OUTPUT_PATH = f"hdfs:///traffic_data_partitioned/{dir.split('/')[-1]}"

    spark = initialize_spark()
    logger.info(f"{'#'*40} Spark Session initialized with memory-efficient configuration")
    
    road_data = XML_reader(dir,spark)
    if road_data is None:
        return 
    logger.info(f"{'#'*40} XML files loaded successfully")
    road_data.printSchema()
    
    logger.info(f"{'#'*40} Loaded {road_data.count()} root records (one per XML file)")
    
    logger.info(f"{'#'*40} Flattening XML structure and processing traffic data...")
    traffic_df = process_traffic_data(road_data)
    if traffic_df is None:
        return
    logger.info(f"{'#'*40} Successfully processed traffic records from XML files")
    
    geolocation_df = read_location_data(geoLocationPath, spark)
    if geolocation_df is None:
        return
    # merge Central and Western , Central & Western to Central and Western
    geolocation_df = geolocation_df.withColumn('District', when(col("District") == "Central & Western", "Central and Western").otherwise(col("District")))
    logger.info(f"{'#'*40} Location data contains {geolocation_df.count()} records")
    
    # Merge traffic data with location data using a broadcast join
    try:
        merged_df = traffic_df.join(
            broadcast(geolocation_df),
            traffic_df["detector_id"] == geolocation_df["AID_ID_Number"],
            "left"
        ).select(
            traffic_df["detector_id"],
            traffic_df["direction"],
            traffic_df["lane_id"],
            traffic_df["occupancy"],
            traffic_df["speed"],
            traffic_df["volume"],
            traffic_df["standard_deviation"],
            traffic_df["period_from"],
            traffic_df["period_to"],
            traffic_df["date"],
            geolocation_df["District"],
            geolocation_df["Road_EN"],
            geolocation_df["Rotation"],
            geolocation_df["GeometryEasting"],
            geolocation_df["GeometryNorthing"]
        )
    except Exception as e:
        logger.exception(f"{'#'*40} Error merge traffic and location data. {e}")
        return

    # Cache the merged DataFrame for performance
    merged_df.cache()
    
    # Trigger caching and get count for logging. This action is now efficient.
    merged_count = merged_df.count()
    logger.info((f"{'#'*40} Merged data contains {merged_count:,} records"))
    logger.info(f"Sample merged data: ")
    merged_df.show(10,truncate=False)
    merged_df.printSchema()
    
    logger.info(f"{'#'*40} Output location: HDFS: {HDFS_OUTPUT_PATH}")
    logger.info(f"{'#'*40} Saving to HDFS")

    N_OUTPUT_FILES = 8
    logger.info(f"{'#'*40} Coalescing to {N_OUTPUT_FILES} output files.")
    try:
        merged_df.coalesce(N_OUTPUT_FILES)\
        .write.mode("append")\
            .option("compression","snappy")\
                    .parquet(HDFS_OUTPUT_PATH)
    except Exception as e:
        logger.exception(f"{'#'*40} Error write dataframe to HDFS. {e}")
        return
    logger.info(f"{'#'*40} HDFS: Data saved (Parquet, Snappy compressed, {N_OUTPUT_FILES} files).")
    
    
    logger.info(f"{'#'*40} Data processing completed successfully.")
    logger.info(f"{'#'*40} Processing Statistics (from cached data):")
    logger.info(f"{'#'*80}")
    logger.info(f"{'#'*40} Total records processed: {merged_count:,} records")
    logger.info(f"{'#'*40} Unique detectors: {merged_df.select('detector_id').distinct().count()}")
    logger.info(f"{'#'*40} Date range: ")
    merged_df.select("date").distinct().orderBy("date").show()
    
    merged_df.unpersist() # Release the cache
    spark.stop()
    logger.info(f"{'#'*40} Spark Session stopped.")
    logger.info("PROCESSING COMPLETED SUCCESSFULLY!")
    
    
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        input_dir = sys.argv[1]

        # Geolocation path is now hardcoded but can be passed as an argument if needed
        main(dir=input_dir)
    else:
        # Default run for development/testing
        main()