from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, regexp_extract, to_timestamp, concat, lit, when
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
        .appName("Traffic XML to CSV - Memory Efficient") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .getOrCreate()


# read xml files from local storage
def XML_reader(local_dir:str, spark: SparkSession) -> Optional[DataFrame]:
    #xml_files = [os.path.join(local_dir,f) for f in os.listdir(local_dir) if f.endswith('.xml')]
    #xml_paths = [f"file://{os.path.abspath(f)}" for f in xml_files]
    #logger.info(f"{'#'*40} Reading XML files from directory: {xml_paths}")
    #logger.info(f"{'#'*40}, Found {len(xml_paths)} XML files in directory: {local_dir}")
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
        
def explode_periods(df:DataFrame) -> Optional[DataFrame]:
    try:
        return df.select(
            col("date"),
            explode(col("periods.period")).alias("period_details")
        )
    except Exception as e:
        logger.exception(f"{'#'*40} Error explode periods. {e}")
        return None

def explode_detector(df:DataFrame) -> Optional[DataFrame]:
    try:
        return df.select(
        col("date"),
        #col("source_file"),
        col("period_details.period_from").alias("time_from"),
        col("period_details.period_to").alias("time_to"),
        explode(col("period_details.detectors.detector")).alias("detector_details")
    ) \
    .withColumn("clean_date", regexp_extract(col("date"), r"(\d{4}-\d{2}-\d{2})", 1)) \
    .withColumn("clean_time_from", regexp_extract(col("time_from"), r"(\d{2}:\d{2}:\d{2})", 0)) \
    .withColumn("clean_time_to", regexp_extract(col("time_to"), r"(\d{2}:\d{2}:\d{2})", 0)) \
    .withColumn("period_from", to_timestamp(concat(col("clean_date"), lit(" "), col("clean_time_from")))) \
    .withColumn("period_to", to_timestamp(concat(col("clean_date"), lit(" "), col("clean_time_to")))) \
    .drop("clean_date", "clean_time_from", "clean_time_to")
    except Exception as e:
        logger.exception(f"{'#'*40} Error explode detector records. {e}")
        return None
    
def explode_lane(df:DataFrame) -> Optional[DataFrame]:
    try:
        return df.select(
        col("date"),
        col("period_from"),
        col("period_to"),
        col("detector_details.detector_id"),
        col("detector_details.direction"),
        explode(col("detector_details.lanes.lane")).alias("lane_details")
        )
    except Exception as e:
        logger.exception(f"{'#'*40} Error explode lane records. {e}")
        return None

def explode_final(df:DataFrame) -> Optional[DataFrame]:
    try:
        return df.select(
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
        )
    except Exception as e:
        logger.exception(f"{'#'*40} Error explode to final dataframe {e}")
        return None

def read_location_data(file_path: str, spark:SparkSession) -> Optional[DataFrame]:
    try:
        return spark.read.option("header", "true").option("inferSchema","true").csv(file_path)
    except Exception as e:
        logger.exception(f"{'#'*40} Error read location data. {e}")
        return None

    
    
def main(dir:str = "202508", geoLocationPath: str = GEOLOCATION_PATH):
    HDFS_OUTPUT_PATH = f"hdfs:///traffic_data_partitioned/{dir.split('/')[-1]}"

    spark = initialize_spark()
    logger.info(f"{'#'*40} Spark Session initialized with memory-efficient configuration")
    
    road_data = XML_reader(dir,spark)
    if road_data == None:
        return 
    logger.info(f"{'#'*40} XML files loaded successfully")
    logger.info(f"{'#'*40} Schema:")
    road_data.printSchema()
    
    road_data_count = road_data.count()
    logger.info(f"{'#'*40} Loaded {road_data_count} root records (one per XML file)")
    
    logger.info(f"{'#'*40} Flattening XML structure...")
    
    periods_df = explode_periods(road_data)
    if periods_df == None:
        return
    logger.info(f"{'#'*40} Successfully explode Periods records")
    
    
    detector_df = explode_detector(periods_df)
    if detector_df == None:
        return
    logger.info(f"{'#'*40} Successfully explode Detector records")

    lane_df = explode_lane(detector_df)
    if lane_df == None:
        return
    logger.info(f"{'#'*40} Successfully explode Lane records")
    
    final_df = explode_final(lane_df)
    if final_df == None:
        return 
    logger.info(f"{'#'*40} Successfully explode Final records")
    
    traffic_df = final_df.filter(col("valid") == 'Y').drop("valid")
    logger.info(f"{'#'*40} Drop invalid data and Drop valid column.")
    
    logger.info(f"{'#'*40} Processing traffic data...")
    traffic_count = traffic_df.count()
    logger.info(f"{'#'*40} Successfully processed {traffic_count:,} traffic records from XML files")
    logger.info(f"{'#'*40} Sample data:")
    traffic_df.show(10, truncate=False)
    geolocation_df = read_location_data(geoLocationPath, spark)
    if geolocation_df == None:
        return
    # merge Central and Western , Central & Western to Central and Western
    geolocation_df = geolocation_df.withColumn('District', when(col("District") == "Central & Western", "Central and Western").otherwise(col("District")))
    logger.info(f"{'#'*40} Location data contains {geolocation_df.count()} records")
    
    # Merge traffic data with location data
    try:
        merged_df = traffic_df.join(
        geolocation_df,
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

    logger.info((f"{'#'*40} Merged data contains {merged_df.count():,} records"))
    logger.info(f"Sample merged data: ")
    merged_df.show(10,truncate=False)
    merged_df.printSchema()
    
    logger.info(f"{'#'*40} Output location: HDFS: {HDFS_OUTPUT_PATH}")
    logger.info(f"{'#'*40} Saving to HDFS")

    '''
    estimsate 40 MB / day, the whole month is 40MB * 30 = 1200MB; divide to 128mb is 1200 /128 ~= 10; so the N_output_files = 10
    actually total 800MB after compression, so 8 files is good enough
    '''
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
    logger.info(f"{'#'*40} Processing Statistics:")
    logger.info(f"{'#'*80}")
    merged_df.cache()
    logger.info(f"{'#'*40} Total records processed: {merged_df.count():,} records")
    logger.info(f"{'#'*40} Unique detectors: {merged_df.select('detector_id').distinct().count()}")
    logger.info(f"{'#'*40} Date range: ")
    merged_df.select("date").distinct().orderBy("date").show()
    spark.stop()
    logger.info(f"{'#'*40} Spark Session stopped.")
    logger.info("PROCESSING COMPLETED SUCCESSFULLY!")
    
    
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        input_dir = sys.argv[1]
        geolocation = sys.argv[2] if len(sys.argv) > 2 else None
        main(dir=input_dir, geoLocationPath=GEOLOCATION_PATH)
    else:
        main()