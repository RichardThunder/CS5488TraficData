"""
Alternative PySpark script using spark-xml library for better performance
Requires: pip install spark-xml OR include com.databricks:spark-xml package

This version is faster for very large XML files
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, input_file_name, regexp_extract, to_timestamp, concat, lit
from pyspark import StorageLevel
import os

def main(dir = "202508"):
    # Initialize Spark Session
    # PySpark 4.0.1+ has built-in XML support - no external package needed!
    # Optimized for 32GB RAM system with 6 cores
    spark = SparkSession.builder \
        .appName("Traffic XML to CSV Converter - Built-in XML") \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.memory", "12g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    print("Spark Session initialized with built-in XML support")

    # Read all XML files using built-in XML data source
    print(f"Reading XML files from {dir}...")

    # Get all XML files explicitly (wildcards don't work with file:// protocol)
    xml_files = [os.path.join(dir, f) for f in os.listdir(dir) if f.endswith('.xml')]
    print(f"Found {len(xml_files)} XML files")

    # Convert to absolute paths with file:// protocol
    xml_paths = [f"file://{os.path.abspath(f)}" for f in xml_files]

    print(f"Loading XML files...")
    road_data = spark.read.format("xml") \
        .option("rowTag", "raw_speed_volume_list") \
        .option("inferSchema", "false") \
        .option("valueTag", "_VALUE") \
        .option("attributePrefix", "_") \
        .option("charset", "UTF-8") \
        .option("mode", "DROPMALFORMED") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .load(xml_paths) \
        .withColumn("source_file", input_file_name()) \
        .persist(StorageLevel.MEMORY_AND_DISK_DESER)

    print("XML files loaded successfully")
    print("Schema:")
    road_data.printSchema()

    # Count to force caching and check for issues early
    road_data_count = road_data.count()
    print(f"Loaded {road_data_count} root records (one per XML file)")

    # Explode the nested structure
    print("Flattening XML structure...")

    # Step 1: Explode periods
    period_df = road_data.select(
        col("date"),
        col("source_file"),
        explode(col("periods.period")).alias("period_details")
    )

    # Step 2: Explode detectors and clean timestamps
    exploded_detector_df = period_df.select(
        col("date"),
        col("source_file"),
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

    # Step 3: Explode lanes
    exploded_lane_df = exploded_detector_df.select(
        col("date"),
        col("source_file"),
        col("period_from"),
        col("period_to"),
        col("detector_details.detector_id"),
        col("detector_details.direction"),
        explode(col("detector_details.lanes.lane")).alias("lane_details")
    )

    # Step 4: Flatten to final structure
    traffic_df = exploded_lane_df.select(
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
        col("date"),
        col("source_file")
    )

    # Don't cache traffic_df to save memory - we'll only use it once for the join
    print(f"Successfully processed {traffic_df.count()} records from XML files")
    print("Sample data:")
    traffic_df.show(10, truncate=False)

    # Read location data
    print("Reading location data...")
    geolocation_path = "Locations_of_Traffic_Detectors.gdb_converted.csv"
    geolocation_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(geolocation_path)

    print(f"Location data contains {geolocation_df.count()} records")

    # Merge traffic data with location data
    print("Merging traffic data with location data...")
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
        traffic_df["valid"],
        traffic_df["volume"],
        traffic_df["standard_deviation"],
        traffic_df["period_from"],
        traffic_df["period_to"],
        traffic_df["date"],
        geolocation_df["District"],
        geolocation_df["Road_EN"],
        geolocation_df["Road_TC"],
        geolocation_df["Road_SC"],
        geolocation_df["Rotation"],
        geolocation_df["GeometryEasting"],
        geolocation_df["GeometryNorthing"]
    )

    print(f"Merged data contains {merged_df.count()} records")
    print("Sample merged data:")
    merged_df.show(10, truncate=False)

    # Save to CSV - single file
    output_path = "traffic_data_merged.csv"
    print(f"Saving merged data to {output_path}...")
    merged_df.coalesce(1).write.mode("overwrite").csv(
        output_path,
        header=True
    )

    print(f"Data successfully saved to {output_path}")

    # Save partitioned by date for better query performance
    partitioned_output_path = "traffic_data_partitioned"
    print(f"Saving partitioned data to {partitioned_output_path}...")
    merged_df.write.mode("overwrite").partitionBy("date").csv(
        partitioned_output_path,
        header=True
    )

    print(f"Partitioned data successfully saved to {partitioned_output_path}")

    # Print statistics
    print("\n=== Processing Statistics ===")
    print(f"Total records: {merged_df.count()}")
    print(f"Unique detectors: {merged_df.select('detector_id').distinct().count()}")
    print(f"Date range:")
    merged_df.select("date").distinct().show()

    # Stop Spark session
    spark.stop()
    print("Processing completed successfully!")

if __name__ == "__main__":
    main()
