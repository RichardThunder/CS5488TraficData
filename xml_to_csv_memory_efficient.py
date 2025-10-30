"""
Memory-efficient PySpark script for XML to CSV conversion
Optimized for 32GB RAM, 6-core system
Avoids caching large datasets
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, input_file_name, regexp_extract, to_timestamp, concat, lit
import os

def main(dir="202508"):
    # Initialize Spark Session with optimized memory settings
    spark = SparkSession.builder \
        .appName("Traffic XML to CSV - Memory Efficient") \
        .config("spark.driver.memory", "20g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.2") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .getOrCreate()

    print("Spark Session initialized with memory-efficient configuration")
    print(f"Driver Memory: 20GB, Executor Memory: 8GB")

    # Read all XML files
    print(f"Reading XML files from {dir}...")
    xml_files = [os.path.join(dir, f) for f in os.listdir(dir) if f.endswith('.xml')]
    print(f"Found {len(xml_files)} XML files")

    xml_paths = [f"file://{os.path.abspath(f)}" for f in xml_files]

    print(f"Loading and processing XML files (no caching to save memory)...")

    # Read XML without caching - let Spark manage memory
    road_data = spark.read.format("xml") \
        .option("rowTag", "raw_speed_volume_list") \
        .option("inferSchema", "false") \
        .option("valueTag", "_VALUE") \
        .option("attributePrefix", "_") \
        .option("charset", "UTF-8") \
        .option("mode", "DROPMALFORMED") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .load(xml_paths) \
        .withColumn("source_file", input_file_name())

    print("XML files loaded successfully")
    print("Schema:")
    road_data.printSchema()

    # Count root records
    road_data_count = road_data.count()
    print(f"Loaded {road_data_count} root records (one per XML file)")

    # Process data without intermediate caching
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

    print(f"Processing traffic data...")
    traffic_count = traffic_df.count()
    print(f"Successfully processed {traffic_count:,} traffic records from XML files")

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

    print(f"Merged data contains {merged_df.count():,} records")
    print("Sample merged data:")
    merged_df.show(10, truncate=False)

    merged_df.printSchema()

    # Setup output paths - local filesystem and HDFS
    base_dir = os.path.abspath(".")
    local_output_path = f"file://{base_dir}/traffic_data_partitioned"
    hdfs_output_path = "hdfs:///user/richard/traffic_data_partitioned"

    print(f"\nOutput locations:")
    print(f"  Local: {base_dir}/traffic_data_partitioned/")
    print(f"  HDFS: {hdfs_output_path}")

    # Save to local filesystem first
    print(f"\n[1/2] Saving to LOCAL filesystem...")
    print("Using Snappy compression for faster I/O...")
    print("Merging files per date (one CSV per date)...")

    # Repartition to 1 partition per date, then partition by date
    merged_df.repartition(1, "date") \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("date") \
        .csv(local_output_path, header=True)
    print(f"✓ Local filesystem: Data saved (Snappy compressed, 1 file per date)")

    # Get local size
    import subprocess
    try:
        result = subprocess.run(['du', '-sh', local_output_path.replace("file://", "")],
                              capture_output=True, text=True)
        if result.returncode == 0:
            size = result.stdout.split()[0]
            print(f"  Local size: {size}")
    except:
        pass

    # Save to HDFS
    print(f"\n[2/2] Saving to HDFS...")
    print(f"Writing to {hdfs_output_path}...")

    # Use the same data, repartition and save to HDFS
    merged_df.repartition(1, "date") \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("date") \
        .csv(hdfs_output_path, header=True)
    print(f"✓ HDFS: Data saved (Snappy compressed, 1 file per date)")

    # Get HDFS size
    try:
        result = subprocess.run(['hdfs', 'dfs', '-du', '-s', '-h', hdfs_output_path],
                              capture_output=True, text=True)
        if result.returncode == 0:
            size_line = result.stdout.strip()
            if size_line:
                size = size_line.split()[0]
                print(f"  HDFS size: {size}")
    except:
        pass

    # Print statistics
    print("\n" + "="*80)
    print("PROCESSING STATISTICS")
    print("="*80)
    record_count = merged_df.count()
    print(f"Total records: {record_count:,}")
    print(f"Unique detectors: {merged_df.select('detector_id').distinct().count()}")
    print("\nDate range:")
    merged_df.select("date").distinct().orderBy("date").show()

    # Stop Spark session
    spark.stop()

    print("\n" + "="*80)
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print("="*80)
    print("\nOutput saved to TWO locations:")
    print(f"\n1. LOCAL FILESYSTEM:")
    print(f"   Path: {base_dir}/traffic_data_partitioned/")
    print(f"   - Snappy compressed (.snappy)")
    print(f"   - ONE file per date (consolidated)")
    print(f"   - Use: spark.read.csv('traffic_data_partitioned', header=True)")
    print(f"\n2. HDFS:")
    print(f"   Path: {hdfs_output_path}")
    print(f"   - Snappy compressed (.snappy)")
    print(f"   - ONE file per date (consolidated)")
    print(f"   - Use: spark.read.csv('{hdfs_output_path}', header=True)")
    print(f"   - View: hdfs dfs -ls {hdfs_output_path}")
    print("\n" + "="*80)

if __name__ == "__main__":
    main()
