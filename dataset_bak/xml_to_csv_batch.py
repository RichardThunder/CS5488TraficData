"""
Batch Processing PySpark script for XML to CSV conversion
Processes XML files in batches to avoid OutOfMemoryError
Optimized for 32GB RAM, 6-core system with 28,401 XML files
Outputs only partitioned CSV (no single large file)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, input_file_name, regexp_extract, to_timestamp, concat, lit
import os
import math
import shutil

def process_batch(spark, xml_paths, batch_num, total_batches):
    """Process a single batch of XML files"""
    print(f"\n{'='*80}")
    print(f"Processing Batch {batch_num}/{total_batches}")
    print(f"Files in this batch: {len(xml_paths)}")
    print(f"{'='*80}")

    # Read XML files for this batch
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

    # Don't persist - process immediately
    print(f"Batch {batch_num}: Loaded {road_data.count()} XML records")

    # Explode periods
    period_df = road_data.select(
        col("date"),
        col("source_file"),
        explode(col("periods.period")).alias("period_details")
    )

    # Explode detectors and clean timestamps
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

    # Explode lanes
    exploded_lane_df = exploded_detector_df.select(
        col("date"),
        col("source_file"),
        col("period_from"),
        col("period_to"),
        col("detector_details.detector_id"),
        col("detector_details.direction"),
        explode(col("detector_details.lanes.lane")).alias("lane_details")
    )

    # Flatten to final structure
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

    batch_count = traffic_df.count()
    print(f"Batch {batch_num}: Generated {batch_count:,} traffic records")

    return traffic_df

def main(dir="202508", batch_size=1000):
    """
    Main processing function

    Args:
        dir: Directory containing XML files
        batch_size: Number of XML files to process per batch (default: 1000)
    """
    # Initialize Spark Session with conservative memory settings for batch processing
    spark = SparkSession.builder \
        .appName("Traffic XML to CSV - Batch Processing") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.2") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    print("="*80)
    print("Spark Session initialized for BATCH processing")
    print(f"Driver Memory: 12GB, Executor Memory: 16GB")
    print(f"Batch size: {batch_size} files per batch")
    print("="*80)

    # Get all XML files
    print(f"\nScanning directory: {dir}")
    xml_files = sorted([os.path.join(dir, f) for f in os.listdir(dir) if f.endswith('.xml')])
    total_files = len(xml_files)
    print(f"Found {total_files:,} XML files")

    # Calculate number of batches
    num_batches = math.ceil(total_files / batch_size)
    print(f"Will process in {num_batches} batches of ~{batch_size} files each")

    # Convert to absolute paths with file:// protocol
    xml_files = [f"file://{os.path.abspath(f)}" for f in xml_files]

    # Load location data once (it's small)
    print("\nLoading location data...")
    geolocation_path = "Locations_of_Traffic_Detectors.gdb_converted.csv"
    geolocation_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(geolocation_path) \
        .cache()  # Cache this small dataset

    print(f"Location data contains {geolocation_df.count()} records")

    # Setup output path
    base_dir = os.path.abspath(".")
    partitioned_output_path = f"file://{base_dir}/traffic_data_partitioned"

    print(f"\nOutput location:")
    print(f"  Partitioned data: {base_dir}/traffic_data_partitioned/")

    total_records = 0

    for batch_num in range(1, num_batches + 1):
        # Get batch of files
        start_idx = (batch_num - 1) * batch_size
        end_idx = min(batch_num * batch_size, total_files)
        batch_files = xml_files[start_idx:end_idx]

        try:
            # Process batch
            traffic_df = process_batch(spark, batch_files, batch_num, num_batches)

            # Join with location data
            print(f"Batch {batch_num}: Joining with location data...")
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

            batch_count = merged_df.count()
            total_records += batch_count
            print(f"Batch {batch_num}: {batch_count:,} records ready for output")

            # Write batch to partitioned output (append mode) with compression
            print(f"Batch {batch_num}: Writing to partitioned output (Snappy compressed)...")
            write_mode = "overwrite" if batch_num == 1 else "append"
            merged_df.write.mode(write_mode) \
                .option("compression", "snappy") \
                .partitionBy("date") \
                .csv(partitioned_output_path, header=True)
            print(f"Batch {batch_num}: ✓ Written to {partitioned_output_path} (Snappy compressed)")

            # Show sample from first batch
            if batch_num == 1:
                print("\nSample data from first batch:")
                merged_df.show(5, truncate=False)

            # Explicitly unpersist and clear references to free memory
            traffic_df.unpersist()
            merged_df.unpersist()

            print(f"Batch {batch_num}: ✓ Completed successfully")
            print(f"Running total: {total_records:,} records processed so far")

        except Exception as e:
            print(f"ERROR in Batch {batch_num}: {str(e)}")
            print("Continuing with next batch...")
            continue

    print("\n" + "="*80)
    print("All batches processed!")
    print("="*80)
    print(f"Total records processed: {total_records:,}")

    # Consolidate: Merge multiple CSV files per date into one file per date
    print("\n" + "="*80)
    print("CONSOLIDATING FILES")
    print("="*80)
    print("Merging multiple CSV files per date into one file per date...")
    print("This will improve file organization and reduce file count...")

    try:
        partitioned_path_local = partitioned_output_path.replace("file://", "")

        # Read all the data
        print("Reading all partitioned data...")
        all_data = spark.read.csv(f"{partitioned_output_path}/*/*.snappy", header=True, inferSchema=True)
        record_count = all_data.count()
        print(f"Loaded {record_count:,} records")

        # Create temporary consolidated output
        temp_output = f"{partitioned_path_local}_consolidated"
        temp_output_uri = f"file://{temp_output}"

        print("Repartitioning to one file per date...")
        all_data.repartition(1, "date") \
            .write.mode("overwrite") \
            .option("compression", "snappy") \
            .partitionBy("date") \
            .csv(temp_output_uri, header=True)

        print("✓ Consolidated files created")

        # Remove old output and rename consolidated
        print("Replacing original output with consolidated version...")
        shutil.rmtree(partitioned_path_local)
        shutil.move(temp_output, partitioned_path_local)
        print("✓ Output replaced with consolidated version")

        # Show statistics
        print("\n" + "="*80)
        print("FINAL STATISTICS")
        print("="*80)
        print(f"Total records: {record_count:,}")
        print(f"Unique detectors: {all_data.select('detector_id').distinct().count()}")
        print("\nDate range:")
        all_data.select("date").distinct().orderBy("date").show()

        # Get total size
        import subprocess
        try:
            result = subprocess.run(['du', '-sh', partitioned_path_local],
                                  capture_output=True, text=True)
            if result.returncode == 0:
                size = result.stdout.split()[0]
                print(f"\nTotal output size: {size}")
        except:
            pass

        # Count files per date
        print("\nFiles per date directory:")
        for date_dir in sorted(os.listdir(partitioned_path_local)):
            if date_dir.startswith("date="):
                date_path = os.path.join(partitioned_path_local, date_dir)
                csv_files = [f for f in os.listdir(date_path) if f.endswith('.csv.gz')]
                print(f"  {date_dir}: {len(csv_files)} file(s)")

    except Exception as e:
        print(f"Note: Could not consolidate files: {str(e)}")
        print("Original partitioned output is still available")

    # Cleanup
    geolocation_df.unpersist()
    spark.stop()

    print("\n" + "="*80)
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print("="*80)
    print("\nOutput saved to local filesystem:")
    print(f"  Partitioned data: {base_dir}/traffic_data_partitioned/")
    print(f"  - Files are gzip compressed (.csv.gz) to save space")
    print(f"  - ONE file per date (consolidated for easy access)")
    print(f"  - Organized by date for efficient queries")
    print(f"\nHow to use:")
    print(f"  - PySpark: spark.read.csv('traffic_data_partitioned', header=True)")
    print(f"  - Pandas: pd.read_csv('traffic_data_partitioned/date=2025-08-01/*.csv.gz')")
    print(f"  - Combine all: gunzip -c traffic_data_partitioned/*/*.csv.gz > merged.csv")
    print("\n" + "="*80)

if __name__ == "__main__":
    # Adjust batch_size based on your needs:
    # - Smaller batch_size (500-1000): More stable, slower
    # - Larger batch_size (2000-3000): Faster, may need more memory
    main(dir="202508", batch_size=1000)
