#!/usr/bin/env python3
"""
Descriptive Statistics Analysis for Hong Kong Traffic Data (Full Year)
Implements Section 1.2 from analysis.md

This script computes comprehensive descriptive statistics including:
- Data validity checks
- Overall statistics (mean, median, std, percentiles)
- Breakdowns by: District, Road, Lane, Hour, Day of Week
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, min as _min, max as _max,
    stddev, percentile_approx, when, countDistinct,
    hour, dayofweek, date_format, concat_ws
)
from pyspark.sql.types import DoubleType, IntegerType
import sys

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("Traffic Descriptive Statistics - Full Year") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.files.ignoreCorruptFiles", "true") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def load_data(spark, data_path):
    """Load partitioned traffic data from HDFS"""
    print(f"Loading data from {data_path}...")

    try:
        # Read CSV files - let Spark auto-detect compression
        # Use mode PERMISSIVE to handle corrupt records
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .csv(data_path)

        # Show schema
        print("\n=== Data Schema ===")
        df.printSchema()

        # Check for corrupt records
        if "_corrupt_record" in df.columns:
            corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
            print(f"\nWarning: Found {corrupt_count} corrupt records (will be excluded)")
            df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

        # Show sample
        print("\n=== Sample Data (first 5 rows) ===")
        df.show(5, truncate=False)

        return df

    except Exception as e:
        print(f"\nError loading data: {e}")
        raise

def data_validity_check(df):
    """1.1 Data Quality Assessment"""
    print("\n" + "="*80)
    print("1. DATA QUALITY ASSESSMENT")
    print("="*80)

    # Total records
    total_records = df.count()
    print(f"\nTotal Records: {total_records:,}")

    # Validity check
    print("\n--- Validity Distribution ---")
    validity_stats = df.groupBy('valid').count() \
        .withColumn('percentage', (col('count') / total_records * 100))
    validity_stats.show()

    # Missing/null values check
    print("\n--- Missing/Null Values ---")
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])
    null_counts.show(vertical=True)

    # Date coverage
    print("\n--- Temporal Coverage ---")
    df.select(_min('date').alias('Start_Date'),
              _max('date').alias('End_Date'),
              countDistinct('date').alias('Unique_Days')).show()

    # Spatial coverage
    print("\n--- Spatial Coverage ---")
    df.select(
        countDistinct('detector_id').alias('Unique_Detectors'),
        countDistinct('District').alias('Unique_Districts'),
        countDistinct('Road_EN').alias('Unique_Roads')
    ).show()

    return df.filter(col('valid') == 'Y')

def overall_statistics(df):
    """1.2 Overall Descriptive Statistics for Key Metrics"""
    print("\n" + "="*80)
    print("2. OVERALL DESCRIPTIVE STATISTICS")
    print("="*80)

    # Convert string columns to numeric if needed
    df_numeric = df.withColumn('speed', col('speed').cast(DoubleType())) \
                   .withColumn('volume', col('volume').cast(IntegerType())) \
                   .withColumn('occupancy', col('occupancy').cast(DoubleType())) \
                   .withColumn('sd', col('standard_deviation').cast(DoubleType()))

    # Basic statistics
    print("\n--- Speed (km/h) ---")
    df_numeric.select(
        avg('speed').alias('Mean'),
        percentile_approx('speed', 0.5).alias('Median'),
        stddev('speed').alias('Std_Dev'),
        _min('speed').alias('Min'),
        _max('speed').alias('Max'),
        percentile_approx('speed', 0.25).alias('25th_Percentile'),
        percentile_approx('speed', 0.75).alias('75th_Percentile'),
        percentile_approx('speed', 0.95).alias('95th_Percentile')
    ).show()

    print("\n--- Volume (vehicles) ---")
    df_numeric.select(
        avg('volume').alias('Mean'),
        percentile_approx('volume', 0.5).alias('Median'),
        stddev('volume').alias('Std_Dev'),
        _min('volume').alias('Min'),
        _max('volume').alias('Max'),
        percentile_approx('volume', 0.25).alias('25th_Percentile'),
        percentile_approx('volume', 0.75).alias('75th_Percentile'),
        percentile_approx('volume', 0.95).alias('95th_Percentile')
    ).show()

    print("\n--- Occupancy (%) ---")
    df_numeric.select(
        avg('occupancy').alias('Mean'),
        percentile_approx('occupancy', 0.5).alias('Median'),
        stddev('occupancy').alias('Std_Dev'),
        _min('occupancy').alias('Min'),
        _max('occupancy').alias('Max'),
        percentile_approx('occupancy', 0.25).alias('25th_Percentile'),
        percentile_approx('occupancy', 0.75).alias('75th_Percentile'),
        percentile_approx('occupancy', 0.95).alias('95th_Percentile')
    ).show()

    print("\n--- Speed Standard Deviation (Flow Stability) ---")
    df_numeric.select(
        avg('sd').alias('Mean'),
        percentile_approx('sd', 0.5).alias('Median'),
        stddev('sd').alias('Std_Dev'),
        _min('sd').alias('Min'),
        _max('sd').alias('Max')
    ).show()

    return df_numeric

def statistics_by_district(df):
    """Statistics broken down by District"""
    print("\n" + "="*80)
    print("3. STATISTICS BY DISTRICT")
    print("="*80)

    district_stats = df.groupBy('District').agg(
        count('*').alias('Records'),
        avg('speed').alias('Avg_Speed'),
        avg('volume').alias('Avg_Volume'),
        avg('occupancy').alias('Avg_Occupancy'),
        _sum('volume').alias('Total_Volume')
    ).orderBy(col('Avg_Speed').asc())

    print("\n--- Districts Ranked by Average Speed (Slowest to Fastest) ---")
    district_stats.show(50, truncate=False)

    return district_stats

def statistics_by_road(df):
    """Statistics broken down by Road"""
    print("\n" + "="*80)
    print("4. STATISTICS BY ROAD")
    print("="*80)

    road_stats = df.groupBy('Road_EN', 'District').agg(
        count('*').alias('Records'),
        avg('speed').alias('Avg_Speed'),
        avg('volume').alias('Avg_Volume'),
        avg('occupancy').alias('Avg_Occupancy'),
        _sum('volume').alias('Total_Volume'),
        countDistinct('detector_id').alias('Num_Detectors')
    )

    print("\n--- Top 20 Slowest Roads (Most Congested) ---")
    road_stats.orderBy('Avg_Speed').show(20, truncate=False)

    print("\n--- Top 20 Busiest Roads (Highest Total Volume) ---")
    road_stats.orderBy(col('Total_Volume').desc()).show(20, truncate=False)

    print("\n--- Top 20 Fastest Roads ---")
    road_stats.orderBy(col('Avg_Speed').desc()).show(20, truncate=False)

    return road_stats

def statistics_by_lane(df):
    """Statistics broken down by Lane Type"""
    print("\n" + "="*80)
    print("5. STATISTICS BY LANE TYPE")
    print("="*80)

    lane_stats = df.groupBy('lane_id').agg(
        count('*').alias('Records'),
        avg('speed').alias('Avg_Speed'),
        avg('volume').alias('Avg_Volume'),
        avg('occupancy').alias('Avg_Occupancy'),
        _sum('volume').alias('Total_Volume')
    ).orderBy('lane_id')

    print("\n--- Lane Comparison ---")
    lane_stats.show(truncate=False)

    return lane_stats

def statistics_by_hour(df):
    """Statistics broken down by Hour of Day"""
    print("\n" + "="*80)
    print("6. STATISTICS BY HOUR OF DAY")
    print("="*80)

    # Extract hour from period_from
    df_with_hour = df.withColumn('hour', col('period_from').substr(1, 2).cast(IntegerType()))

    hourly_stats = df_with_hour.groupBy('hour').agg(
        count('*').alias('Records'),
        avg('speed').alias('Avg_Speed'),
        avg('volume').alias('Avg_Volume'),
        avg('occupancy').alias('Avg_Occupancy'),
        _sum('volume').alias('Total_Volume')
    ).orderBy('hour')

    print("\n--- Hourly Traffic Pattern (24-hour cycle) ---")
    hourly_stats.show(24, truncate=False)

    return hourly_stats

def statistics_by_day_of_week(df):
    """Statistics broken down by Day of Week"""
    print("\n" + "="*80)
    print("7. STATISTICS BY DAY OF WEEK")
    print("="*80)

    # Create day of week (1=Sunday, 2=Monday, ..., 7=Saturday in Spark)
    df_with_dow = df.withColumn('day_of_week', dayofweek(col('date'))) \
                    .withColumn('day_name',
                        when(col('day_of_week') == 1, 'Sunday')
                        .when(col('day_of_week') == 2, 'Monday')
                        .when(col('day_of_week') == 3, 'Tuesday')
                        .when(col('day_of_week') == 4, 'Wednesday')
                        .when(col('day_of_week') == 5, 'Thursday')
                        .when(col('day_of_week') == 6, 'Friday')
                        .when(col('day_of_week') == 7, 'Saturday'))

    dow_stats = df_with_dow.groupBy('day_of_week', 'day_name').agg(
        count('*').alias('Records'),
        avg('speed').alias('Avg_Speed'),
        avg('volume').alias('Avg_Volume'),
        avg('occupancy').alias('Avg_Occupancy'),
        _sum('volume').alias('Total_Volume')
    ).orderBy('day_of_week')

    print("\n--- Day of Week Pattern ---")
    dow_stats.show(truncate=False)

    # Weekday vs Weekend comparison
    print("\n--- Weekday vs Weekend Comparison ---")
    weekday_weekend = df_with_dow.withColumn('is_weekend',
        when((col('day_of_week') == 1) | (col('day_of_week') == 7), 'Weekend')
        .otherwise('Weekday')
    )

    weekday_weekend.groupBy('is_weekend').agg(
        count('*').alias('Records'),
        avg('speed').alias('Avg_Speed'),
        avg('volume').alias('Avg_Volume'),
        avg('occupancy').alias('Avg_Occupancy')
    ).show(truncate=False)

    return dow_stats

def statistics_by_direction(df):
    """Statistics broken down by Traffic Direction"""
    print("\n" + "="*80)
    print("8. STATISTICS BY DIRECTION")
    print("="*80)

    direction_stats = df.groupBy('direction').agg(
        count('*').alias('Records'),
        avg('speed').alias('Avg_Speed'),
        avg('volume').alias('Avg_Volume'),
        avg('occupancy').alias('Avg_Occupancy'),
        _sum('volume').alias('Total_Volume')
    ).orderBy('direction')

    print("\n--- Direction Comparison ---")
    direction_stats.show(50, truncate=False)

    return direction_stats

def save_statistics_summary(spark, district_stats, road_stats, lane_stats,
                           hourly_stats, dow_stats, output_path):
    """Save all statistics to output files"""
    print("\n" + "="*80)
    print("SAVING STATISTICS SUMMARIES")
    print("="*80)

    try:
        # Save each summary
        print(f"\nSaving district statistics to {output_path}/district_stats/")
        district_stats.coalesce(1).write.mode("overwrite").csv(
            f"{output_path}/district_stats", header=True
        )

        print(f"Saving road statistics to {output_path}/road_stats/")
        road_stats.coalesce(1).write.mode("overwrite").csv(
            f"{output_path}/road_stats", header=True
        )

        print(f"Saving lane statistics to {output_path}/lane_stats/")
        lane_stats.coalesce(1).write.mode("overwrite").csv(
            f"{output_path}/lane_stats", header=True
        )

        print(f"Saving hourly statistics to {output_path}/hourly_stats/")
        hourly_stats.coalesce(1).write.mode("overwrite").csv(
            f"{output_path}/hourly_stats", header=True
        )

        print(f"Saving day-of-week statistics to {output_path}/dow_stats/")
        dow_stats.coalesce(1).write.mode("overwrite").csv(
            f"{output_path}/dow_stats", header=True
        )

        print("\n✓ All statistics saved successfully!")

    except Exception as e:
        print(f"\n✗ Error saving statistics: {e}")

def checkpoint_exists(spark, path):
    """Check if a checkpoint path exists"""
    try:
        # Try to access the path
        spark.read.parquet(path).limit(1).count()
        return True
    except:
        return False

def main():
    """Main execution function"""
    # Configuration
    DATA_PATH = "traffic_data_partitioned"
    OUTPUT_PATH = "traffic_statistics"
    CHECKPOINT_PATH = "traffic_checkpoints"
    VALID_DATA_PATH = f"{CHECKPOINT_PATH}/valid_data"

    print("="*80)
    print("HONG KONG TRAFFIC DATA - DESCRIPTIVE STATISTICS ANALYSIS")
    print("Full Year Analysis (with Checkpointing)")
    print("="*80)

    # Create Spark session
    spark = create_spark_session()

    try:
        # Step 1: Load and prepare valid data
        if checkpoint_exists(spark, VALID_DATA_PATH):
            print("\n[1/9] ✓ Checkpoint found! Loading valid data from checkpoint...")
            print(f"        Reading from: {VALID_DATA_PATH}")
            clean_df = spark.read.parquet(VALID_DATA_PATH)
            valid_count = clean_df.count()
            print(f"        Valid records loaded: {valid_count:,}")
        else:
            print("\n[1/9] Loading raw data...")
            df = load_data(spark, DATA_PATH)

            print("\n[2/9] Performing data quality assessment...")
            clean_df = data_validity_check(df)

            valid_count = clean_df.count()
            print(f"\nValid records: {valid_count:,}")

            if valid_count == 0:
                raise ValueError("No valid records found in dataset!")

            # Save valid dataset to Parquet for future runs
            print(f"\n[3/9] Saving valid dataset to Parquet for better read performance...")
            print(f"        Writing to: {VALID_DATA_PATH}")
            clean_df.write.mode("overwrite").parquet(VALID_DATA_PATH)
            print(f"        ✓ Valid dataset saved ({valid_count:,} records)")

            # Reload from Parquet for consistent performance
            clean_df = spark.read.parquet(VALID_DATA_PATH)

        # Cache clean data for better performance
        print("\n[4/9] Caching valid records for processing...")
        clean_df.cache()
        clean_df.count()  # Force cache

        # 2. Overall statistics
        print("\n[5/9] Computing overall statistics...")
        df_numeric = overall_statistics(clean_df)

        # 3-8. Breakdown statistics with checkpointing
        DISTRICT_CHECKPOINT = f"{CHECKPOINT_PATH}/district_stats"
        ROAD_CHECKPOINT = f"{CHECKPOINT_PATH}/road_stats"
        LANE_CHECKPOINT = f"{CHECKPOINT_PATH}/lane_stats"
        HOURLY_CHECKPOINT = f"{CHECKPOINT_PATH}/hourly_stats"
        DOW_CHECKPOINT = f"{CHECKPOINT_PATH}/dow_stats"

        # District statistics
        if checkpoint_exists(spark, DISTRICT_CHECKPOINT):
            print("\n[6/9] ✓ Loading district statistics from checkpoint...")
            district_stats = spark.read.parquet(DISTRICT_CHECKPOINT)
        else:
            print("\n[6/9] Computing district statistics...")
            district_stats = statistics_by_district(df_numeric)
            district_stats.write.mode("overwrite").parquet(DISTRICT_CHECKPOINT)
            print(f"        ✓ Checkpoint saved: {DISTRICT_CHECKPOINT}")

        # Road statistics
        if checkpoint_exists(spark, ROAD_CHECKPOINT):
            print("\n[7/9] ✓ Loading road statistics from checkpoint...")
            road_stats = spark.read.parquet(ROAD_CHECKPOINT)
        else:
            print("\n[7/9] Computing road statistics...")
            road_stats = statistics_by_road(df_numeric)
            road_stats.write.mode("overwrite").parquet(ROAD_CHECKPOINT)
            print(f"        ✓ Checkpoint saved: {ROAD_CHECKPOINT}")

        # Lane statistics
        if checkpoint_exists(spark, LANE_CHECKPOINT):
            print("\n[8/9] ✓ Loading lane statistics from checkpoint...")
            lane_stats = spark.read.parquet(LANE_CHECKPOINT)
        else:
            print("\n[8/9] Computing lane statistics...")
            lane_stats = statistics_by_lane(df_numeric)
            lane_stats.write.mode("overwrite").parquet(LANE_CHECKPOINT)
            print(f"        ✓ Checkpoint saved: {LANE_CHECKPOINT}")

        # Hourly statistics
        if checkpoint_exists(spark, HOURLY_CHECKPOINT):
            print("\n[9/9] ✓ Loading hourly statistics from checkpoint...")
            hourly_stats = spark.read.parquet(HOURLY_CHECKPOINT)
        else:
            print("\n[9/9] Computing hourly statistics...")
            hourly_stats = statistics_by_hour(df_numeric)
            hourly_stats.write.mode("overwrite").parquet(HOURLY_CHECKPOINT)
            print(f"        ✓ Checkpoint saved: {HOURLY_CHECKPOINT}")

        # Day of week statistics
        if checkpoint_exists(spark, DOW_CHECKPOINT):
            print("\n      ✓ Loading day-of-week statistics from checkpoint...")
            dow_stats = spark.read.parquet(DOW_CHECKPOINT)
        else:
            print("\n      Computing day-of-week statistics...")
            dow_stats = statistics_by_day_of_week(df_numeric)
            dow_stats.write.mode("overwrite").parquet(DOW_CHECKPOINT)
            print(f"        ✓ Checkpoint saved: {DOW_CHECKPOINT}")

        # Direction statistics (optional - not saved to final output)
        print("\n      Computing direction statistics...")
        direction_stats = statistics_by_direction(df_numeric)

        # 9. Save final statistics summaries to output directory
        print("\n[Final] Saving statistics summaries to output directory...")
        save_statistics_summary(
            spark, district_stats, road_stats, lane_stats,
            hourly_stats, dow_stats, OUTPUT_PATH
        )

        print("\n" + "="*80)
        print("✓ ANALYSIS COMPLETE!")
        print("="*80)
        print(f"\nStatistics saved to: {OUTPUT_PATH}/")
        print("Summary files created:")
        print("  - district_stats/")
        print("  - road_stats/")
        print("  - lane_stats/")
        print("  - hourly_stats/")
        print("  - dow_stats/")
        print(f"\nCheckpoints saved to: {CHECKPOINT_PATH}/")
        print("  - valid_data/ (1.6B records in Parquet format)")
        print("  - district_stats/")
        print("  - road_stats/")
        print("  - lane_stats/")
        print("  - hourly_stats/")
        print("  - dow_stats/")
        print("\nTip: Re-run this script to use checkpoints for faster processing!")
        print("     Delete checkpoint directories to force recomputation.")
        print("\nAnalysis successful!")

    except Exception as e:
        print("\n" + "="*80)
        print("✗ ERROR: Analysis failed!")
        print("="*80)
        print(f"\nError message: {e}")
        print("\nFull traceback:")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        # Stop Spark session
        print("\nClosing Spark session...")
        spark.stop()
        print("Done.")

if __name__ == "__main__":
    main()