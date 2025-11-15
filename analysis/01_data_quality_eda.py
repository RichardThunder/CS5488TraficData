from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, isnan, isnull, avg, stddev, min as spark_min,
    max as spark_max, percentile_approx, sum as spark_sum
)
import logging
import time
import os

"""
Analysis 1: Data Quality Assessment & Exploratory Data Analysis (EDA)

Based on analysis.md Section 1:
- Data quality assessment (validity, missing data, outliers)
- Descriptive statistics (speed, volume, occupancy)
- Breakdown by district, road type, lane type
"""

# Configuration
DATA_PATH = "hdfs:///traffic_data_partitioned/*"  # All months for yearly analysis
OUTPUT_DIR = "analysis_results/01_eda"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def initialize_spark():
    """Initialize Spark session with optimized settings"""
    return SparkSession.builder \
        .appName("Traffic Data Quality & EDA") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .enableHiveSupport() \
        .getOrCreate()

def assess_data_quality(df, spark):
    """
    Section 1.1: Data Quality Assessment
    """
    logger.info("=" * 80)
    logger.info("DATA QUALITY ASSESSMENT")
    logger.info("=" * 80)

    start_time = time.time()

    # Total records
    total_records = df.count()
    logger.info(f"\nTotal records in dataset: {total_records:,}")

    # 1. Validity Check
    logger.info("\n--- Validity Check ---")
    validity_stats = df.groupBy("valid").count().collect()
    for row in validity_stats:
        pct = (row['count'] / total_records) * 100
        logger.info(f"Valid={row['valid']}: {row['count']:,} ({pct:.2f}%)")

    # Filter to valid records only for further analysis
    df_valid = df.filter(col("valid") == "Y")
    valid_count = df_valid.count()
    logger.info(f"\nProceeding with {valid_count:,} valid records")

    # 2. Missing/Null Data Analysis
    logger.info("\n--- Missing Data Analysis ---")
    null_counts = df_valid.select([
        count(when(col(c).isNull() | isnan(c), c)).alias(c)
        for c in df_valid.columns if c not in ['valid']
    ]).collect()[0].asDict()

    for col_name, null_count in null_counts.items():
        if null_count > 0:
            pct = (null_count / valid_count) * 100
            logger.info(f"{col_name}: {null_count:,} nulls ({pct:.2f}%)")

    # 3. Outlier Detection
    logger.info("\n--- Outlier Detection ---")

    # Speed outliers (unrealistic values)
    speed_outliers = df_valid.filter(
        (col("speed") < 0) | (col("speed") > 200)
    ).count()
    logger.info(f"Speed outliers (< 0 or > 200 km/h): {speed_outliers:,}")

    # Volume outliers (negative values)
    volume_outliers = df_valid.filter(col("volume") < 0).count()
    logger.info(f"Volume outliers (< 0): {volume_outliers:,}")

    # Occupancy outliers (> 100%)
    occupancy_outliers = df_valid.filter(
        (col("occupancy") < 0) | (col("occupancy") > 100)
    ).count()
    logger.info(f"Occupancy outliers (< 0 or > 100%): {occupancy_outliers:,}")

    # Clean data: remove outliers and nulls on critical fields
    df_clean = df_valid.filter(
        (col("speed") >= 0) & (col("speed") <= 200) &
        (col("volume") >= 0) &
        (col("occupancy") >= 0) & (col("occupancy") <= 100) &
        col("Road_EN").isNotNull()
    )

    clean_count = df_clean.count()
    removed = valid_count - clean_count
    logger.info(f"\nCleaned dataset: {clean_count:,} records")
    logger.info(f"Removed {removed:,} records ({(removed/valid_count)*100:.2f}%)")

    # 4. Temporal Coverage
    logger.info("\n--- Temporal Coverage ---")
    date_range = df_clean.select(
        spark_min("date").alias("min_date"),
        spark_max("date").alias("max_date")
    ).collect()[0]

    logger.info(f"Date range: {date_range['min_date']} to {date_range['max_date']}")

    # Count distinct dates
    distinct_dates = df_clean.select("date").distinct().count()
    logger.info(f"Distinct dates: {distinct_dates}")

    # 5. Spatial Coverage
    logger.info("\n--- Spatial Coverage ---")
    detector_count = df_clean.select("detector_id").distinct().count()
    road_count = df_clean.select("Road_EN").distinct().count()
    district_count = df_clean.select("District").distinct().count()

    logger.info(f"Unique detectors: {detector_count:,}")
    logger.info(f"Unique roads: {road_count:,}")
    logger.info(f"Districts: {district_count}")

    elapsed = time.time() - start_time
    logger.info(f"\nData quality assessment completed in {elapsed:.2f}s")

    return df_clean

def descriptive_statistics(df, spark):
    """
    Section 1.2: Descriptive Statistics
    """
    logger.info("\n" + "=" * 80)
    logger.info("DESCRIPTIVE STATISTICS")
    logger.info("=" * 80)

    start_time = time.time()

    # Overall statistics for key metrics
    logger.info("\n--- Overall Statistics ---")

    stats = df.select(
        avg("speed").alias("avg_speed"),
        stddev("speed").alias("std_speed"),
        spark_min("speed").alias("min_speed"),
        spark_max("speed").alias("max_speed"),
        percentile_approx("speed", 0.25).alias("speed_p25"),
        percentile_approx("speed", 0.50).alias("speed_p50"),
        percentile_approx("speed", 0.75).alias("speed_p75"),
        percentile_approx("speed", 0.95).alias("speed_p95"),

        avg("volume").alias("avg_volume"),
        stddev("volume").alias("std_volume"),
        spark_sum("volume").alias("total_volume"),
        spark_min("volume").alias("min_volume"),
        spark_max("volume").alias("max_volume"),

        avg("occupancy").alias("avg_occupancy"),
        stddev("occupancy").alias("std_occupancy"),
        spark_min("occupancy").alias("min_occupancy"),
        spark_max("occupancy").alias("max_occupancy"),

        avg("standard_deviation").alias("avg_speed_std")
    ).collect()[0]

    logger.info("\nSPEED Statistics (km/h):")
    logger.info(f"  Mean: {stats['avg_speed']:.2f}")
    logger.info(f"  Std Dev: {stats['std_speed']:.2f}")
    logger.info(f"  Min: {stats['min_speed']:.2f}")
    logger.info(f"  Max: {stats['max_speed']:.2f}")
    logger.info(f"  25th Percentile: {stats['speed_p25']:.2f}")
    logger.info(f"  50th Percentile (Median): {stats['speed_p50']:.2f}")
    logger.info(f"  75th Percentile: {stats['speed_p75']:.2f}")
    logger.info(f"  95th Percentile: {stats['speed_p95']:.2f}")

    logger.info("\nVOLUME Statistics (vehicles):")
    logger.info(f"  Mean: {stats['avg_volume']:.2f}")
    logger.info(f"  Std Dev: {stats['std_volume']:.2f}")
    logger.info(f"  Total: {stats['total_volume']:,.0f}")
    logger.info(f"  Min: {stats['min_volume']:.0f}")
    logger.info(f"  Max: {stats['max_volume']:.0f}")

    logger.info("\nOCCUPANCY Statistics (%):")
    logger.info(f"  Mean: {stats['avg_occupancy']:.2f}")
    logger.info(f"  Std Dev: {stats['std_occupancy']:.2f}")
    logger.info(f"  Min: {stats['min_occupancy']:.2f}")
    logger.info(f"  Max: {stats['max_occupancy']:.2f}")

    logger.info(f"\nAverage Speed Standard Deviation: {stats['avg_speed_std']:.2f}")

    # Statistics by District
    logger.info("\n--- Statistics by District ---")
    district_stats = df.groupBy("District").agg(
        count("*").alias("record_count"),
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy"),
        spark_sum("volume").alias("total_volume")
    ).orderBy(col("total_volume").desc())

    logger.info("\nTop Districts by Traffic Volume:")
    district_stats.show(20, truncate=False)

    # Export to CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    district_stats.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/district_statistics")

    # Statistics by Lane Type
    logger.info("\n--- Statistics by Lane Type ---")
    lane_stats = df.groupBy("lane_id").agg(
        count("*").alias("record_count"),
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy")
    ).orderBy(col("avg_speed").desc())

    lane_stats.show(truncate=False)

    lane_stats.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/lane_statistics")

    # Statistics by Direction
    logger.info("\n--- Statistics by Direction ---")
    direction_stats = df.groupBy("direction").agg(
        count("*").alias("record_count"),
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy"),
        spark_sum("volume").alias("total_volume")
    ).orderBy(col("total_volume").desc())

    direction_stats.show(truncate=False)

    direction_stats.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/direction_statistics")

    elapsed = time.time() - start_time
    logger.info(f"\nDescriptive statistics completed in {elapsed:.2f}s")

def main():
    logger.info("Starting Data Quality & EDA Analysis")
    logger.info(f"Data path: {DATA_PATH}")
    logger.info(f"Output directory: {OUTPUT_DIR}")

    overall_start = time.time()

    spark = initialize_spark()

    try:
        # Read data
        logger.info("\nReading traffic data...")
        read_start = time.time()
        df = spark.read.parquet(DATA_PATH)
        read_time = time.time() - read_start
        logger.info(f"Data loaded in {read_time:.2f}s")

        # Cache for performance
        df.cache()

        # Run analyses
        df_clean = assess_data_quality(df, spark)
        df_clean.cache()

        descriptive_statistics(df_clean, spark)

        # Save cleaned dataset for future analyses
        logger.info(f"\nSaving cleaned dataset to {OUTPUT_DIR}/cleaned_data...")
        df_clean.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/cleaned_data")
        logger.info("Cleaned dataset saved successfully")

        total_time = time.time() - overall_start
        logger.info(f"\n{'='*80}")
        logger.info(f"ANALYSIS COMPLETED in {total_time:.2f}s")
        logger.info(f"{'='*80}")
        logger.info(f"\nResults saved to: {OUTPUT_DIR}/")

    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
