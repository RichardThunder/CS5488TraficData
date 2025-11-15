from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, month, date_format, avg, sum as spark_sum,
    count, stddev, when
)
from pyspark.sql.window import Window
import logging
import time
import os

"""
Analysis 2: Temporal Pattern Analysis

Based on analysis.md Section 2:
- Intra-day patterns (hourly traffic profiles, rush hours)
- Weekly patterns (weekday vs weekend)
- Monthly & seasonal patterns
- Holiday analysis
"""

# Configuration
CLEANED_DATA_PATH = "analysis_results/01_eda/cleaned_data"  # From previous analysis
OUTPUT_DIR = "analysis_results/02_temporal_patterns"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def initialize_spark():
    """Initialize Spark session with optimized settings"""
    return SparkSession.builder \
        .appName("Traffic Temporal Pattern Analysis") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .enableHiveSupport() \
        .getOrCreate()

def analyze_intraday_patterns(df, spark):
    """
    Section 2.1: Intra-Day Patterns (Daily Cycles)
    Identify rush hours and hourly traffic rhythms
    """
    logger.info("=" * 80)
    logger.info("INTRA-DAY PATTERN ANALYSIS")
    logger.info("=" * 80)

    start_time = time.time()

    # Add hour column
    df_hourly = df.withColumn("hour", hour(col("period_from")))

    # Hourly traffic profile - overall
    logger.info("\n--- Hourly Traffic Profile (24-hour cycle) ---")
    hourly_stats = df_hourly.groupBy("hour").agg(
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy"),
        count("*").alias("record_count"),
        stddev("speed").alias("std_speed")
    ).orderBy("hour")

    logger.info("\nHourly Statistics:")
    hourly_stats.show(24, truncate=False)

    # Save to CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    hourly_stats.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/hourly_profile")

    # Identify peak hours
    logger.info("\n--- Peak Hours Identification ---")

    # Morning rush: lowest speed or highest volume between 6-10
    morning_rush = hourly_stats.filter(
        (col("hour") >= 6) & (col("hour") <= 10)
    ).orderBy(col("avg_speed").asc()).limit(1).collect()[0]

    logger.info(f"Morning Rush Hour Peak: {morning_rush['hour']}:00")
    logger.info(f"  Avg Speed: {morning_rush['avg_speed']:.2f} km/h")
    logger.info(f"  Avg Volume: {morning_rush['avg_volume']:.2f}")
    logger.info(f"  Avg Occupancy: {morning_rush['avg_occupancy']:.2f}%")

    # Evening rush: lowest speed or highest volume between 17-21
    evening_rush = hourly_stats.filter(
        (col("hour") >= 17) & (col("hour") <= 21)
    ).orderBy(col("avg_speed").asc()).limit(1).collect()[0]

    logger.info(f"\nEvening Rush Hour Peak: {evening_rush['hour']}:00")
    logger.info(f"  Avg Speed: {evening_rush['avg_speed']:.2f} km/h")
    logger.info(f"  Avg Volume: {evening_rush['avg_volume']:.2f}")
    logger.info(f"  Avg Occupancy: {evening_rush['avg_occupancy']:.2f}%")

    # Direction analysis by hour
    logger.info("\n--- Direction Analysis by Hour ---")
    hourly_direction = df_hourly.groupBy("hour", "direction").agg(
        avg("speed").alias("avg_speed"),
        spark_sum("volume").alias("total_volume"),
        avg("occupancy").alias("avg_occupancy")
    ).orderBy("hour", "direction")

    hourly_direction.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/hourly_direction_profile")

    # Lane utilization by hour
    logger.info("\n--- Lane Utilization by Hour ---")
    hourly_lane = df_hourly.groupBy("hour", "lane_id").agg(
        avg("speed").alias("avg_speed"),
        spark_sum("volume").alias("total_volume"),
        avg("occupancy").alias("avg_occupancy")
    ).orderBy("hour", "lane_id")

    hourly_lane.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/hourly_lane_profile")

    elapsed = time.time() - start_time
    logger.info(f"\nIntra-day analysis completed in {elapsed:.2f}s")

def analyze_weekly_patterns(df, spark):
    """
    Section 2.2: Weekly Patterns
    Distinguish workday vs weekend behavior
    """
    logger.info("\n" + "=" * 80)
    logger.info("WEEKLY PATTERN ANALYSIS")
    logger.info("=" * 80)

    start_time = time.time()

    # Add day of week (1=Sunday, 2=Monday, ..., 7=Saturday in Spark)
    df_weekly = df.withColumn("day_of_week", dayofweek(col("date"))) \
                  .withColumn("day_name", date_format(col("date"), "EEEE")) \
                  .withColumn("is_weekend",
                             when((col("day_of_week") == 1) | (col("day_of_week") == 7), "Weekend")
                             .otherwise("Weekday"))

    # Statistics by day of week
    logger.info("\n--- Traffic by Day of Week ---")
    daily_stats = df_weekly.groupBy("day_of_week", "day_name").agg(
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy"),
        spark_sum("volume").alias("total_volume"),
        count("*").alias("record_count")
    ).orderBy("day_of_week")

    daily_stats.show(truncate=False)

    daily_stats.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/daily_statistics")

    # Weekday vs Weekend comparison
    logger.info("\n--- Weekday vs Weekend Comparison ---")
    weekend_comparison = df_weekly.groupBy("is_weekend").agg(
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy"),
        spark_sum("volume").alias("total_volume"),
        stddev("speed").alias("std_speed")
    )

    weekend_comparison.show(truncate=False)

    weekend_comparison.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/weekday_weekend_comparison")

    # Hour-by-hour weekday vs weekend
    logger.info("\n--- Hourly Pattern: Weekday vs Weekend ---")
    df_hourly_weekly = df_weekly.withColumn("hour", hour(col("period_from")))

    hourly_weekend_comparison = df_hourly_weekly.groupBy("hour", "is_weekend").agg(
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy")
    ).orderBy("hour", "is_weekend")

    hourly_weekend_comparison.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/hourly_weekday_weekend")

    # Monday effect analysis
    logger.info("\n--- Monday Effect Analysis ---")
    monday_vs_others = df_weekly.withColumn("is_monday",
                                             when(col("day_of_week") == 2, "Monday")
                                             .otherwise("Other Weekdays"))

    monday_stats = monday_vs_others.filter(col("is_weekend") == "Weekday") \
        .groupBy("is_monday").agg(
            avg("speed").alias("avg_speed"),
            avg("volume").alias("avg_volume"),
            avg("occupancy").alias("avg_occupancy")
        )

    monday_stats.show(truncate=False)

    # Friday evening pattern (17:00-21:00)
    logger.info("\n--- Friday Evening Pattern (Weekend Exodus) ---")
    df_friday_evening = df_weekly.withColumn("hour", hour(col("period_from"))) \
        .filter(
            (col("day_of_week") == 6) &  # Friday
            (col("hour") >= 17) & (col("hour") <= 21)
        )

    friday_evening_stats = df_friday_evening.groupBy("hour").agg(
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy")
    ).orderBy("hour")

    logger.info("Friday Evening (17:00-21:00):")
    friday_evening_stats.show(truncate=False)

    elapsed = time.time() - start_time
    logger.info(f"\nWeekly analysis completed in {elapsed:.2f}s")

def analyze_monthly_seasonal_patterns(df, spark):
    """
    Section 2.3: Monthly & Seasonal Patterns
    Detect long-term trends and seasonal variations
    """
    logger.info("\n" + "=" * 80)
    logger.info("MONTHLY & SEASONAL PATTERN ANALYSIS")
    logger.info("=" * 80)

    start_time = time.time()

    # Add month column
    df_monthly = df.withColumn("month", month(col("date"))) \
                   .withColumn("month_name", date_format(col("date"), "MMMM"))

    # Monthly statistics
    logger.info("\n--- Monthly Traffic Statistics ---")
    monthly_stats = df_monthly.groupBy("month", "month_name").agg(
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy"),
        spark_sum("volume").alias("total_volume"),
        count("*").alias("record_count"),
        stddev("speed").alias("std_speed")
    ).orderBy("month")

    monthly_stats.show(12, truncate=False)

    monthly_stats.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/monthly_statistics")

    # Seasonal classification (Hong Kong seasons)
    logger.info("\n--- Seasonal Pattern Analysis ---")
    df_seasonal = df_monthly.withColumn("season",
        when((col("month") >= 3) & (col("month") <= 5), "Spring") \
        .when((col("month") >= 6) & (col("month") <= 8), "Summer") \
        .when((col("month") >= 9) & (col("month") <= 11), "Autumn") \
        .otherwise("Winter")
    )

    seasonal_stats = df_seasonal.groupBy("season").agg(
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy"),
        spark_sum("volume").alias("total_volume")
    ).orderBy(
        when(col("season") == "Spring", 1)
        .when(col("season") == "Summer", 2)
        .when(col("season") == "Autumn", 3)
        .otherwise(4)
    )

    seasonal_stats.show(truncate=False)

    seasonal_stats.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/seasonal_statistics")

    # Month-by-month trend
    logger.info("\n--- Month-by-Month Trend Analysis ---")
    logger.info("Analyzing if traffic is getting worse over the year...")

    trend_data = monthly_stats.orderBy("month").collect()
    if len(trend_data) > 1:
        first_month = trend_data[0]
        last_month = trend_data[-1]

        speed_change = ((last_month['avg_speed'] - first_month['avg_speed'])
                       / first_month['avg_speed'] * 100)
        volume_change = ((last_month['total_volume'] - first_month['total_volume'])
                        / first_month['total_volume'] * 100)

        logger.info(f"Speed change from {first_month['month_name']} to {last_month['month_name']}: "
                   f"{speed_change:+.2f}%")
        logger.info(f"Volume change: {volume_change:+.2f}%")

    elapsed = time.time() - start_time
    logger.info(f"\nMonthly/seasonal analysis completed in {elapsed:.2f}s")

def analyze_rush_hour_details(df, spark):
    """
    Detailed rush hour analysis - combining time and day patterns
    """
    logger.info("\n" + "=" * 80)
    logger.info("RUSH HOUR DETAILED ANALYSIS")
    logger.info("=" * 80)

    start_time = time.time()

    df_rush = df.withColumn("hour", hour(col("period_from"))) \
                .withColumn("day_of_week", dayofweek(col("date"))) \
                .withColumn("is_weekend",
                           when((col("day_of_week") == 1) | (col("day_of_week") == 7), "Weekend")
                           .otherwise("Weekday"))

    # Morning rush hour (6:00-10:00) weekday vs weekend
    logger.info("\n--- Morning Rush Hour (6:00-10:00): Weekday vs Weekend ---")
    morning_rush = df_rush.filter(
        (col("hour") >= 6) & (col("hour") < 10)
    ).groupBy("is_weekend").agg(
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy"),
        count("*").alias("record_count")
    )

    morning_rush.show(truncate=False)

    # Evening rush hour (17:00-21:00) weekday vs weekend
    logger.info("\n--- Evening Rush Hour (17:00-21:00): Weekday vs Weekend ---")
    evening_rush = df_rush.filter(
        (col("hour") >= 17) & (col("hour") < 21)
    ).groupBy("is_weekend").agg(
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy"),
        count("*").alias("record_count")
    )

    evening_rush.show(truncate=False)

    # Top 10 most congested roads during rush hours
    logger.info("\n--- Top 10 Most Congested Roads During Rush Hours ---")

    morning_congested = df_rush.filter(
        (col("hour") >= 6) & (col("hour") < 10) &
        (col("is_weekend") == "Weekday")
    ).groupBy("Road_EN", "District").agg(
        avg("speed").alias("avg_speed"),
        avg("occupancy").alias("avg_occupancy"),
        spark_sum("volume").alias("total_volume")
    ).withColumn("congestion_score",
                 (col("avg_occupancy") * 0.5 + (100 - col("avg_speed")) * 0.5)) \
     .orderBy(col("congestion_score").desc()) \
     .limit(10)

    logger.info("\nMorning Rush (Weekdays):")
    morning_congested.show(truncate=False)

    morning_congested.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/morning_rush_top10_congested")

    elapsed = time.time() - start_time
    logger.info(f"\nRush hour analysis completed in {elapsed:.2f}s")

def main():
    logger.info("Starting Temporal Pattern Analysis")
    logger.info(f"Data path: {CLEANED_DATA_PATH}")
    logger.info(f"Output directory: {OUTPUT_DIR}")

    overall_start = time.time()

    spark = initialize_spark()

    try:
        # Read cleaned data from previous analysis
        logger.info("\nReading cleaned traffic data...")
        read_start = time.time()
        df = spark.read.parquet(CLEANED_DATA_PATH)
        read_time = time.time() - read_start
        logger.info(f"Data loaded in {read_time:.2f}s")

        # Cache for performance
        df.cache()
        record_count = df.count()
        logger.info(f"Total records: {record_count:,}")

        # Run analyses
        analyze_intraday_patterns(df, spark)
        analyze_weekly_patterns(df, spark)
        analyze_monthly_seasonal_patterns(df, spark)
        analyze_rush_hour_details(df, spark)

        total_time = time.time() - overall_start
        logger.info(f"\n{'='*80}")
        logger.info(f"TEMPORAL ANALYSIS COMPLETED in {total_time:.2f}s")
        logger.info(f"{'='*80}")
        logger.info(f"\nResults saved to: {OUTPUT_DIR}/")

    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
