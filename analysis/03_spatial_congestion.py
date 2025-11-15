from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, desc, stddev, when,
    min as spark_min, max as spark_max, hour
)
import logging
import time
import os

"""
Analysis 3: Spatial Analysis & Congestion

Based on analysis.md Sections 3 & 5:
- Geographic hotspot identification (slowest, busiest, most congested roads)
- Lane analysis
- Direction analysis
- District rankings
- Congestion index and analysis
"""

# Configuration
CLEANED_DATA_PATH = "analysis_results/01_eda/cleaned_data"
OUTPUT_DIR = "analysis_results/03_spatial_congestion"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def initialize_spark():
    """Initialize Spark session with optimized settings"""
    return SparkSession.builder \
        .appName("Traffic Spatial & Congestion Analysis") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .enableHiveSupport() \
        .getOrCreate()

def identify_hotspots(df, spark):
    """
    Section 3.1: Geographic Hotspot Identification
    Find slowest, busiest, and most congested roads
    """
    logger.info("=" * 80)
    logger.info("GEOGRAPHIC HOTSPOT IDENTIFICATION")
    logger.info("=" * 80)

    start_time = time.time()

    # Top 10 Slowest Roads
    logger.info("\n--- Top 10 SLOWEST Roads (Lowest Average Speed) ---")
    slowest_roads = df.groupBy("Road_EN", "District").agg(
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy"),
        count("*").alias("observation_count")
    ).orderBy("avg_speed").limit(10)

    slowest_roads.show(10, truncate=False)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    slowest_roads.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/top10_slowest_roads")

    # Top 10 Busiest Roads
    logger.info("\n--- Top 10 BUSIEST Roads (Highest Total Volume) ---")
    busiest_roads = df.groupBy("Road_EN", "District").agg(
        spark_sum("volume").alias("total_volume"),
        avg("speed").alias("avg_speed"),
        avg("occupancy").alias("avg_occupancy"),
        count("*").alias("observation_count")
    ).orderBy(col("total_volume").desc()).limit(10)

    busiest_roads.show(10, truncate=False)

    busiest_roads.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/top10_busiest_roads")

    # Top 10 Most Congested Roads (Combined metric)
    logger.info("\n--- Top 10 MOST CONGESTED Roads (Low Speed + High Occupancy) ---")
    congested_roads = df.groupBy("Road_EN", "District").agg(
        avg("speed").alias("avg_speed"),
        avg("occupancy").alias("avg_occupancy"),
        spark_sum("volume").alias("total_volume"),
        count("*").alias("observation_count"),
        # Count severe congestion events
        spark_sum(when((col("occupancy") > 50) & (col("speed") < 40), 1)
                 .otherwise(0)).alias("severe_congestion_events")
    ).withColumn("congestion_index",
                 # Congestion index: higher occupancy and lower speed = more congested
                 col("avg_occupancy") * 0.5 + (100 - col("avg_speed")) * 0.5
    ).orderBy(col("congestion_index").desc()).limit(10)

    congested_roads.show(10, truncate=False)

    congested_roads.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/top10_most_congested_roads")

    # District Rankings
    logger.info("\n--- District Rankings ---")
    district_ranking = df.groupBy("District").agg(
        avg("speed").alias("avg_speed"),
        spark_sum("volume").alias("total_volume"),
        avg("occupancy").alias("avg_occupancy"),
        count("*").alias("observation_count"),
        spark_sum(when((col("occupancy") > 50) & (col("speed") < 40), 1)
                 .otherwise(0)).alias("severe_congestion_events")
    ).withColumn("congestion_index",
                 col("avg_occupancy") * 0.5 + (100 - col("avg_speed")) * 0.5
    ).orderBy(col("congestion_index").desc())

    district_ranking.show(20, truncate=False)

    district_ranking.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/district_rankings")

    elapsed = time.time() - start_time
    logger.info(f"\nHotspot identification completed in {elapsed:.2f}s")

def analyze_lanes(df, spark):
    """
    Section 3.2: Lane Analysis
    Understand lane-specific behaviors
    """
    logger.info("\n" + "=" * 80)
    logger.info("LANE ANALYSIS")
    logger.info("=" * 80)

    start_time = time.time()

    # Speed by Lane
    logger.info("\n--- Average Speed by Lane ---")
    lane_speed = df.groupBy("lane_id").agg(
        avg("speed").alias("avg_speed"),
        stddev("speed").alias("std_speed"),
        spark_min("speed").alias("min_speed"),
        spark_max("speed").alias("max_speed"),
        count("*").alias("record_count")
    ).orderBy(col("avg_speed").desc())

    lane_speed.show(truncate=False)

    # Volume by Lane
    logger.info("\n--- Traffic Volume by Lane ---")
    lane_volume = df.groupBy("lane_id").agg(
        spark_sum("volume").alias("total_volume"),
        avg("volume").alias("avg_volume"),
        count("*").alias("record_count")
    ).orderBy(col("total_volume").desc())

    lane_volume.show(truncate=False)

    # Occupancy by Lane
    logger.info("\n--- Occupancy by Lane ---")
    lane_occupancy = df.groupBy("lane_id").agg(
        avg("occupancy").alias("avg_occupancy"),
        stddev("occupancy").alias("std_occupancy"),
        count("*").alias("record_count")
    ).orderBy(col("avg_occupancy").desc())

    lane_occupancy.show(truncate=False)

    # Combined lane statistics
    lane_stats = df.groupBy("lane_id").agg(
        avg("speed").alias("avg_speed"),
        spark_sum("volume").alias("total_volume"),
        avg("occupancy").alias("avg_occupancy"),
        count("*").alias("record_count")
    ).orderBy("lane_id")

    lane_stats.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/lane_statistics")

    # Peak hour lane performance
    logger.info("\n--- Lane Performance During Peak Hours ---")
    df_peak = df.withColumn("hour", hour(col("period_from"))) \
        .filter(
            ((col("hour") >= 6) & (col("hour") < 10)) |  # Morning rush
            ((col("hour") >= 17) & (col("hour") < 21))   # Evening rush
        )

    peak_lane_stats = df_peak.groupBy("lane_id").agg(
        avg("speed").alias("avg_speed_peak"),
        avg("occupancy").alias("avg_occupancy_peak"),
        count("*").alias("record_count")
    ).orderBy("lane_id")

    logger.info("Lane performance during rush hours (6-10, 17-21):")
    peak_lane_stats.show(truncate=False)

    elapsed = time.time() - start_time
    logger.info(f"\nLane analysis completed in {elapsed:.2f}s")

def analyze_directions(df, spark):
    """
    Section 3.3: Direction Analysis
    Identify commuting patterns and asymmetric congestion
    """
    logger.info("\n" + "=" * 80)
    logger.info("DIRECTION ANALYSIS")
    logger.info("=" * 80)

    start_time = time.time()

    df_direction = df.withColumn("hour", hour(col("period_from")))

    # Morning inbound vs outbound
    logger.info("\n--- Morning Rush (6-10): Direction Comparison ---")
    morning_direction = df_direction.filter(
        (col("hour") >= 6) & (col("hour") < 10)
    ).groupBy("direction").agg(
        avg("speed").alias("avg_speed"),
        spark_sum("volume").alias("total_volume"),
        avg("occupancy").alias("avg_occupancy"),
        count("*").alias("record_count")
    ).orderBy("direction")

    morning_direction.show(truncate=False)

    # Evening inbound vs outbound
    logger.info("\n--- Evening Rush (17-21): Direction Comparison ---")
    evening_direction = df_direction.filter(
        (col("hour") >= 17) & (col("hour") < 21)
    ).groupBy("direction").agg(
        avg("speed").alias("avg_speed"),
        spark_sum("volume").alias("total_volume"),
        avg("occupancy").alias("avg_occupancy"),
        count("*").alias("record_count")
    ).orderBy("direction")

    evening_direction.show(truncate=False)

    # Asymmetric congestion detection
    logger.info("\n--- Asymmetric Congestion (by Road) ---")
    logger.info("Roads where one direction is significantly more congested...")

    road_direction = df_direction.groupBy("Road_EN", "direction").agg(
        avg("speed").alias("avg_speed"),
        avg("occupancy").alias("avg_occupancy"),
        count("*").alias("observation_count")
    ).filter(col("observation_count") > 100)  # Filter for significance

    # This creates a more complex comparison - export to CSV for analysis
    road_direction.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/road_direction_comparison")

    # District commuter flow patterns
    logger.info("\n--- District Commuter Flow Patterns ---")

    # Morning: which districts have high inbound traffic
    morning_district_flow = df_direction.filter(
        (col("hour") >= 6) & (col("hour") < 10)
    ).groupBy("District", "direction").agg(
        spark_sum("volume").alias("total_volume"),
        avg("speed").alias("avg_speed")
    ).orderBy("District", col("total_volume").desc())

    morning_district_flow.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/morning_district_flow")

    elapsed = time.time() - start_time
    logger.info(f"\nDirection analysis completed in {elapsed:.2f}s")

def analyze_congestion_details(df, spark):
    """
    Section 5: Detailed Congestion Analysis
    Congestion index, duration, severity classification
    """
    logger.info("\n" + "=" * 80)
    logger.info("DETAILED CONGESTION ANALYSIS")
    logger.info("=" * 80)

    start_time = time.time()

    # Define traffic flow regimes
    logger.info("\n--- Traffic Flow Regime Classification ---")

    df_regimes = df.withColumn("traffic_regime",
        when((col("speed") > 60) & (col("occupancy") < 20), "Free Flow")
        .when((col("speed") >= 30) & (col("speed") <= 60) &
              (col("occupancy") >= 20) & (col("occupancy") <= 40), "Synchronized Flow")
        .when((col("speed") < 30) & (col("occupancy") > 40), "Wide Moving Jam")
        .otherwise("Transitional")
    )

    regime_distribution = df_regimes.groupBy("traffic_regime").agg(
        count("*").alias("record_count"),
        avg("speed").alias("avg_speed"),
        avg("occupancy").alias("avg_occupancy")
    ).orderBy(col("record_count").desc())

    logger.info("\nTraffic Regime Distribution:")
    regime_distribution.show(truncate=False)

    # Calculate percentage
    total_count = df.count()
    logger.info(f"\nTotal records: {total_count:,}")
    for row in regime_distribution.collect():
        pct = (row['record_count'] / total_count) * 100
        logger.info(f"{row['traffic_regime']}: {pct:.2f}%")

    regime_distribution.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/traffic_regime_distribution")

    # Congestion severity by road
    logger.info("\n--- Congestion Severity by Road ---")

    road_congestion = df_regimes.groupBy("Road_EN", "District").agg(
        count("*").alias("total_observations"),
        spark_sum(when(col("traffic_regime") == "Free Flow", 1).otherwise(0)).alias("free_flow_count"),
        spark_sum(when(col("traffic_regime") == "Synchronized Flow", 1).otherwise(0)).alias("synch_flow_count"),
        spark_sum(when(col("traffic_regime") == "Wide Moving Jam", 1).otherwise(0)).alias("jam_count"),
        avg("speed").alias("avg_speed"),
        avg("occupancy").alias("avg_occupancy")
    ).withColumn("congestion_rate",
                 (col("jam_count") / col("total_observations") * 100)
    ).orderBy(col("congestion_rate").desc()).limit(20)

    logger.info("\nTop 20 Roads by Congestion Rate (% time in Wide Moving Jam):")
    road_congestion.show(20, truncate=False)

    road_congestion.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/road_congestion_severity")

    # Speed variability (indicates unstable flow)
    logger.info("\n--- Speed Variability Analysis (Traffic Flow Stability) ---")

    speed_variability = df.groupBy("Road_EN", "District").agg(
        avg("speed").alias("avg_speed"),
        stddev("speed").alias("std_speed"),
        count("*").alias("observation_count")
    ).filter(col("observation_count") > 100) \
     .withColumn("coefficient_of_variation",
                col("std_speed") / col("avg_speed")
     ).orderBy(col("coefficient_of_variation").desc()).limit(20)

    logger.info("\nTop 20 Roads with Highest Speed Variability (Unstable Flow):")
    speed_variability.show(20, truncate=False)

    speed_variability.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/speed_variability")

    elapsed = time.time() - start_time
    logger.info(f"\nCongestion detail analysis completed in {elapsed:.2f}s")

def generate_geospatial_export(df, spark):
    """
    Export data for geospatial visualization
    """
    logger.info("\n" + "=" * 80)
    logger.info("GEOSPATIAL DATA EXPORT")
    logger.info("=" * 80)

    start_time = time.time()

    # Aggregate by detector location for heat mapping
    logger.info("\n--- Preparing data for heat maps ---")

    geo_data = df.groupBy("detector_id", "Road_EN", "District",
                          "GeometryEasting", "GeometryNorthing").agg(
        avg("speed").alias("avg_speed"),
        avg("occupancy").alias("avg_occupancy"),
        spark_sum("volume").alias("total_volume"),
        count("*").alias("observation_count")
    ).withColumn("congestion_index",
                 col("avg_occupancy") * 0.5 + (100 - col("avg_speed")) * 0.5
    )

    logger.info("Exporting geospatial data...")
    geo_data.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/geospatial_data")

    logger.info("Geospatial data exported successfully")
    logger.info("Use this data with Folium, Plotly, or Kepler.gl for mapping")

    elapsed = time.time() - start_time
    logger.info(f"\nGeospatial export completed in {elapsed:.2f}s")

def main():
    logger.info("Starting Spatial & Congestion Analysis")
    logger.info(f"Data path: {CLEANED_DATA_PATH}")
    logger.info(f"Output directory: {OUTPUT_DIR}")

    overall_start = time.time()

    spark = initialize_spark()

    try:
        # Read cleaned data
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
        identify_hotspots(df, spark)
        analyze_lanes(df, spark)
        analyze_directions(df, spark)
        analyze_congestion_details(df, spark)
        generate_geospatial_export(df, spark)

        total_time = time.time() - overall_start
        logger.info(f"\n{'='*80}")
        logger.info(f"SPATIAL & CONGESTION ANALYSIS COMPLETED in {total_time:.2f}s")
        logger.info(f"{'='*80}")
        logger.info(f"\nResults saved to: {OUTPUT_DIR}/")

    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
