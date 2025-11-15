from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, stddev, corr, when, lit,
    expr, round as spark_round
)
import logging
import time
import os

"""
Analysis 4: Traffic Flow Theory

Based on analysis.md Section 4:
- Speed-Volume-Occupancy relationships
- Traffic flow regimes identification
- Fundamental diagram analysis
- Capacity estimation
"""

# Configuration
CLEANED_DATA_PATH = "analysis_results/01_eda/cleaned_data"
OUTPUT_DIR = "analysis_results/04_traffic_flow_theory"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def initialize_spark():
    """Initialize Spark session with optimized settings"""
    return SparkSession.builder \
        .appName("Traffic Flow Theory Analysis") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .enableHiveSupport() \
        .getOrCreate()

def analyze_speed_volume_relationship(df, spark):
    """
    Section 4.1: Speed-Volume Relationships
    Fundamental traffic flow theory
    """
    logger.info("=" * 80)
    logger.info("SPEED-VOLUME RELATIONSHIP ANALYSIS")
    logger.info("=" * 80)

    start_time = time.time()

    # Create volume bins for analysis
    logger.info("\n--- Binned Speed vs Volume Analysis ---")

    # Bin volume into ranges
    df_binned = df.withColumn("volume_bin",
        when(col("volume") == 0, "0-Empty")
        .when(col("volume") <= 5, "1-5")
        .when(col("volume") <= 10, "6-10")
        .when(col("volume") <= 20, "11-20")
        .when(col("volume") <= 30, "21-30")
        .when(col("volume") <= 40, "31-40")
        .when(col("volume") <= 50, "41-50")
        .when(col("volume") <= 75, "51-75")
        .when(col("volume") <= 100, "76-100")
        .otherwise("100+")
    )

    volume_speed_relationship = df_binned.groupBy("volume_bin").agg(
        count("*").alias("record_count"),
        avg("speed").alias("avg_speed"),
        stddev("speed").alias("std_speed"),
        avg("occupancy").alias("avg_occupancy")
    ).orderBy("volume_bin")

    logger.info("\nSpeed by Volume Bins:")
    volume_speed_relationship.show(truncate=False)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    volume_speed_relationship.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/speed_volume_relationship")

    # Correlation analysis
    logger.info("\n--- Correlation Analysis ---")

    correlations = df.select(
        corr("speed", "volume").alias("speed_volume_corr"),
        corr("speed", "occupancy").alias("speed_occupancy_corr"),
        corr("volume", "occupancy").alias("volume_occupancy_corr")
    ).collect()[0]

    logger.info(f"Speed-Volume Correlation: {correlations['speed_volume_corr']:.4f}")
    logger.info(f"Speed-Occupancy Correlation: {correlations['speed_occupancy_corr']:.4f}")
    logger.info(f"Volume-Occupancy Correlation: {correlations['volume_occupancy_corr']:.4f}")

    # Expected relationships:
    # - Speed-Volume: Negative at high volumes (congestion), positive at low volumes
    # - Speed-Occupancy: Negative (inverse relationship)
    # - Volume-Occupancy: Positive

    elapsed = time.time() - start_time
    logger.info(f"\nSpeed-Volume analysis completed in {elapsed:.2f}s")

def analyze_volume_occupancy_relationship(df, spark):
    """
    Volume-Occupancy Relationship
    """
    logger.info("\n" + "=" * 80)
    logger.info("VOLUME-OCCUPANCY RELATIONSHIP ANALYSIS")
    logger.info("=" * 80)

    start_time = time.time()

    # Bin occupancy for analysis
    df_binned = df.withColumn("occupancy_bin",
        when(col("occupancy") < 10, "0-10%")
        .when(col("occupancy") < 20, "10-20%")
        .when(col("occupancy") < 30, "20-30%")
        .when(col("occupancy") < 40, "30-40%")
        .when(col("occupancy") < 50, "40-50%")
        .when(col("occupancy") < 60, "50-60%")
        .when(col("occupancy") < 70, "60-70%")
        .when(col("occupancy") < 80, "70-80%")
        .otherwise("80%+")
    )

    occupancy_volume_relationship = df_binned.groupBy("occupancy_bin").agg(
        count("*").alias("record_count"),
        avg("volume").alias("avg_volume"),
        avg("speed").alias("avg_speed"),
        stddev("volume").alias("std_volume")
    ).orderBy("occupancy_bin")

    logger.info("\nVolume by Occupancy Bins:")
    occupancy_volume_relationship.show(truncate=False)

    occupancy_volume_relationship.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/volume_occupancy_relationship")

    elapsed = time.time() - start_time
    logger.info(f"\nVolume-Occupancy analysis completed in {elapsed:.2f}s")

def analyze_speed_occupancy_relationship(df, spark):
    """
    Speed-Occupancy Relationship
    Critical density identification
    """
    logger.info("\n" + "=" * 80)
    logger.info("SPEED-OCCUPANCY RELATIONSHIP ANALYSIS")
    logger.info("=" * 80)

    start_time = time.time()

    # Bin occupancy for detailed analysis
    df_binned = df.withColumn("occupancy_bin",
        when(col("occupancy") < 5, "0-5%")
        .when(col("occupancy") < 10, "5-10%")
        .when(col("occupancy") < 15, "10-15%")
        .when(col("occupancy") < 20, "15-20%")
        .when(col("occupancy") < 25, "20-25%")
        .when(col("occupancy") < 30, "25-30%")
        .when(col("occupancy") < 35, "30-35%")
        .when(col("occupancy") < 40, "35-40%")
        .when(col("occupancy") < 45, "40-45%")
        .when(col("occupancy") < 50, "45-50%")
        .when(col("occupancy") < 60, "50-60%")
        .when(col("occupancy") < 70, "60-70%")
        .when(col("occupancy") < 80, "70-80%")
        .otherwise("80%+")
    )

    speed_occupancy_relationship = df_binned.groupBy("occupancy_bin").agg(
        count("*").alias("record_count"),
        avg("speed").alias("avg_speed"),
        stddev("speed").alias("std_speed"),
        avg("volume").alias("avg_volume")
    ).orderBy("occupancy_bin")

    logger.info("\nSpeed by Occupancy Bins:")
    speed_occupancy_relationship.show(15, truncate=False)

    # Identify critical density (occupancy where speed drops rapidly)
    logger.info("\nIdentifying critical density threshold...")
    logger.info("(Critical density = occupancy level where speed starts dropping significantly)")

    speed_occupancy_relationship.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/speed_occupancy_relationship")

    elapsed = time.time() - start_time
    logger.info(f"\nSpeed-Occupancy analysis completed in {elapsed:.2f}s")

def analyze_fundamental_diagram(df, spark):
    """
    Create data for Fundamental Diagram of Traffic Flow
    Flow (volume) vs Density (occupancy)
    """
    logger.info("\n" + "=" * 80)
    logger.info("FUNDAMENTAL DIAGRAM OF TRAFFIC FLOW")
    logger.info("=" * 80)

    start_time = time.time()

    logger.info("\n--- Flow-Density Relationship ---")
    logger.info("(Fundamental Diagram: Flow vs Density)")

    # Create bins for both flow and density
    fundamental_data = df.withColumn("flow_bin",
        when(col("volume") <= 10, "Low (0-10)")
        .when(col("volume") <= 30, "Medium (11-30)")
        .when(col("volume") <= 50, "High (31-50)")
        .otherwise("Very High (50+)")
    ).withColumn("density_bin",
        when(col("occupancy") < 20, "Low (<20%)")
        .when(col("occupancy") < 40, "Medium (20-40%)")
        .when(col("occupancy") < 60, "High (40-60%)")
        .otherwise("Very High (60%+)")
    )

    fundamental_diagram = fundamental_data.groupBy("density_bin", "flow_bin").agg(
        count("*").alias("record_count"),
        avg("speed").alias("avg_speed"),
        avg("volume").alias("avg_volume"),
        avg("occupancy").alias("avg_occupancy")
    ).orderBy("density_bin", "flow_bin")

    fundamental_diagram.show(20, truncate=False)

    fundamental_diagram.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/fundamental_diagram")

    # Export detailed scatter data for plotting
    logger.info("\n--- Exporting scatter plot data ---")

    # Sample data for scatter plots (to keep file size manageable)
    scatter_data = df.select(
        "volume",
        "occupancy",
        "speed"
    ).sample(False, 0.01)  # 1% sample

    scatter_data.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/scatter_plot_data")

    logger.info("Scatter plot data exported (1% sample)")

    elapsed = time.time() - start_time
    logger.info(f"\nFundamental diagram analysis completed in {elapsed:.2f}s")

def analyze_capacity_estimation(df, spark):
    """
    Estimate road capacity based on maximum observed flow
    """
    logger.info("\n" + "=" * 80)
    logger.info("CAPACITY ESTIMATION")
    logger.info("=" * 80)

    start_time = time.time()

    # Capacity by road
    logger.info("\n--- Road Capacity Estimation ---")
    logger.info("(Based on maximum observed flow)")

    road_capacity = df.groupBy("Road_EN", "District", "lane_id").agg(
        avg("volume").alias("avg_volume"),
        expr("max(volume)").alias("max_volume"),
        expr("percentile_approx(volume, 0.95)").alias("volume_95th"),
        count("*").alias("observation_count"),
        avg("speed").alias("avg_speed")
    ).filter(col("observation_count") > 100) \
     .orderBy(col("max_volume").desc()).limit(20)

    logger.info("\nTop 20 Roads by Maximum Observed Volume (Capacity Indicator):")
    road_capacity.show(20, truncate=False)

    road_capacity.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/road_capacity_estimates")

    # Capacity by lane type
    logger.info("\n--- Capacity by Lane Type ---")

    lane_capacity = df.groupBy("lane_id").agg(
        avg("volume").alias("avg_volume"),
        expr("max(volume)").alias("max_volume"),
        expr("percentile_approx(volume, 0.95)").alias("volume_95th"),
        expr("percentile_approx(volume, 0.90)").alias("volume_90th"),
        count("*").alias("observation_count")
    ).orderBy(col("max_volume").desc())

    lane_capacity.show(truncate=False)

    lane_capacity.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/lane_capacity_estimates")

    elapsed = time.time() - start_time
    logger.info(f"\nCapacity estimation completed in {elapsed:.2f}s")

def analyze_flow_regimes_detailed(df, spark):
    """
    Detailed analysis of traffic flow regimes
    """
    logger.info("\n" + "=" * 80)
    logger.info("TRAFFIC FLOW REGIMES - DETAILED ANALYSIS")
    logger.info("=" * 80)

    start_time = time.time()

    # Classify into regimes
    df_regimes = df.withColumn("flow_regime",
        when((col("speed") > 60) & (col("occupancy") < 20), "Free Flow")
        .when((col("speed") >= 30) & (col("speed") <= 60), "Synchronized Flow")
        .when((col("speed") < 30) & (col("occupancy") > 40), "Congested Flow")
        .otherwise("Transitional")
    )

    # Regime characteristics
    logger.info("\n--- Flow Regime Characteristics ---")

    regime_stats = df_regimes.groupBy("flow_regime").agg(
        count("*").alias("record_count"),
        avg("speed").alias("avg_speed"),
        stddev("speed").alias("std_speed"),
        avg("volume").alias("avg_volume"),
        stddev("volume").alias("std_volume"),
        avg("occupancy").alias("avg_occupancy"),
        stddev("occupancy").alias("std_occupancy")
    ).orderBy(col("record_count").desc())

    regime_stats.show(truncate=False)

    # Regime by road type
    logger.info("\n--- Flow Regimes by Lane Type ---")

    lane_regime = df_regimes.groupBy("lane_id", "flow_regime").agg(
        count("*").alias("record_count")
    ).orderBy("lane_id", col("record_count").desc())

    lane_regime.show(20, truncate=False)

    lane_regime.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/lane_flow_regimes")

    # Regime by time of day
    logger.info("\n--- Flow Regimes by Hour ---")

    from pyspark.sql.functions import hour

    df_hourly_regime = df_regimes.withColumn("hour", hour(col("period_from")))

    hourly_regime = df_hourly_regime.groupBy("hour", "flow_regime").agg(
        count("*").alias("record_count")
    ).orderBy("hour", col("record_count").desc())

    hourly_regime.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_DIR}/hourly_flow_regimes")

    elapsed = time.time() - start_time
    logger.info(f"\nFlow regime analysis completed in {elapsed:.2f}s")

def main():
    logger.info("Starting Traffic Flow Theory Analysis")
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
        analyze_speed_volume_relationship(df, spark)
        analyze_volume_occupancy_relationship(df, spark)
        analyze_speed_occupancy_relationship(df, spark)
        analyze_fundamental_diagram(df, spark)
        analyze_capacity_estimation(df, spark)
        analyze_flow_regimes_detailed(df, spark)

        total_time = time.time() - overall_start
        logger.info(f"\n{'='*80}")
        logger.info(f"TRAFFIC FLOW THEORY ANALYSIS COMPLETED in {total_time:.2f}s")
        logger.info(f"{'='*80}")
        logger.info(f"\nResults saved to: {OUTPUT_DIR}/")
        logger.info("\nUse the exported CSV files to create:")
        logger.info("  - Fundamental diagrams (Flow vs Density)")
        logger.info("  - Speed-Flow curves")
        logger.info("  - Capacity analysis charts")

    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
