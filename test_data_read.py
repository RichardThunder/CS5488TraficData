#!/usr/bin/env python3
"""
Simple test script to verify we can read the traffic data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Create Spark session with error tolerance
spark = SparkSession.builder \
    .appName("Test Data Read") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.files.ignoreCorruptFiles", "true") \
    .config("spark.sql.files.ignoreMissingFiles", "true") \
    .getOrCreate()

print("="*80)
print("Testing Data Read from traffic_data_partitioned")
print("="*80)

try:
    # Try to read the data
    print("\nReading CSV files...")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "PERMISSIVE") \
        .csv("traffic_data_partitioned")

    print("\n✓ Data loaded successfully!")

    # Show schema
    print("\nSchema:")
    df.printSchema()

    # Count records
    print("\nCounting records (this may take a while)...")
    total = df.count()
    print(f"✓ Total records: {total:,}")

    # Show sample
    print("\nSample data (first 10 rows):")
    df.show(10, truncate=False)

    # Check date partitions
    print("\nDate range:")
    df.select("date").distinct().orderBy("date").show(10)

    print("\n" + "="*80)
    print("✓ Test completed successfully!")
    print("="*80)

except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("\nSpark session closed.")
