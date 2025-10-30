"""
Quick test of the spark-xml converter with built-in XML support
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, input_file_name, regexp_extract
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Test Built-in XML") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print("Spark Session initialized")

# Get first 5 XML files
xml_directory = "202508"
all_files = [f for f in os.listdir(xml_directory) if f.endswith('.xml')][:5]
# Use absolute paths with file:// protocol
test_paths = [f"file://{os.path.abspath(os.path.join(xml_directory, f))}" for f in all_files]

print(f"Testing with {len(test_paths)} files:")
for p in test_paths:
    print(f"  - {p}")

# Read XML files
print("\nReading XML files...")
xml_df = spark.read.format("xml") \
    .option("rowTag", "period") \
    .option("nullValue", "") \
    .load(test_paths)

print("\nXML Schema:")
xml_df.printSchema()

print(f"\nNumber of periods loaded: {xml_df.count()}")
print("\nSample data:")
xml_df.show(2, truncate=False, vertical=True)

# Try to access nested data
print("\nTrying to explode detectors...")
xml_with_file = xml_df.withColumn("source_file", input_file_name())

detectors_df = xml_with_file.select(
    col("period_from"),
    col("period_to"),
    col("source_file"),
    explode(col("detectors.detector")).alias("detector")
)

print(f"Number of detector records: {detectors_df.count()}")
detectors_df.show(5, truncate=False)

spark.stop()
print("\nTest completed!")
