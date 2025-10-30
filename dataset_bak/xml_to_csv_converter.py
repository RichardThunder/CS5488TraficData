"""
PySpark script to convert traffic XML files to CSV and merge with location data
Processes 28,000+ XML files efficiently using Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
import xml.etree.ElementTree as ET
import os

def parse_xml_file(file_path):
    """
    Parse a single XML file and return list of records
    Each record contains: date, period_from, period_to, detector_id, direction,
    lane_id, speed, occupancy, volume, sd, valid
    """
    records = []
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Get date (with namespace handling)
        ns = {'': 'http://www.w3.org/2001/XMLSchema-instance'}
        date_elem = root.find('date')
        if date_elem is None:
            date_elem = root.find('.//{*}date')
        date = date_elem.text if date_elem is not None else None

        # Iterate through periods
        periods = root.find('periods')
        if periods is None:
            periods = root.find('.//{*}periods')

        if periods is not None:
            for period in periods.findall('period') or periods.findall('.//{*}period'):
                period_from_elem = period.find('period_from') or period.find('.//{*}period_from')
                period_to_elem = period.find('period_to') or period.find('.//{*}period_to')

                period_from = period_from_elem.text if period_from_elem is not None else None
                period_to = period_to_elem.text if period_to_elem is not None else None

                # Iterate through detectors
                detectors = period.find('detectors') or period.find('.//{*}detectors')
                if detectors is not None:
                    for detector in detectors.findall('detector') or detectors.findall('.//{*}detector'):
                        detector_id_elem = detector.find('detector_id') or detector.find('.//{*}detector_id')
                        direction_elem = detector.find('direction') or detector.find('.//{*}direction')

                        detector_id = detector_id_elem.text if detector_id_elem is not None else None
                        direction = direction_elem.text if direction_elem is not None else None

                        # Iterate through lanes
                        lanes = detector.find('lanes') or detector.find('.//{*}lanes')
                        if lanes is not None:
                            for lane in lanes.findall('lane') or lanes.findall('.//{*}lane'):
                                lane_id_elem = lane.find('lane_id') or lane.find('.//{*}lane_id')
                                speed_elem = lane.find('speed') or lane.find('.//{*}speed')
                                occupancy_elem = lane.find('occupancy') or lane.find('.//{*}occupancy')
                                volume_elem = lane.find('volume') or lane.find('.//{*}volume')
                                sd_elem = lane.find('s.d.') or lane.find('.//{*}s.d.')
                                valid_elem = lane.find('valid') or lane.find('.//{*}valid')

                                record = {
                                    'date': date,
                                    'period_from': period_from,
                                    'period_to': period_to,
                                    'detector_id': detector_id,
                                    'direction': direction,
                                    'lane_id': lane_id_elem.text if lane_id_elem is not None else None,
                                    'speed': int(speed_elem.text) if speed_elem is not None and speed_elem.text else None,
                                    'occupancy': int(occupancy_elem.text) if occupancy_elem is not None and occupancy_elem.text else None,
                                    'volume': int(volume_elem.text) if volume_elem is not None and volume_elem.text else None,
                                    'sd': float(sd_elem.text) if sd_elem is not None and sd_elem.text else None,
                                    'valid': valid_elem.text if valid_elem is not None else None,
                                    'source_file': os.path.basename(file_path)
                                }
                                records.append(record)
    except Exception as e:
        print(f"Error parsing {file_path}: {str(e)}")

    return records

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Traffic XML to CSV Converter") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    print("Spark Session initialized successfully")

    # Define schema for the traffic data
    traffic_schema = StructType([
        StructField("date", StringType(), True),
        StructField("period_from", StringType(), True),
        StructField("period_to", StringType(), True),
        StructField("detector_id", StringType(), True),
        StructField("direction", StringType(), True),
        StructField("lane_id", StringType(), True),
        StructField("speed", IntegerType(), True),
        StructField("occupancy", IntegerType(), True),
        StructField("volume", IntegerType(), True),
        StructField("sd", DoubleType(), True),
        StructField("valid", StringType(), True),
        StructField("source_file", StringType(), True)
    ])

    # Get all XML file paths
    xml_directory = "202508"
    xml_files = [os.path.join(xml_directory, f) for f in os.listdir(xml_directory) if f.endswith('.xml')]
    print(f"Found {len(xml_files)} XML files to process")

    # Create RDD of file paths
    file_paths_rdd = spark.sparkContext.parallelize(xml_files, numSlices=200)

    # Parse XML files in parallel
    print("Parsing XML files...")
    parsed_data_rdd = file_paths_rdd.flatMap(parse_xml_file)

    # Convert RDD to DataFrame
    traffic_df = spark.createDataFrame(parsed_data_rdd, schema=traffic_schema)

    # Cache the DataFrame as we'll use it multiple times
    traffic_df.cache()

    print(f"Successfully parsed {traffic_df.count()} records from XML files")
    print("Sample data:")
    traffic_df.show(10, truncate=False)

    # Read location data
    print("Reading location data...")
    location_df = spark.read.csv(
        "Locations_of_Traffic_Detectors.gdb_converted.csv",
        header=True,
        inferSchema=True
    )

    print(f"Location data contains {location_df.count()} records")
    location_df.show(5, truncate=False)

    # Merge traffic data with location data
    print("Merging traffic data with location data...")
    merged_df = traffic_df.join(
        location_df,
        traffic_df.detector_id == location_df.AID_ID_Number,
        "left"
    )

    # Drop duplicate detector_id column
    merged_df = merged_df.drop("AID_ID_Number")

    print(f"Merged data contains {merged_df.count()} records")
    print("Sample merged data:")
    merged_df.show(10, truncate=False)

    # Save to CSV
    output_path = "traffic_data_merged.csv"
    print(f"Saving merged data to {output_path}...")

    # Coalesce to single partition for single CSV file (or use multiple partitions for faster write)
    merged_df.coalesce(1).write.mode("overwrite").csv(
        output_path,
        header=True
    )

    print(f"Data successfully saved to {output_path}")

    # Also save a partitioned version for better performance with large data
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
    print(f"Date range: {merged_df.select('date').distinct().collect()}")

    # Stop Spark session
    spark.stop()
    print("Processing completed successfully!")

if __name__ == "__main__":
    main()
