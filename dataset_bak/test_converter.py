"""
Test script to validate XML parsing before processing all 28,000 files
Tests with a small sample of XML files
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import xml.etree.ElementTree as ET
import os

def parse_xml_file(file_path):
    """Parse a single XML file and return list of records"""
    records = []
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Get date (with namespace handling)
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
        raise

    return records

def main():
    print("=" * 80)
    print("Traffic XML Parser - Test Script")
    print("=" * 80)

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Traffic XML Parser - Test") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    print("\n✓ Spark Session initialized")

    # Define schema
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

    # Test with first 10 XML files
    xml_directory = "202508"
    all_xml_files = [os.path.join(xml_directory, f) for f in os.listdir(xml_directory) if f.endswith('.xml')]
    test_files = all_xml_files[:10]  # Test with first 10 files

    print(f"\n✓ Found {len(all_xml_files)} total XML files")
    print(f"✓ Testing with {len(test_files)} sample files")

    # Test parsing first file locally
    print(f"\n--- Testing single file parse: {test_files[0]} ---")
    try:
        records = parse_xml_file(test_files[0])
        print(f"✓ Successfully parsed {len(records)} records from first file")
        if records:
            print("\nSample record:")
            for key, value in records[0].items():
                print(f"  {key}: {value}")
    except Exception as e:
        print(f"✗ Error parsing first file: {e}")
        return

    # Parse test files in parallel
    print(f"\n--- Testing parallel processing with Spark ---")
    file_paths_rdd = spark.sparkContext.parallelize(test_files, numSlices=5)
    parsed_data_rdd = file_paths_rdd.flatMap(parse_xml_file)
    traffic_df = spark.createDataFrame(parsed_data_rdd, schema=traffic_schema)

    # Show statistics
    total_records = traffic_df.count()
    print(f"✓ Successfully parsed {total_records} total records from {len(test_files)} files")
    print(f"✓ Average records per file: {total_records / len(test_files):.1f}")

    # Show sample data
    print("\n--- Sample Data (First 10 rows) ---")
    traffic_df.show(10, truncate=False)

    # Show unique detectors
    unique_detectors = traffic_df.select("detector_id").distinct().count()
    print(f"\n✓ Unique detectors in sample: {unique_detectors}")

    # Show unique dates
    print("\n--- Unique Dates ---")
    traffic_df.select("date").distinct().show()

    # Show unique lanes
    print("\n--- Unique Lane Types ---")
    traffic_df.select("lane_id").distinct().show()

    # Test location data merge
    print("\n--- Testing Location Data Merge ---")
    try:
        location_df = spark.read.csv(
            "Locations_of_Traffic_Detectors.gdb_converted.csv",
            header=True,
            inferSchema=True
        )
        print(f"✓ Location data loaded: {location_df.count()} records")

        merged_df = traffic_df.join(
            location_df,
            traffic_df.detector_id == location_df.AID_ID_Number,
            "left"
        )
        merged_df = merged_df.drop("AID_ID_Number")

        print(f"✓ Merged data: {merged_df.count()} records")
        print("\n--- Sample Merged Data ---")
        merged_df.select("detector_id", "date", "lane_id", "speed", "volume", "District", "Road_EN").show(10, truncate=False)

        # Check for unmatched detectors
        unmatched = traffic_df.join(
            location_df,
            traffic_df.detector_id == location_df.AID_ID_Number,
            "left_anti"
        ).select("detector_id").distinct()

        unmatched_count = unmatched.count()
        if unmatched_count > 0:
            print(f"\n⚠ Warning: {unmatched_count} detector IDs have no location data:")
            unmatched.show()
        else:
            print("\n✓ All detector IDs have matching location data")

    except Exception as e:
        print(f"✗ Error with location data: {e}")

    # Estimate processing time for all files
    print("\n" + "=" * 80)
    print("ESTIMATION FOR FULL DATASET")
    print("=" * 80)
    total_files = len(all_xml_files)
    avg_records_per_file = total_records / len(test_files)
    estimated_total_records = int(total_files * avg_records_per_file)

    print(f"Total files to process: {total_files:,}")
    print(f"Estimated total records: {estimated_total_records:,}")
    print(f"Estimated output CSV size: {estimated_total_records * 200 / 1024 / 1024:.2f} MB (approx)")

    spark.stop()
    print("\n✓ Test completed successfully!")
    print("\nYou can now run the full conversion with:")
    print("  python xml_to_csv_converter.py")

if __name__ == "__main__":
    main()
