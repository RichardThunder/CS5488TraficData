# Traffic XML to CSV Converter with PySpark

This project converts 28,000+ traffic XML files from Hong Kong traffic detectors into a unified CSV format and merges them with location data.

## Files

1. **xml_to_csv_converter.py** - Main script using standard Python XML parsing with PySpark parallelization
2. **xml_to_csv_spark_xml.py** - Alternative script using spark-xml library (faster for very large files)
3. **test_converter.py** - Test script to validate XML parsing with sample files

## Requirements

### Basic Version (xml_to_csv_converter.py)
```bash
pip install pyspark
```

### Spark-XML Version (xml_to_csv_spark_xml.py)
```bash
pip install pyspark
# The spark-xml package will be downloaded automatically via Maven
```

## Input Data Structure

### XML Files (202508/*.xml)
- 28,000+ XML files with traffic detector data
- Format: rawSpeedVol-all.xml
- Structure:
  - Date
  - Periods (multiple time periods)
    - Detectors (multiple detectors per period)
      - Lanes (multiple lanes per detector: Fast Lane, Middle Lane, Slow Lane)
        - Speed, Occupancy, Volume, Standard Deviation, Valid flag

### Location Data (Locations_of_Traffic_Detectors.gdb_converted.csv)
- Contains geographical information for each detector
- Columns: OBJECTID, AID_ID_Number, District, Road_EN, Direction, GeometryEasting, GeometryNorthing, etc.

## Usage

### Method 1: Basic XML Parser (Recommended for most cases)

```bash
python xml_to_csv_converter.py
```

**Pros:**
- No external package dependencies
- Works out of the box with standard PySpark
- Good for moderate-sized XML files

**Output:**
- `traffic_data_merged.csv/` - Single merged CSV file
- `traffic_data_partitioned/` - Partitioned by date for better query performance

### Method 2: Spark-XML Library (Faster for large files)

```bash
python xml_to_csv_spark_xml.py
```

**Pros:**
- Better performance for very large XML files
- Leverages Databricks spark-xml library
- Automatic schema inference

**Output:**
- `traffic_data_merged_sparkxml.csv/` - Single merged CSV file
- `traffic_data_partitioned_sparkxml/` - Partitioned by date

### Testing

Before processing all files, test with a few samples:

```bash
python test_converter.py
```

## Output Schema

The final CSV will contain the following columns:

| Column | Description |
|--------|-------------|
| date | Date of measurement |
| period_from | Start time of measurement period |
| period_to | End time of measurement period |
| detector_id | Traffic detector ID (e.g., AID01101) |
| direction | Traffic direction (e.g., South East) |
| lane_id | Lane identifier (Fast Lane, Middle Lane, Slow Lane) |
| speed | Average speed (km/h) |
| occupancy | Lane occupancy percentage |
| volume | Traffic volume (number of vehicles) |
| sd | Standard deviation of speed |
| valid | Data validity flag (Y/N) |
| OBJECTID | Location reference ID |
| District | District name |
| Road_EN | Road name in English |
| Road_TC | Road name in Traditional Chinese |
| Road_SC | Road name in Simplified Chinese |
| Rotation | Detector rotation angle |
| GeometryEasting | Geographic coordinate (Easting) |
| GeometryNorthing | Geographic coordinate (Northing) |

## Performance Tuning

### For Memory Issues

Adjust Spark memory settings in the script:

```python
.config("spark.driver.memory", "8g") \
.config("spark.executor.memory", "8g") \
```

### For Faster Processing

Increase parallelism:

```python
.config("spark.sql.shuffle.partitions", "400") \
```

### For Large Output Files

Use partitioned output instead of single CSV:

```python
# Output will be in multiple part files
merged_df.write.mode("overwrite").csv("output_dir", header=True)
```

## Troubleshooting

### Issue: OutOfMemoryError
**Solution:** Increase driver and executor memory, or process files in batches

### Issue: Slow performance
**Solution:** Try the spark-xml version or increase shuffle partitions

### Issue: spark-xml package not found
**Solution:** Check internet connection for Maven package download, or manually add the JAR file

### Issue: XML parsing errors
**Solution:** Run test_converter.py to identify problematic XML files

## Data Processing Flow

1. **Load XML Files**: Read all 28,000+ XML files from 202508/ directory
2. **Parse & Flatten**: Extract nested data into flat structure
3. **Load Location Data**: Read detector location CSV
4. **Join**: Merge traffic data with location data on detector_id
5. **Export**: Save as CSV (single file or partitioned)

## Example Query After Processing

Using Spark SQL on the output:

```python
# Load the partitioned data
df = spark.read.csv("traffic_data_partitioned", header=True, inferSchema=True)

# Query: Average speed by detector and lane
df.groupBy("detector_id", "lane_id").agg({"speed": "avg"}).show()

# Query: Traffic volume by hour
df.withColumn("hour", substring("period_from", 1, 2)) \
  .groupBy("hour").agg({"volume": "sum"}) \
  .orderBy("hour").show()
```

## License

This script is for processing Hong Kong government open data.
Data source: https://data.gov.hk/
