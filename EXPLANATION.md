# PySpark XML to CSV Converter - Detailed Explanation

## What This Code Does

This PySpark application converts 28,401 Hong Kong traffic XML files into a unified CSV format and enriches the data with geographic location information.

### Input Data

1. **XML Files** (202508/*.xml)
   - 28,401 traffic measurement files
   - Each file contains traffic detector readings for multiple time periods
   - File naming: `YYYYMMDD-HHMM-rawSpeedVol-all.xml`
   - Example: `20250826-0959-rawSpeedVol-all.xml`

2. **Location CSV** (Locations_of_Traffic_Detectors.gdb_converted.csv)
   - Geographic information for 786 traffic detectors
   - Contains: District, Road names, GPS coordinates

### XML Structure

Each XML file has a nested hierarchy:
```
raw_speed_volume_list (root)
  ├── date (e.g., "2025-08-26")
  └── periods (multiple time periods)
       └── period
            ├── period_from (e.g., "09:51:00")
            ├── period_to (e.g., "09:51:30")
            └── detectors (multiple detectors)
                 └── detector
                      ├── detector_id (e.g., "AID01101")
                      ├── direction (e.g., "South East")
                      └── lanes (multiple lanes)
                           └── lane
                                ├── lane_id (e.g., "Fast Lane", "Middle Lane", "Slow Lane")
                                ├── speed (km/h)
                                ├── occupancy (%)
                                ├── volume (vehicle count)
                                ├── s.d. (standard deviation)
                                └── valid (Y/N)
```

### Processing Steps

#### Step 1: Initialize Spark Session
```python
spark = SparkSession.builder \
    .appName("Traffic XML to CSV Converter") \
    .config("spark.driver.memory", "20g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()
```
- Creates a Spark cluster (even on a single machine)
- Allocates memory for distributed processing
- Driver: Coordinates the work (20GB)
- Executor: Does the actual computation (8GB)

#### Step 2: Load XML Files
```python
road_data = spark.read.format("xml") \
    .option("rowTag", "raw_speed_volume_list") \
    .load(xml_paths)
```
- Reads all 28,401 XML files in parallel
- Uses Spark's built-in XML reader
- Each file becomes one row in the dataframe
- Preserves the nested structure

#### Step 3: Flatten Nested Structure

**3a. Explode Periods**
```python
period_df = road_data.select(
    col("date"),
    explode(col("periods.period")).alias("period_details")
)
```
- Converts: 1 XML file → Multiple period rows
- Each file has ~140 periods (30-second intervals)

**3b. Explode Detectors**
```python
exploded_detector_df = period_df.select(
    col("date"),
    col("period_details.period_from"),
    explode(col("period_details.detectors.detector"))
)
```
- Converts: 1 period → Multiple detector rows
- Each period has ~779 detectors across Hong Kong

**3c. Explode Lanes**
```python
exploded_lane_df = exploded_detector_df.select(
    col("detector_details.detector_id"),
    explode(col("detector_details.lanes.lane"))
)
```
- Converts: 1 detector → Multiple lane rows (typically 3-4 lanes)
- Creates one row per lane measurement

**3d. Flatten Final Structure**
```python
traffic_df = exploded_lane_df.select(
    col("detector_id"),
    col("lane_details.speed"),
    col("lane_details.volume"),
    ...
)
```
- Extracts all nested fields to top-level columns
- Cleans timestamps from strings to proper timestamp format
- Result: ~118 million flat rows

#### Step 4: Join with Location Data
```python
merged_df = traffic_df.join(
    geolocation_df,
    traffic_df["detector_id"] == geolocation_df["AID_ID_Number"],
    "left"
)
```
- Adds geographic information to each traffic record
- Left join: Keeps all traffic records even if location is missing
- Enriches data with: District, Road name, GPS coordinates

#### Step 5: Save Results

**Single CSV File**
```python
merged_df.coalesce(1).write.csv("traffic_data_merged.csv", header=True)
```
- Combines all data into one CSV file
- `coalesce(1)`: Reduces to single partition
- Good for: Small datasets, easy sharing

**Partitioned by Date**
```python
merged_df.write.partitionBy("date").csv("traffic_data_partitioned", header=True)
```
- Creates separate directories for each date
- Structure: `traffic_data_partitioned/date=2025-08-26/part-*.csv`
- Good for: Query performance, filtering by date

### Output Schema

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| detector_id | string | Traffic detector ID | AID01101 |
| direction | string | Traffic direction | South East |
| lane_id | string | Lane identifier | Fast Lane |
| speed | int | Average speed (km/h) | 62 |
| occupancy | int | Lane occupancy (%) | 8 |
| volume | int | Vehicle count | 5 |
| standard_deviation | double | Speed std dev | 14.6 |
| period_from | timestamp | Start time | 2025-08-26 09:51:00 |
| period_to | timestamp | End time | 2025-08-26 09:51:30 |
| date | string | Measurement date | 2025-08-26 |
| District | string | District name | Southern |
| Road_EN | string | Road name (English) | Aberdeen Praya Road |
| Road_TC | string | Road name (Traditional Chinese) | 香港仔海旁道 |
| Road_SC | string | Road name (Simplified Chinese) | 香港仔海旁道 |
| Rotation | int | Detector rotation angle | 100 |
| GeometryEasting | double | GPS coordinate | 833758 |
| GeometryNorthing | double | GPS coordinate | 812147 |

## Environment Requirements

### Hardware Requirements

**Minimum:**
- CPU: 4 cores
- RAM: 16GB
- Disk: 50GB free space

**Recommended (your system):**
- CPU: 6 cores
- RAM: 32GB
- Disk: 100GB free space

### Software Requirements

#### 1. Operating System
- Linux (Ubuntu 20.04+, CentOS 7+)
- macOS 10.14+
- Windows 10+ with WSL2

#### 2. Python Environment
```bash
Python: 3.8 - 3.12
Required packages:
  - pyspark >= 4.0.0
```

#### 3. Java Runtime
```bash
Java: OpenJDK 11 or 17
Check: java -version
```
PySpark requires Java to run Spark's JVM backend.

#### 4. Installation Commands

**Using Conda (Recommended):**
```bash
conda create -n BDA python=3.12
conda activate BDA
pip install pyspark==4.0.1
```

**Using pip:**
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate  # Windows
pip install pyspark==4.0.1
```

### Runtime Environment

#### Spark Execution Mode: Local Mode

This code runs in **Spark Local Mode**:
- No cluster setup required
- Runs on a single machine
- Simulates distributed processing using threads
- Format: `local[*]` means use all available CPU cores

#### Memory Configuration

The code configures:
```python
.config("spark.driver.memory", "20g")    # Coordinator process
.config("spark.executor.memory", "8g")   # Worker process
```

**Total allocation: ~28GB out of 32GB**
- Leaves 4GB for OS and other processes
- Driver: Manages coordination, stores small data
- Executor: Does heavy computation, processes partitions

#### Parallelism Settings

```python
.config("spark.sql.shuffle.partitions", "12")  # 6 cores × 2
.config("spark.default.parallelism", "12")
```
- Data is split into 12 partitions
- Each core can process 2 partitions concurrently
- Optimal for 6-core CPU

### File System

The code uses:
- **Local filesystem**: `file://` protocol
- Absolute paths: `/home/richard/project/BDA/pyspark/`
- No HDFS required (though Spark supports it)

### Execution Flow

```
┌─────────────────────────────────────────────────────────┐
│ Python Process (main)                                   │
│ - Reads XML file list                                   │
│ - Initializes SparkSession                              │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│ Spark Driver (JVM Process)                              │
│ - Coordinates execution                                 │
│ - Manages metadata                                      │
│ - Stores small results                                  │
│ Memory: 20GB                                            │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│ Spark Executors (JVM Threads - Local Mode)             │
│ ┌─────────┐ ┌─────────┐ ┌─────────┐                   │
│ │Thread 1 │ │Thread 2 │ │Thread...│ (up to 6 cores)   │
│ │Parse XML│ │Parse XML│ │Parse XML│                   │
│ │Explode  │ │Explode  │ │Explode  │                   │
│ │Join     │ │Join     │ │Join     │                   │
│ └─────────┘ └─────────┘ └─────────┘                   │
│ Memory: 8GB (shared by all threads)                     │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│ Output Files                                            │
│ - traffic_data_merged.csv/                              │
│ - traffic_data_partitioned/                             │
└─────────────────────────────────────────────────────────┘
```

## Why PySpark Instead of Pandas?

| Aspect | Pandas | PySpark |
|--------|--------|---------|
| Memory | Must fit in RAM | Spills to disk automatically |
| Processing | Single-threaded | Multi-threaded/distributed |
| 118M rows | ❌ Out of Memory | ✅ Handles easily |
| Cluster support | ❌ No | ✅ Yes (can scale to 100+ machines) |
| Learning curve | Easy | Moderate |

For this dataset (28,401 files → 118 million rows), PySpark is essential.

## Performance Characteristics

### Expected Runtime (on your 6-core, 32GB system)

- **XML Loading**: ~5-10 minutes (reading 28,401 files)
- **Flattening**: ~10-15 minutes (exploding nested structures)
- **Joining**: ~2-3 minutes (adding location data)
- **CSV Writing**: ~15-20 minutes (single file) or ~5-10 minutes (partitioned)

**Total: 30-50 minutes**

### Disk Usage

- Input XML: ~20GB
- Output CSV (single): ~23GB
- Output CSV (partitioned): ~23GB
- Temporary files during processing: ~10-15GB
- **Total required: ~80GB free space**

### Memory Usage Pattern

```
Time ──────────────────────────────────────────────►
      Loading   Exploding    Joining    Writing
      ═══       ═════════    ═══════    ════════
RAM   ▓▓▓       ▓▓▓▓▓▓▓▓     ▓▓▓▓▓▓     ▓▓▓▓
      8GB       20-25GB      15-20GB    10GB
```

## Troubleshooting

### Memory Warnings
```
WARN MemoryStore: Not enough space to cache...
```
**Normal behavior** - Spark automatically uses disk for overflow.

### OutOfMemoryError
Reduce memory settings:
```python
.config("spark.driver.memory", "16g")
.config("spark.executor.memory", "6g")
```

### Slow Performance
Adjust parallelism:
```python
.config("spark.sql.shuffle.partitions", "24")  # Increase
```

### File Not Found
Check paths are absolute:
```python
xml_path = f"file://{os.path.abspath(dir)}/*.xml"
```

## Data Source

- **Provider**: Hong Kong Transport Department
- **URL**: https://data.gov.hk/
- **Update Frequency**: Real-time (30-second intervals)
- **Coverage**: 779 traffic detectors across Hong Kong
- **Data Format**: XML (SpeedVolOcc-BR schema)

## Use Cases for Output Data

1. **Traffic Analysis**: Identify congestion patterns
2. **Urban Planning**: Optimize road infrastructure
3. **Machine Learning**: Predict traffic flow
4. **Visualization**: Create traffic heatmaps
5. **Business Intelligence**: Route optimization for logistics

## License

This code processes Hong Kong government open data, which is freely available for public use.
