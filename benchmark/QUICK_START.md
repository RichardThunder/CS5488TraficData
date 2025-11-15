# Quick Start Guide

## Installation

No additional installation required. The framework uses standard PySpark, Pandas, and Python libraries.

## Basic Usage

### Run All Benchmarks

```bash
cd /home/richard/project/BDA/pyspark
python benchmark/run_benchmarks.py
```

This will:
1. Run all configured tests on all configured datasets
2. Print results to console
3. Export timing data to `benchmark_logs/benchmark_timing_results.csv`

### Run a Single Test

```python
from pyspark.sql import SparkSession
from benchmark.concrete_tests import SparkBusyRoadTest

# Initialize Spark
spark = SparkSession.builder.appName("SingleTest").getOrCreate()

# Create test
test = SparkBusyRoadTest(
    name="PySpark-BusyRoad",
    dataset_size="1_month_202508",
    data_paths=['hdfs:///traffic_data_partitioned/202508'],
    spark=spark,
    data_size_bytes=1073741824,
    total_records=500000
)

# Run test
result = test.run()

# Check results
if result['success']:
    print(f"Test completed in {result['total_time']:.2f} seconds")
else:
    print(f"Test failed: {result['error']}")

spark.stop()
```

## Configuration

### Add/Remove Datasets

Edit `run_benchmarks.py`:

```python
DATASETS = {
    '1_month': ['hdfs:///traffic_data_partitioned/202508'],
    '2_months': ['hdfs:///traffic_data_partitioned/202508',
                 'hdfs:///traffic_data_partitioned/202507'],
}
```

### Select Tests to Run

Edit `run_benchmarks.py`:

```python
TEST_CLASSES = [
    (SparkBusyRoadTest, "PySpark-BusyRoad", False),
    (HiveBusyRoadTest, "Hive-BusyRoad", False),
    # Comment out to skip:
    # (PandasBusyRoadTest, "Pandas-BusyRoad", True),
]
```

### Adjust Pandas Memory Limit

Edit `run_benchmarks.py`:

```python
# Skip Pandas for datasets larger than X months
PANDAS_SKIP_THRESHOLD = 2
```

## Create Custom Test

### 1. Create Test Class

```python
from benchmark.base_analysis_test import BaseAnalysisTest
from pyspark.sql import DataFrame
from typing import Dict, Any

class MyCustomTest(BaseAnalysisTest):
    """My custom analysis."""

    def initialize(self) -> None:
        """Setup."""
        if self.spark is None:
            raise ValueError("SparkSession required")

    def read_data(self) -> DataFrame:
        """Read data."""
        return self.spark.read.parquet(*self.data_paths)

    def clean_data(self, data: DataFrame) -> DataFrame:
        """Clean data."""
        return data.filter(col("Road_EN").isNotNull())

    def execute_analysis(self, data: DataFrame) -> Dict[str, Any]:
        """Run analysis."""
        # Your analysis code here
        result = data.groupBy("District").count()
        return {'district_counts': result}

    def cleanup(self) -> None:
        """Cleanup (optional)."""
        pass
```

### 2. Add to Test Harness

In `run_benchmarks.py`:

```python
from benchmark.concrete_tests import MyCustomTest

TEST_CLASSES = [
    (MyCustomTest, "MyTest", False),
    # ... other tests
]
```

### 3. Run

```bash
python benchmark/run_benchmarks.py
```

## Common Tasks

### View Results

Results are saved to CSV:

```bash
cat benchmark_logs/benchmark_timing_results.csv
```

Or use pandas:

```python
import pandas as pd
df = pd.read_csv('benchmark_logs/benchmark_timing_results.csv')
print(df)
```

### Filter Results by Test

```python
import pandas as pd
df = pd.read_csv('benchmark_logs/benchmark_timing_results.csv')
spark_results = df[df['tool'] == 'PySpark-BusyRoad']
print(spark_results)
```

### Compare Execution Times

```python
import pandas as pd
df = pd.read_csv('benchmark_logs/benchmark_timing_results.csv')

# Get total execution times
totals = df[df['phase'] == 'Total Execution']
pivot = totals.pivot_table(
    index='dataset_size',
    columns='tool',
    values='duration_seconds'
)
print(pivot)
```

### Analyze Phase Breakdown

```python
import pandas as pd
df = pd.read_csv('benchmark_logs/benchmark_timing_results.csv')

# Get breakdown for specific test
test_data = df[
    (df['tool'] == 'PySpark-BusyRoad') &
    (df['dataset_size'] == '1_month_202508') &
    (df['phase'] != 'Total Execution')
]

print(test_data[['phase', 'duration_seconds', 'details']])
```

## Troubleshooting

### HDFS Connection Error

```
IOError: Failed to get FileSystem for path
```

**Solution**: Check HDFS is running and paths are correct:

```bash
hdfs dfs -ls /traffic_data_partitioned/202508
```

### Pandas Out of Memory

```
MemoryError: Unable to allocate array
```

**Solution**: Increase `PANDAS_SKIP_THRESHOLD` or skip Pandas for large datasets.

### Hive Query File Not Found

```
FileNotFoundError: [Errno 2] No such file or directory: 'benchmark/queries/morning_rush.hql'
```

**Solution**: Ensure SQL files exist:

```bash
ls benchmark/queries/
```

Create missing files:

```bash
mkdir -p benchmark/queries
cat > benchmark/queries/morning_rush.hql << 'EOF'
SELECT
    Road_EN,
    District,
    SUM(volume) as total_volume,
    SUM(occupancy) as total_occupancy
FROM traffic_data
WHERE HOUR(period_from) >= 6 AND HOUR(period_from) < 10
GROUP BY Road_EN, District
ORDER BY total_volume DESC
LIMIT 10
EOF
```

### Test Fails Silently

Check the timing results for error details:

```python
import pandas as pd
df = pd.read_csv('benchmark_logs/benchmark_timing_results.csv')
errors = df[df['details'].str.contains('FAILED', na=False)]
print(errors[['tool', 'dataset_size', 'details']])
```

## Next Steps

- Read `README_FRAMEWORK.md` for detailed documentation
- View `ARCHITECTURE.md` for design details
- See `example_usage.py` for more examples
- Check `benchmark.py` for the original implementation

## File Structure

```
benchmark/
├── __init__.py                    # Package initialization
├── base_analysis_test.py          # Abstract base class
├── concrete_tests.py              # Test implementations
├── run_benchmarks.py              # Main test harness
├── benchmark.py                   # Original implementation
├── README_FRAMEWORK.md            # Full documentation
├── ARCHITECTURE.md                # Architecture details
├── QUICK_START.md                 # This file
├── example_usage.py               # Usage examples
├── queries/                       # SQL query files
│   ├── morning_rush.hql
│   └── evening_rush.hql
└── benchmark_logs/                # Output directory
    └── benchmark_timing_results.csv
```

## Tips

1. **Start Small**: Test with 1-month dataset first
2. **Check Resources**: Monitor memory/CPU during execution
3. **Use Appropriate Tools**: Pandas for small data, Spark for large
4. **Clean Cache**: Clear Spark cache between runs if needed
5. **Version Control**: Commit CSV results for comparison over time
