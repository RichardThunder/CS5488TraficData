# Benchmark Test Framework

A flexible, extensible Python framework for benchmarking data processing tasks across different engines (PySpark, Hive, Pandas) using the Template Method design pattern.

## Architecture Overview

The framework consists of three main components:

### 1. BaseAnalysisTest (The Blueprint)

An abstract class that defines the structure every benchmark test must follow:

- `initialize()` - Set up the test environment
- `read_data()` - Load data from sources
- `clean_data()` - Preprocess and clean the data
- `execute_analysis()` - Perform the actual analysis
- `cleanup()` - Clean up resources

The `run()` method orchestrates these steps in order, times each phase, and collects results.

### 2. Concrete Test Classes (The Implementations)

Specific implementations for different engines and analysis types:

**Busy Road Analysis:**
- `SparkBusyRoadTest` - PySpark implementation
- `HiveBusyRoadTest` - HiveQL implementation
- `PandasBusyRoadTest` - Pandas implementation

**Congestion Analysis:**
- `SparkCongestionTest` - PySpark implementation
- `HiveCongestionTest` - Hive implementation
- `PandasCongestionTest` - Pandas implementation

### 3. run_benchmarks.py (The Test Harness)

The main script that:
- Initializes SparkSession
- Iterates through datasets
- Runs configured tests
- Collects and exports timing results to CSV

## Quick Start

### Basic Usage

```python
# Run all configured benchmarks
python benchmark/run_benchmarks.py
```

### Configuration

Edit `run_benchmarks.py` to configure:

```python
# Define datasets to test
DATASETS = {
    '1_month_202508': ['hdfs:///traffic_data_partitioned/202508'],
    '2_months': ['hdfs:///traffic_data_partitioned/202508',
                 'hdfs:///traffic_data_partitioned/202507'],
}

# Define which tests to run
TEST_CLASSES = [
    (SparkBusyRoadTest, "PySpark-BusyRoad", False),
    (HiveBusyRoadTest, "Hive-BusyRoad", False),
    (PandasBusyRoadTest, "Pandas-BusyRoad", True),  # Skip for large datasets
]

# Skip Pandas for datasets larger than this
PANDAS_SKIP_THRESHOLD = 2  # months
```

## Creating Custom Tests

### Example: New Analysis Type

```python
from benchmark.base_analysis_test import BaseAnalysisTest
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg
from typing import Dict, Any

class SparkAverageSpeedTest(BaseAnalysisTest):
    """Calculate average speed by road."""

    def initialize(self) -> None:
        """Initialize test - verify SparkSession exists."""
        if self.spark is None:
            raise ValueError("SparkSession required")
        logger.info("Average Speed test initialized")

    def read_data(self) -> DataFrame:
        """Read data from Parquet files."""
        return self.spark.read.parquet(*self.data_paths)

    def clean_data(self, data: DataFrame) -> DataFrame:
        """Remove invalid records."""
        return data.filter(
            col("Road_EN").isNotNull() &
            col("speed").isNotNull()
        )

    def execute_analysis(self, data: DataFrame) -> Dict[str, Any]:
        """Calculate average speed by road."""
        result_df = data.groupBy("Road_EN", "District") \
            .agg(avg("speed").alias("avg_speed")) \
            .orderBy(col("avg_speed").desc()) \
            .limit(10)

        result_df.show()

        return {
            'avg_speeds': result_df,
            'count': result_df.count()
        }

    def cleanup(self) -> None:
        """Optional cleanup."""
        pass
```

### Add to Test Harness

```python
# In run_benchmarks.py
from benchmark.concrete_tests import SparkAverageSpeedTest

TEST_CLASSES = [
    (SparkAverageSpeedTest, "PySpark-AvgSpeed", False),
    # ... other tests
]
```

## Running Individual Tests

You can also run tests programmatically:

```python
from pyspark.sql import SparkSession
from benchmark.concrete_tests import SparkBusyRoadTest

# Initialize Spark
spark = SparkSession.builder.appName("Test").getOrCreate()

# Create and run test
test = SparkBusyRoadTest(
    name="PySpark-BusyRoad",
    dataset_size="1_month",
    data_paths=['hdfs:///traffic_data_partitioned/202508'],
    spark=spark,
    data_size_bytes=1000000,
    total_records=500000
)

result = test.run()

print(f"Success: {result['success']}")
print(f"Total time: {result['total_time']:.2f}s")
print(f"Timing breakdown: {result['timing_results']}")
```

## Output

### Console Output

The harness provides detailed logging:

```
================================================================================
STARTING PYSPARK-BUSYROAD TEST - Dataset: 1_month_202508
================================================================================
[1_month_202508][PySpark-BusyRoad] Initialize: 0.001s | Records: 0 | test environment setup
[1_month_202508][PySpark-BusyRoad] Read Data: 2.345s | Records: 0 | from 1 path(s)
[1_month_202508][PySpark-BusyRoad] Clean Data: 0.567s | Records: 0 | data cleaning
[1_month_202508][PySpark-BusyRoad] Execute Analysis: 5.432s | Records: 0 | main analysis
...
PySpark-BusyRoad total execution time: 8.35 seconds

--------------------------------------------------------------------------------
SUMMARY FOR 1_month_202508
--------------------------------------------------------------------------------
PySpark-BusyRoad       :     8.35s
Hive-BusyRoad          :    10.23s
Pandas-BusyRoad        :    15.67s

Fastest: PySpark-BusyRoad (8.35s)
  PySpark-BusyRoad is 1.23x faster than Hive-BusyRoad
  PySpark-BusyRoad is 1.88x faster than Pandas-BusyRoad
```

### CSV Output

Results are exported to `benchmark_logs/benchmark_timing_results.csv`:

```csv
timestamp,dataset_size,data_size_bytes,total_records,tool,phase,duration_seconds,records_processed,details
2025-11-15T10:30:45,1_month_202508,1073741824,500000,PySpark-BusyRoad,Initialize,0.001,0,test environment setup
2025-11-15T10:30:47,1_month_202508,1073741824,500000,PySpark-BusyRoad,Read Data,2.345,0,from 1 path(s)
...
```

## File Structure

```
benchmark/
├── base_analysis_test.py      # Abstract base class
├── concrete_tests.py          # Concrete test implementations
├── run_benchmarks.py          # Main test harness
├── README_FRAMEWORK.md        # This file
├── queries/                   # SQL query files for Hive tests
│   ├── morning_rush.hql
│   └── evening_rush.hql
└── benchmark_logs/            # Output directory (auto-created)
    └── benchmark_timing_results.csv
```

## Design Patterns Used

### Template Method Pattern

The `BaseAnalysisTest` class implements the Template Method pattern:
- Defines a skeleton algorithm (`run()` method)
- Delegates specific steps to subclasses (`initialize()`, `read_data()`, etc.)
- Controls the overall flow while allowing customization

### Benefits:
- Eliminates code duplication
- Enforces consistent structure across tests
- Makes it easy to add new analysis types or engines
- Timing and logging handled automatically

## Advanced Features

### Custom Timing Phases

Add custom timing records within your analysis:

```python
def execute_analysis(self, data: DataFrame) -> Dict[str, Any]:
    import time

    # Phase 1
    start = time.time()
    result1 = self._some_operation(data)
    self.record_timing("Custom Phase 1", time.time() - start,
                      records=100, details="my details")

    # Phase 2
    start = time.time()
    result2 = self._another_operation(data)
    self.record_timing("Custom Phase 2", time.time() - start)

    return {'result1': result1, 'result2': result2}
```

### Conditional Test Execution

Control which tests run based on dataset size:

```python
TEST_CLASSES = [
    (SparkTest, "Spark", False),           # Always run
    (PandasTest, "Pandas", True),          # Skip for large datasets
    (HeavyTest, "Heavy", lambda size: size > 5),  # Custom condition
]
```

### Resource Cleanup

Override `cleanup()` for resource management:

```python
def cleanup(self) -> None:
    """Clean up cached data."""
    if self.cleaned_data is not None:
        self.cleaned_data.unpersist()
    if self.temp_view_created:
        self.spark.catalog.dropTempView("my_view")
```

## Troubleshooting

### Pandas Out of Memory

Increase `PANDAS_SKIP_THRESHOLD` or add more memory:

```python
PANDAS_SKIP_THRESHOLD = 1  # Only run Pandas on very small datasets
```

### Hive Query File Not Found

Ensure SQL files exist in the correct directory:

```python
# In HiveBusyRoadTest.__init__:
def __init__(self, *args, sql_dir: str = "benchmark/queries", **kwargs):
    super().__init__(*args, **kwargs)
    self.sql_dir = sql_dir
```

### HDFS Connection Issues

Check Spark configuration and HDFS availability:

```python
# Test HDFS connectivity
spark.read.parquet("hdfs:///traffic_data_partitioned/202508").count()
```

## Best Practices

1. **Keep Tests Focused**: Each test class should implement one type of analysis
2. **Use Descriptive Names**: Name tests clearly (e.g., `SparkBusyRoadTest`, not `Test1`)
3. **Handle Errors Gracefully**: The framework catches exceptions, but log useful information
4. **Clean Up Resources**: Always implement `cleanup()` for expensive resources
5. **Document Custom Tests**: Add docstrings explaining what each test does
6. **Test Incrementally**: Start with small datasets, then scale up

## Contributing

To add a new test:

1. Create a new class inheriting from `BaseAnalysisTest`
2. Implement all abstract methods
3. Add the test to `TEST_CLASSES` in `run_benchmarks.py`
4. Test with a small dataset first
5. Document any special requirements

## License

Part of the BDA/pyspark project.
