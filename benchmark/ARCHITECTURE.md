# Benchmark Framework Architecture

## Design Pattern: Template Method

The framework uses the **Template Method** design pattern to provide a consistent structure for benchmarking while allowing flexibility in implementation.

## Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     run_benchmarks.py                           │
│                      (Test Harness)                             │
│                                                                 │
│  - Initializes SparkSession                                    │
│  - Configures datasets and tests                               │
│  - Runs tests in loop                                          │
│  - Collects and exports results to CSV                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ creates & runs
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  BaseAnalysisTest                               │
│                  (Abstract Blueprint)                           │
│                                                                 │
│  Abstract Methods (must implement):                            │
│    - initialize()        : Set up test environment             │
│    - read_data()         : Load data from sources              │
│    - clean_data()        : Preprocess data                     │
│    - execute_analysis()  : Perform analysis                    │
│                                                                 │
│  Optional Method:                                               │
│    - cleanup()           : Clean up resources                  │
│                                                                 │
│  Template Method (pre-implemented):                            │
│    - run()               : Orchestrates all steps & timing     │
│                                                                 │
│  Utility Methods:                                               │
│    - record_timing()     : Log phase timings                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ inherited by
                         ▼
        ┌────────────────┴────────────────┐
        │                                 │
        ▼                                 ▼
┌──────────────────┐            ┌──────────────────┐
│ Busy Road Tests  │            │ Congestion Tests │
│                  │            │                  │
│ ├─ Spark         │            │ ├─ Spark         │
│ ├─ Hive          │            │ ├─ Hive          │
│ └─ Pandas        │            │ └─ Pandas        │
└──────────────────┘            └──────────────────┘
```

## Execution Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. START: run_benchmarks.py                                     │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. Initialize SparkSession                                      │
│    - Configure Spark settings                                   │
│    - Enable Hive support                                        │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. FOR EACH Dataset:                                            │
│    ┌─────────────────────────────────────────────────────────┐ │
│    │ 3a. Get dataset statistics (size, record count)         │ │
│    └─────────────────────────┬───────────────────────────────┘ │
│                              ▼                                  │
│    ┌─────────────────────────────────────────────────────────┐ │
│    │ 3b. FOR EACH Test Class:                                │ │
│    │    ┌────────────────────────────────────────────────┐   │ │
│    │    │ i. Check if test should be skipped (size)      │   │ │
│    │    └──────────────────┬─────────────────────────────┘   │ │
│    │                       ▼                                  │ │
│    │    ┌────────────────────────────────────────────────┐   │ │
│    │    │ ii. Create test instance                       │   │ │
│    │    │     new TestClass(name, dataset, paths, ...)   │   │ │
│    │    └──────────────────┬─────────────────────────────┘   │ │
│    │                       ▼                                  │ │
│    │    ┌────────────────────────────────────────────────┐   │ │
│    │    │ iii. Call test.run()  ←─────────────────┐      │   │ │
│    │    │                                         │      │   │ │
│    │    │      [BaseAnalysisTest.run()]           │      │   │ │
│    │    │      ┌──────────────────────────┐       │      │   │ │
│    │    │      │ 1. initialize()          │ ◄ Time│      │   │ │
│    │    │      ├──────────────────────────┤       │      │   │ │
│    │    │      │ 2. read_data()           │ ◄ Time│      │   │ │
│    │    │      ├──────────────────────────┤       │      │   │ │
│    │    │      │ 3. clean_data()          │ ◄ Time│      │   │ │
│    │    │      ├──────────────────────────┤       │      │   │ │
│    │    │      │ 4. execute_analysis()    │ ◄ Time│      │   │ │
│    │    │      ├──────────────────────────┤       │      │   │ │
│    │    │      │ 5. cleanup()             │ ◄ Time│      │   │ │
│    │    │      └──────────────────────────┘       │      │   │ │
│    │    │                                         │      │   │ │
│    │    │      Returns: {                         │      │   │ │
│    │    │        'success': bool,                 │      │   │ │
│    │    │        'total_time': float,             │      │   │ │
│    │    │        'timing_results': [...],         │      │   │ │
│    │    │        'analysis_results': {...}        │      │   │ │
│    │    │      }                                   │      │   │ │
│    │    └────────────────────────────────────┬────┴──────┘   │ │
│    │                                         ▼                │ │
│    │    ┌────────────────────────────────────────────────┐   │ │
│    │    │ iv. Collect timing results                     │   │ │
│    │    └────────────────────────────────────────────────┘   │ │
│    └──────────────────────────────────────────────────────┘ │
│                              ▼                                  │
│    ┌─────────────────────────────────────────────────────────┐ │
│    │ 3c. Print dataset summary                               │ │
│    │     - List all test results                             │ │
│    │     - Identify fastest test                             │ │
│    │     - Calculate speedup ratios                          │ │
│    └─────────────────────────────────────────────────────────┘ │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. Print overall summary                                        │
│    - All datasets and their results                             │
│    - Fastest test for each dataset                              │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. Export to CSV                                                │
│    - benchmark_logs/benchmark_timing_results.csv                │
│    - All timing records from all tests                          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. Cleanup & Stop SparkSession                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Class Hierarchy

```
BaseAnalysisTest (Abstract)
│
├─ SparkBusyRoadTest
│  └─ Uses: PySpark DataFrames
│  └─ Analysis: Top 10 busiest roads (morning & evening)
│
├─ HiveBusyRoadTest
│  └─ Uses: HiveQL queries (loaded from .hql files)
│  └─ Analysis: Top 10 busiest roads (morning & evening)
│
├─ PandasBusyRoadTest
│  └─ Uses: Pandas DataFrames
│  └─ Analysis: Top 10 busiest roads (morning & evening)
│
├─ SparkCongestionTest
│  └─ Uses: PySpark DataFrames
│  └─ Analysis: High congestion roads (>50% occupancy)
│
├─ HiveCongestionTest
│  └─ Uses: HiveQL queries
│  └─ Analysis: High congestion roads (>50% occupancy)
│
├─ PandasCongestionTest
│  └─ Uses: Pandas DataFrames
│  └─ Analysis: High congestion roads (>50% occupancy)
│
└─ [Your Custom Test]
   └─ Implement the 4 abstract methods
   └─ Add to TEST_CLASSES in run_benchmarks.py
```

## Data Flow

```
HDFS Parquet Files
       │
       │ read_data()
       ▼
  Raw DataFrame
       │
       │ clean_data()
       ▼
 Cleaned DataFrame
       │
       │ execute_analysis()
       ▼
 Analysis Results
       │
       │ (collected by run())
       ▼
  Timing Records
       │
       │ (aggregated by harness)
       ▼
    CSV Export
```

## Timing Collection

Each test automatically records timing for these phases:

```
┌─────────────────┬──────────────────────────────────────────┐
│ Phase           │ What's Measured                          │
├─────────────────┼──────────────────────────────────────────┤
│ Initialize      │ Test setup (connections, config, etc.)   │
│ Read Data       │ Loading data from HDFS/files             │
│ Clean Data      │ Filtering nulls, preprocessing           │
│ Execute Analysis│ Main analysis computation                │
│ Cleanup         │ Resource cleanup (optional)              │
│ Total Execution │ Sum of all phases                        │
└─────────────────┴──────────────────────────────────────────┘
```

Additional custom phases can be recorded within `execute_analysis()` using `self.record_timing()`.

## Configuration Points

### In run_benchmarks.py:

```python
# 1. Dataset Configuration
DATASETS = {
    'name': ['path1', 'path2', ...]
}

# 2. Test Selection
TEST_CLASSES = [
    (TestClass, "display_name", skip_large_datasets_bool)
]

# 3. Pandas Threshold
PANDAS_SKIP_THRESHOLD = 2  # months

# 4. Spark Configuration
def initialize_spark():
    return SparkSession.builder \
        .config("key", "value") \
        # ... more configs
```

### In Concrete Test Classes:

```python
class MyTest(BaseAnalysisTest):
    def __init__(self, *args, custom_param="default", **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_param = custom_param
```

## Error Handling

```
┌────────────────────────────────────────────────────────┐
│ BaseAnalysisTest.run()                                 │
│                                                        │
│  try:                                                  │
│    ┌─────────────────────────────────────────────┐    │
│    │ Run all phases                              │    │
│    │ Record timing for each                      │    │
│    └─────────────────────────────────────────────┘    │
│  except Exception as e:                               │
│    ┌─────────────────────────────────────────────┐    │
│    │ Log error                                   │    │
│    │ Set success = False                         │    │
│    │ Record error in timing                      │    │
│    └─────────────────────────────────────────────┘    │
│  finally:                                              │
│    ┌─────────────────────────────────────────────┐    │
│    │ Try to run cleanup()                        │    │
│    │ Record total time                           │    │
│    │ Return results (even if failed)             │    │
│    └─────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────┘

Harness continues even if individual tests fail
```

## Extension Points

### 1. Add New Analysis Type

```python
class MyCustomTest(BaseAnalysisTest):
    # Implement abstract methods
    pass

# Add to run_benchmarks.py
TEST_CLASSES.append((MyCustomTest, "Custom", False))
```

### 2. Add New Engine

```python
class DaskBusyRoadTest(BaseAnalysisTest):
    # Implement using Dask instead of Spark/Pandas
    pass
```

### 3. Custom Timing Phases

```python
def execute_analysis(self, data):
    import time

    start = time.time()
    # ... some work
    self.record_timing("Custom Phase", time.time() - start)
```

### 4. Custom Output Format

Modify `export_timing_to_csv()` or create new export functions.

## Benefits of This Architecture

1. **Consistency**: All tests follow the same structure
2. **Reusability**: Common timing/logging logic in base class
3. **Extensibility**: Easy to add new tests or engines
4. **Maintainability**: Clear separation of concerns
5. **Flexibility**: Override any method for custom behavior
6. **Testability**: Each test is independent and self-contained

## Anti-Patterns to Avoid

1. ❌ Don't skip calling `super().__init__()` in custom tests
2. ❌ Don't modify `self.timing_results` directly (use `record_timing()`)
3. ❌ Don't forget to implement all abstract methods
4. ❌ Don't perform heavy computation in `__init__()`
5. ❌ Don't forget to cleanup resources in `cleanup()`
