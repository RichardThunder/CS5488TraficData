
# Big Data Analysis of Hong Kong Traffic

---

## Project Overview

**Goal:** Analyze Hong Kong traffic data to identify patterns and benchmark the performance of different big data tools.

**Core Objectives:**
-   **Store** large-scale traffic data in Hadoop HDFS.
-   **Process and Analyze** the data using Apache Spark.
-   **Benchmark** computation across different tools:
    -   PySpark (Parallel)
    -   HiveQL (SQL-based)
    -   Pandas (Serialized)

---

## System Architecture

-   **Data Storage:** Hadoop HDFS
    -   1 Namenode VM
    -   3 Datanode VMs (15GB RAM, 8 cores each)
-   **Resource Management:** YARN
-   **Processing:** Apache Spark
-   **Environment:**
    -   Ubuntu 24
    -   16 Cores, 32 vCores, 64GB RAM

---

## Data Preprocessing

A PySpark job (`1_xml_to_csv.py`) handles the initial ETL process:

1.  **Reads** raw XML traffic data.
2.  **Flattens** the nested XML structure.
3.  **Filters** invalid records (`valid='Y'`).
4.  **Enriches** data by joining with a geolocation dataset.
5.  **Writes** the cleaned and merged data to HDFS in Parquet format with Snappy compression.

This creates a clean, partitioned dataset ready for analysis.

---

## Analysis (Work in Progress)

The analysis portion of the project is planned but not yet fully implemented.

**Planned analysis includes:**
-   Data Quality EDA
-   Temporal Patterns (e.g., rush hours)
-   Spatial Congestion (e.g., bottleneck areas)
-   Traffic Flow Theory application

---

## Benchmarking Framework

A robust benchmarking framework was built to compare the performance of different data processing engines.

-   **Design:** Uses the **Template Method Pattern** (`BaseAnalysisTest`).
-   **Orchestration:** A test harness (`run_benchmarks.py`) executes tests across different datasets and tools.
-   **Metrics Collected:**
    -   Initialization time
    -   Data read time
    -   Data cleaning time
    -   Analysis execution time
    -   Total execution time

---

## Benchmark Tests

Two main analysis scenarios were benchmarked:

1.  **Busy Road Analysis:**
    -   **Goal:** Find the top 10 busiest roads during morning and evening rush hours.
    -   **Engines:** PySpark, Hive, Pandas

2.  **Congestion Analysis:**
    -   **Goal:** Identify roads with high congestion (occupancy > 50%).
    -   **Engines:** PySpark, Hive, Pandas

---

## Benchmark Results

| Tool                | Avg. Execution Time |
| ------------------- | ------------------- |
| **Hive-Congestion** | **3.73s**           |
| Hive-BusyRoad       | 9.02s               |
| PySpark-Congestion  | 18.40s              |
| PySpark-BusyRoad    | 56.49s              |
| Pandas-BusyRoad     | 61.38s              |
| Pandas-Congestion   | 110.43s             |

**Key Findings:**
-   **Hive** consistently outperformed PySpark and Pandas for these specific queries.
-   **Pandas** was significantly slower, especially in the data cleaning phase, highlighting the limitations of single-node processing for large datasets.
-   **Hive** also showed the best scaling efficiency as the dataset size increased.

---

## Conclusion

-   **Successfully** built a data pipeline to process and store large-scale traffic data.
-   **Developed** a flexible and extensible framework for benchmarking big data tools.
-   **Demonstrated** the performance differences between Spark, Hive, and Pandas for specific analytical tasks.
-   **Revealed** that for the tested SQL-like queries, Hive was the most performant tool in this environment.

**Future Work:**
-   Implement the planned traffic analysis modules.
-   Expand the benchmark suite with more complex analytical tasks.
-   Visualize the analysis results (e.g., on a map of Hong Kong).
