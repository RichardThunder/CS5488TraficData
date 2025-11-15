from benchmark.base_analysis_test import BaseAnalysisTest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, hour, sum as spark_sum, desc
from pyspark import StorageLevel
import pandas as pd
import logging
import os
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class SparkBusyRoadTest(BaseAnalysisTest):
    """
    PySpark implementation for finding top 10 busiest roads during rush hours.

    This test uses PySpark DataFrames and SQL functions to analyze traffic data
    and identify the busiest roads during morning (6:00-10:00) and evening
    (17:00-21:00) rush hours.
    """

    def initialize(self) -> None:
        """Initialize - PySpark doesn't need additional initialization beyond SparkSession."""
        if self.spark is None:
            raise ValueError("SparkSession is required for PySpark tests")
        logger.info("PySpark test initialized")

    def read_data(self) -> DataFrame:
        """Read data from Parquet files using Spark."""
        logger.info(f"Reading data from {self.data_paths}")
        df = self.spark.read.parquet(self.data_paths)
        return df

    def clean_data(self, data: DataFrame) -> DataFrame:
        """Clean data by removing null Road_EN values."""
        logger.info("Cleaning data: removing null Road_EN values")
        cleaned_df = data.filter(col("Road_EN").isNotNull())

        # Cache the cleaned data
        cleaned_df.persist(StorageLevel.MEMORY_AND_DISK)
        record_count = cleaned_df.count()
        logger.info(f"Data cleaned and cached. Records: {record_count:,}")

        return cleaned_df

    def execute_analysis(self, data: DataFrame) -> Dict[str, Any]:
        """Execute the busy road analysis for morning and evening rush hours."""
        results = {}

        # Morning rush hour: 6:00 - 10:00
        logger.info("Analyzing morning rush hour (6:00-10:00)")
        morning_results = self._analyze_rush_hour(data, 6, 10, "Morning")
        results['morning'] = morning_results

        if morning_results['data'] is not None:
            logger.info("\n--- TOP 10 BUSIEST ROADS (Morning 6:00-10:00) ---")
            morning_results['data'].show(truncate=False)

        # Evening rush hour: 17:00 - 21:00
        logger.info("Analyzing evening rush hour (17:00-21:00)")
        evening_results = self._analyze_rush_hour(data, 17, 21, "Evening")
        results['evening'] = evening_results

        if evening_results['data'] is not None:
            logger.info("\n--- TOP 10 BUSIEST ROADS (Evening 17:00-21:00) ---")
            evening_results['data'].show(truncate=False)

        return results

    def _analyze_rush_hour(self, df: DataFrame, start_hour: int, end_hour: int,
                          period_name: str) -> Dict[str, Any]:
        """
        Find top 10 busiest roads during specified hours.

        Args:
            df: Input DataFrame with traffic data
            start_hour: Start hour of the period (0-23)
            end_hour: End hour of the period (0-23)
            period_name: Name of the period for logging

        Returns:
            Dictionary containing analysis results
        """
        result_df = df.filter(
            (hour(col("period_from")) >= start_hour) &
            (hour(col("period_from")) < end_hour)
        ).groupBy("Road_EN", "District") \
         .agg(
             spark_sum("volume").alias("total_volume"),
             spark_sum("occupancy").alias("total_occupancy")
         ) \
         .orderBy(desc("total_volume")) \
         .limit(10)

        count = result_df.count() if result_df is not None else 0

        return {
            'data': result_df,
            'count': count,
            'period': f"{start_hour}:00-{end_hour}:00"
        }

    def cleanup(self) -> None:
        """Unpersist cached data."""
        if self.cleaned_data is not None:
            self.cleaned_data.unpersist()
            logger.info("Unpersisted cached data")


class HiveBusyRoadTest(BaseAnalysisTest):
    """
    Hive implementation for finding top 10 busiest roads during rush hours.

    This test uses HiveQL queries loaded from external files to analyze traffic data.
    """

    def __init__(self, *args, sql_dir: str = "benchmark/queries", **kwargs):
        """
        Initialize Hive test.

        Args:
            sql_dir: Directory containing SQL query files
        """
        super().__init__(*args, **kwargs)
        self.sql_dir = sql_dir

    def initialize(self) -> None:
        """Initialize - Hive doesn't need additional initialization beyond SparkSession."""
        if self.spark is None:
            raise ValueError("SparkSession with Hive support is required for Hive tests")
        logger.info("Hive test initialized")

    def read_data(self) -> DataFrame:
        """Read data from Parquet files and create temp view."""
        logger.info(f"Reading data from {self.data_paths}")
        df = self.spark.read.parquet(self.data_paths)
        return df

    def clean_data(self, data: DataFrame) -> DataFrame:
        """Clean data and create temporary Hive view."""
        logger.info("Cleaning data and creating temporary Hive view")
        cleaned_df = data.filter(col("Road_EN").isNotNull())
        cleaned_df.createOrReplaceTempView("traffic_data")
        logger.info("Created temporary Hive view: traffic_data")
        return cleaned_df

    def execute_analysis(self, data: DataFrame) -> Dict[str, Any]:
        """Execute Hive queries for morning and evening rush hours."""
        results = {}

        # Morning query
        logger.info("\n--- HIVE: TOP 10 BUSIEST ROADS (Morning 6:00-10:00) ---")
        morning_query = self._load_sql_query("morning_rush.hql")
        morning_results = self.spark.sql(morning_query)
        morning_count = morning_results.count()
        morning_results.show(truncate=False)

        results['morning'] = {
            'data': morning_results,
            'count': morning_count,
            'query': morning_query
        }

        # Evening query
        logger.info("\n--- HIVE: TOP 10 BUSIEST ROADS (Evening 17:00-21:00) ---")
        evening_query = self._load_sql_query("evening_rush.hql")
        evening_results = self.spark.sql(evening_query)
        evening_count = evening_results.count()
        evening_results.show(truncate=False)

        results['evening'] = {
            'data': evening_results,
            'count': evening_count,
            'query': evening_query
        }

        return results

    def _load_sql_query(self, filename: str) -> str:
        """Load SQL query from file."""
        filepath = os.path.join(self.sql_dir, filename)
        with open(filepath, 'r') as f:
            return f.read()


class PandasBusyRoadTest(BaseAnalysisTest):
    """
    Pandas implementation for finding top 10 busiest roads during rush hours.

    This test uses Pandas DataFrames for serial computation. It's useful for
    comparing distributed processing (Spark/Hive) with single-machine processing.

    Note: Should only be used for small datasets to avoid memory issues.
    """

    def initialize(self) -> None:
        """Initialize - Pandas doesn't need special initialization."""
        if self.spark is None:
            raise ValueError("SparkSession is required to read data initially")
        logger.info("Pandas test initialized")

    def read_data(self) -> pd.DataFrame:
        """Read data using Spark then convert to Pandas."""
        logger.info(f"Reading data from {self.data_paths} via Spark")
        df_spark = self.spark.read.parquet(self.data_paths)
        return df_spark

    def clean_data(self, data: DataFrame) -> pd.DataFrame:
        """Clean data in Spark, then convert to Pandas."""
        logger.info("Cleaning data (removing null Road_EN values)")
        cleaned_spark = data.filter(col("Road_EN").isNotNull())

        logger.info("Converting to Pandas DataFrame")
        df_pandas = cleaned_spark.toPandas()
        record_count = len(df_pandas)
        logger.info(f"Converted to Pandas. Records: {record_count:,}")

        # Preprocess: extract hour from period_from
        logger.info("Preprocessing: extracting hour from period_from")
        df_pandas['hour'] = pd.to_datetime(df_pandas['period_from']).dt.hour

        return df_pandas

    def execute_analysis(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Execute Pandas analysis for morning and evening rush hours."""
        results = {}

        # Morning rush hour: 6:00 - 10:00
        logger.info("Analyzing morning rush hour (6:00-10:00)")
        morning_results = self._analyze_rush_hour(data, 6, 10)
        results['morning'] = morning_results

        logger.info("\n--- PANDAS: TOP 10 BUSIEST ROADS (Morning 6:00-10:00) ---")
        logger.info(morning_results['data'].to_string(index=False))

        # Evening rush hour: 17:00 - 21:00
        logger.info("Analyzing evening rush hour (17:00-21:00)")
        evening_results = self._analyze_rush_hour(data, 17, 21)
        results['evening'] = evening_results

        logger.info("\n--- PANDAS: TOP 10 BUSIEST ROADS (Evening 17:00-21:00) ---")
        logger.info(evening_results['data'].to_string(index=False))

        return results

    def _analyze_rush_hour(self, df: pd.DataFrame, start_hour: int,
                          end_hour: int) -> Dict[str, Any]:
        """
        Find top 10 busiest roads during specified hours using Pandas.

        Args:
            df: Input Pandas DataFrame with traffic data
            start_hour: Start hour of the period (0-23)
            end_hour: End hour of the period (0-23)

        Returns:
            Dictionary containing analysis results
        """
        # Filter by hour
        filtered_df = df[(df['hour'] >= start_hour) & (df['hour'] < end_hour)]

        # Group by road and aggregate
        results_df = filtered_df.groupby(['Road_EN', 'District']).agg({
            'volume': 'sum',
            'occupancy': 'sum'
        }).reset_index()

        results_df.columns = ['Road_EN', 'District', 'total_volume', 'total_occupancy']

        # Sort and get top 10
        results_df = results_df.sort_values('total_volume', ascending=False).head(10)

        return {
            'data': results_df,
            'count': len(results_df),
            'period': f"{start_hour}:00-{end_hour}:00"
        }


class SparkCongestionTest(BaseAnalysisTest):
    """
    PySpark implementation for analyzing traffic congestion patterns.

    This is a placeholder for a different type of analysis. You can implement
    your own congestion analysis logic here.
    """

    def initialize(self) -> None:
        """Initialize - PySpark doesn't need additional initialization."""
        if self.spark is None:
            raise ValueError("SparkSession is required for PySpark tests")
        logger.info("PySpark Congestion test initialized")

    def read_data(self) -> DataFrame:
        """Read data from Parquet files using Spark."""
        logger.info(f"Reading data from {self.data_paths}")
        df = self.spark.read.parquet(self.data_paths)
        return df

    def clean_data(self, data: DataFrame) -> DataFrame:
        """Clean data by removing null values."""
        logger.info("Cleaning data: removing null values")
        cleaned_df = data.filter(
            col("Road_EN").isNotNull() &
            col("occupancy").isNotNull() &
            col("volume").isNotNull()
        )

        cleaned_df.persist(StorageLevel.MEMORY_AND_DISK)
        record_count = cleaned_df.count()
        logger.info(f"Data cleaned and cached. Records: {record_count:,}")

        return cleaned_df

    def execute_analysis(self, data: DataFrame) -> Dict[str, Any]:
        """
        Execute congestion analysis.

        Example: Find roads with high occupancy rates (congestion).
        You can customize this to implement your specific congestion analysis.
        """
        logger.info("Analyzing congestion patterns")

        # Example: Find roads with average occupancy > 50%
        congestion_df = data.groupBy("Road_EN", "District") \
            .agg(
                spark_sum("volume").alias("total_volume"),
                (spark_sum("occupancy") / spark_sum("volume") * 100).alias("avg_occupancy_pct")
            ) \
            .filter(col("avg_occupancy_pct") > 50) \
            .orderBy(desc("avg_occupancy_pct")) \
            .limit(20)

        count = congestion_df.count()

        logger.info(f"\n--- ROADS WITH HIGH CONGESTION (>50% occupancy) ---")
        congestion_df.show(truncate=False)

        return {
            'congested_roads': congestion_df,
            'count': count
        }

    def cleanup(self) -> None:
        """Unpersist cached data."""
        if self.cleaned_data is not None:
            self.cleaned_data.unpersist()
            logger.info("Unpersisted cached data")


class HiveCongestionTest(BaseAnalysisTest):
    """
    Hive implementation for analyzing traffic congestion patterns.

    This is a placeholder for Hive-based congestion analysis.
    """

    def __init__(self, *args, sql_dir: str = "benchmark/queries", **kwargs):
        super().__init__(*args, **kwargs)
        self.sql_dir = sql_dir

    def initialize(self) -> None:
        """Initialize - Hive doesn't need additional initialization."""
        if self.spark is None:
            raise ValueError("SparkSession with Hive support is required")
        logger.info("Hive Congestion test initialized")

    def read_data(self) -> DataFrame:
        """Read data from Parquet files."""
        logger.info(f"Reading data from {self.data_paths}")
        df = self.spark.read.parquet(self.data_paths)
        return df

    def clean_data(self, data: DataFrame) -> DataFrame:
        """Clean data and create temporary Hive view."""
        logger.info("Cleaning data and creating temporary Hive view")
        cleaned_df = data.filter(
            col("Road_EN").isNotNull() &
            col("occupancy").isNotNull() &
            col("volume").isNotNull()
        )
        cleaned_df.createOrReplaceTempView("traffic_data")
        logger.info("Created temporary Hive view: traffic_data")
        return cleaned_df

    def execute_analysis(self, data: DataFrame) -> Dict[str, Any]:
        """
        Execute Hive congestion analysis.

        You can create a congestion_analysis.hql file with your custom query.
        """
        logger.info("\n--- HIVE: CONGESTION ANALYSIS ---")

        # Example inline query (you can load from file instead)
        congestion_query = """
        SELECT
            Road_EN,
            District,
            SUM(volume) as total_volume,
            (SUM(occupancy) / SUM(volume) * 100) as avg_occupancy_pct
        FROM traffic_data
        GROUP BY Road_EN, District
        HAVING avg_occupancy_pct > 50
        ORDER BY avg_occupancy_pct DESC
        LIMIT 20
        """

        results_df = self.spark.sql(congestion_query)
        count = results_df.count()
        results_df.show(truncate=False)

        return {
            'congested_roads': results_df,
            'count': count,
            'query': congestion_query
        }


class PandasCongestionTest(BaseAnalysisTest):
    """
    Pandas implementation for analyzing traffic congestion patterns.

    This is a placeholder for Pandas-based congestion analysis.
    """

    def initialize(self) -> None:
        """Initialize - Pandas doesn't need special initialization."""
        if self.spark is None:
            raise ValueError("SparkSession is required to read data initially")
        logger.info("Pandas Congestion test initialized")

    def read_data(self) -> pd.DataFrame:
        """Read data using Spark then convert to Pandas."""
        logger.info(f"Reading data from {self.data_paths} via Spark")
        df_spark = self.spark.read.parquet(self.data_paths)
        return df_spark

    def clean_data(self, data: DataFrame) -> pd.DataFrame:
        """Clean data in Spark, then convert to Pandas."""
        logger.info("Cleaning data")
        cleaned_spark = data.filter(
            col("Road_EN").isNotNull() &
            col("occupancy").isNotNull() &
            col("volume").isNotNull()
        )

        logger.info("Converting to Pandas DataFrame")
        df_pandas = cleaned_spark.toPandas()
        logger.info(f"Converted to Pandas. Records: {len(df_pandas):,}")

        return df_pandas

    def execute_analysis(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Execute Pandas congestion analysis."""
        logger.info("Analyzing congestion patterns")

        # Group by road and calculate average occupancy
        results_df = data.groupby(['Road_EN', 'District']).agg({
            'volume': 'sum',
            'occupancy': 'sum'
        }).reset_index()

        results_df['avg_occupancy_pct'] = (results_df['occupancy'] / results_df['volume'] * 100)
        results_df = results_df[results_df['avg_occupancy_pct'] > 50]
        results_df = results_df.sort_values('avg_occupancy_pct', ascending=False).head(20)

        logger.info("\n--- PANDAS: ROADS WITH HIGH CONGESTION (>50% occupancy) ---")
        logger.info(results_df.to_string(index=False))

        return {
            'congested_roads': results_df,
            'count': len(results_df)
        }
