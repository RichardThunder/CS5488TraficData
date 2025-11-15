from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from typing import Dict, List, Optional, Any
import time
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


class BaseAnalysisTest(ABC):
    """
    Abstract base class for analysis benchmarks using the Template Method pattern.

    This class defines the blueprint for all benchmark tests. Each concrete test
    must implement the four core methods: initialize(), read_data(), clean_data(),
    and execute_analysis().

    The run() method orchestrates the execution of these steps in order and
    collects timing information for each phase.
    """

    def __init__(self, name: str, data_path: str,
                 spark: Optional[SparkSession] = None):
        """
        Initialize the test.

        Args:
            name: Name of the test (e.g., "PySpark", "Hive", "Pandas")
            data_path: path to the data files
            spark: SparkSession instance (optional, some tests may create their own)
            data_size_bytes: Size of the dataset in bytes
            total_records: Total number of records in the dataset
        """
        self.name = name
        self.data_path = data_path
        self.spark = spark
        self.total_records = 0

        # Storage for timing results
        self.timing_results: List[Dict[str, Any]] = []

        # Storage for intermediate data
        self.data = None
        self.cleaned_data = None
        self.analysis_results = None

    def record_timing(self, phase: str, duration: float, records: int = 0, details: str = ""):
        """
        Record timing information for a specific phase.

        Args:
            phase: Name of the phase (e.g., "Data Read", "Execute Analysis")
            duration: Duration in seconds
            records: Number of records processed
            details: Additional details about the operation
        """
        result = {
            'timestamp': datetime.now().isoformat(),
            'total_records': self.total_records,
            'tool': self.name,
            'phase': phase,
            'duration_seconds': round(duration, 3),
            'records_processed': records,
            'details': details
        }
        self.timing_results.append(result)
        logger.info(f"[{self.name}] {phase}: {duration:.3f}s | Records: {records:,} | {details}")



    def get_total_record(self, df:DataFrame) -> int:
        """
        Get the dataset frame.

        Returns:
            The records number as int.
        """
        
        self.total_records = df.count()
        return self.total_records
    
    @abstractmethod
    def initialize(self) -> None:
        """
        Initialize the test environment.

        This might include:
        - Setting up connections
        - Configuring settings
        - Allocating resources

        Must be implemented by concrete test classes.
        """
        pass

    @abstractmethod
    def read_data(self) -> DataFrame:
        """
        Read the data from the specified paths.

        Returns:
            The data in the format appropriate for this test
            (e.g., Spark DataFrame, Pandas DataFrame, etc.)

        Must be implemented by concrete test classes.
        """
        pass

    @abstractmethod
    def clean_data(self, data: Any) -> Any:
        """
        Clean the data (e.g., remove nulls, filter invalid records).

        Args:
            data: The raw data to clean

        Returns:
            The cleaned data

        Must be implemented by concrete test classes.
        """
        pass

    @abstractmethod
    def execute_analysis(self, data: Any) -> Dict[str, Any]:
        """
        Execute the analysis on the cleaned data.

        Args:
            data: The cleaned data to analyze

        Returns:
            Dictionary containing analysis results

        Must be implemented by concrete test classes.
        """
        pass

    def cleanup(self) -> None:
        """
        Clean up resources after the test completes.

        This is optional and can be overridden by concrete classes if needed.
        Default implementation does nothing.
        """
        pass

    def save_timing_results_to_hdfs(self, hdfs_path: str, mode: str = "append"):
        """
        Save timing results to HDFS as a Parquet file.

        Args:
            hdfs_path: HDFS path where to save the results (e.g., "hdfs:///benchmark_results/timing")
            mode: Save mode - "append" (default), "overwrite", or "error"

        Returns:
            The HDFS path where results were saved

        Raises:
            ValueError: If SparkSession is not available
            Exception: If saving to HDFS fails
        """
        if self.spark is None:
            raise ValueError("SparkSession is required to save results to HDFS")

        if not self.timing_results:
            logger.warning("No timing results to save")
            

        try:
            # Convert timing results to Spark DataFrame
            df = self.spark.createDataFrame(self.timing_results)

            # Save to HDFS as Parquet
            logger.info(f"Saving {len(self.timing_results)} timing records to HDFS: {hdfs_path}")
            df.write.mode(mode).parquet(hdfs_path)

            logger.info(f"Successfully saved timing results to {hdfs_path}")
            

        except Exception as e:
            logger.error(f"Failed to save timing results to HDFS: {e}", exc_info=True)
            raise

    def save_timing_results_to_csv_hdfs(self, hdfs_path: str, mode: str = "append") -> str:
        """
        Save timing results to HDFS as a CSV file.

        Args:
            hdfs_path: HDFS path where to save the results (e.g., "hdfs:///benchmark_results/timing.csv")
            mode: Save mode - "append" (default), "overwrite", or "error"

        Returns:
            The HDFS path where results were saved

        Raises:
            ValueError: If SparkSession is not available
            Exception: If saving to HDFS fails
        """
        if self.spark is None:
            raise ValueError("SparkSession is required to save results to HDFS")

        if not self.timing_results:
            logger.warning("No timing results to save")
            return hdfs_path

        try:
            # Convert timing results to Spark DataFrame
            df = self.spark.createDataFrame(self.timing_results)

            # Save to HDFS as CSV
            logger.info(f"Saving {len(self.timing_results)} timing records to HDFS CSV: {hdfs_path}")
            df.coalesce(1).write.mode(mode).option("header", "true").csv(hdfs_path)

            logger.info(f"Successfully saved timing results to {hdfs_path}")
            return hdfs_path

        except Exception as e:
            logger.error(f"Failed to save timing results to HDFS CSV: {e}", exc_info=True)
            raise

    def run(self) -> Dict[str, Any]:
        """
        Template method that runs the entire benchmark test.

        This method orchestrates the execution of all test phases in order:
        1. Initialize
        2. Read data
        3. Clean data
        4. Execute analysis
        5. Cleanup

        Returns:
            Dictionary containing:
                - total_time: Total execution time in seconds
                - timing_results: List of timing records for each phase
                - analysis_results: Results from the analysis
                - success: Boolean indicating if test completed successfully
                - error: Error message if test failed (None otherwise)
        """
        logger.info("=" * 80)
        logger.info(f"STARTING {self.name.upper()} TEST")
        logger.info("=" * 80)

        overall_start = time.time()
        success = True
        error_msg = None

        try:
            # Phase 1: Initialize
            init_start = time.time()
            self.initialize()
            init_time = time.time() - init_start
            self.record_timing("Initialize", init_time, 0, "test environment setup")

            # Phase 2: Read data
            read_start = time.time()
            self.data = self.read_data()
            read_time = time.time() - read_start
            self.record_timing("Read Data", read_time, 0, f"from {self.data_path}")


            # Phase 3: Clean data
            clean_start = time.time()
            self.cleaned_data = self.clean_data(self.data)
            clean_time = time.time() - clean_start
            self.record_timing("Clean Data", clean_time, 0, "data cleaning")
            # Phase 3.5: Get total records after cleaning
            self.get_total_record(self.cleaned_data)

            # Phase 4: Execute analysis
            analysis_start = time.time()
            self.analysis_results = self.execute_analysis(self.cleaned_data)
            analysis_time = time.time() - analysis_start
            self.record_timing("Execute Analysis", analysis_time, 0, "main analysis")

            # Phase 5: Cleanup
            cleanup_start = time.time()
            self.cleanup()
            cleanup_time = time.time() - cleanup_start
            if cleanup_time > 0.001:  # Only record if cleanup took measurable time
                self.record_timing("Cleanup", cleanup_time, 0, "resource cleanup")
            #phase 6: save results

            self.save_timing_results_to_hdfs(hdfs_path="hdfs:///benchmark_results/timing")
            self.save_timing_results_to_csv_hdfs(hdfs_path="hdfs:///benchmark_results/timing_csv")

        except Exception as e:
            success = False
            error_msg = str(e)
            logger.error(f"{self.name} test failed: {error_msg}", exc_info=True)

            # Try to cleanup even if test failed
            try:
                self.cleanup()
            except Exception as cleanup_error:
                logger.error(f"Cleanup also failed: {cleanup_error}", exc_info=True)

        finally:
            total_time = time.time() - overall_start
            self.record_timing("Total Execution", total_time, 0,
                             "SUCCESS" if success else f"FAILED: {error_msg}")
            logger.info(f"\n{self.name} total execution time: {total_time:.2f} seconds")

        return {
            'total_time': total_time,
            'timing_results': self.timing_results,
            'analysis_results': self.analysis_results if success else None,
            'success': success,
            'error': error_msg
        }
