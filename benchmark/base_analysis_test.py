from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from typing import Dict, List, Optional, Any
import time
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
import uuid

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
        self.uuid = str(uuid.uuid1())
        self.hdfs_path = "hdfs:///benchmark_results/timing"

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
            'details': details,
            'uuid': self.uuid
        }
        self.timing_results.append(result)
        logger.info(f"[{self.name}] {phase}: {duration:.3f}s | Records: {records:,} | {details}")



    def get_total_record(self, df: Any) -> int:
        """
        Get the total number of records from either a Spark or Pandas DataFrame.

        Returns:
            The number of records as an int.
        """
        if isinstance(df, DataFrame):  # Spark DataFrame
            return df.count()
        elif hasattr(df, 'shape'):  # Check for Pandas DataFrame
            return df.shape[0]
        
        logger.warning(f"Unsupported data type for get_total_record: {type(df)}. Returning 0.")
        return 0
    
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
        # Timing results are now saved in the finally block of run() after total execution time is recorded
        pass


    def save_timing_results_to_csv_hdfs(self, hdfs_path: str) -> str:
        """
        Save timing results to HDFS as a CSV file, always appending to the destination.
        Each save operation will create a new single part-file in the target directory.

        Args:
            hdfs_path: HDFS directory path where to save the results (e.g., "hdfs:///benchmark_results/timing_csv")

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

            # Save to HDFS as parquet, always appending. coalesce(1) ensures a single file per run.
            logger.info(f"Appending {len(self.timing_results)} timing records to HDFS parquet directory: {hdfs_path}")
            df.write.mode("append").parquet(hdfs_path)

            logger.info(f"Successfully appended timing results to {hdfs_path}")
            return hdfs_path

        except Exception as e:
            logger.error(f"Failed to save timing results to HDFS parquet: {e}", exc_info=True)
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
            if self.spark is not None:
                logger.info("Using provided SparkSession")
            else:
                self.initialize()
            init_time = time.time() - init_start
            self.record_timing("Initialize", init_time, 0, "test environment setup")

            # Phase 2: Read data
            read_start = time.time()
            try:
                self.data = self.read_data()
                read_time = time.time() - read_start
                # Phase 3.5: Get total records after cleaning
                self.total_records = self.get_total_record(self.data)
                self.record_timing("Read Data", read_time, self.total_records, f"from {self.data_path}")
            except Exception as read_error:
                read_time = time.time() - read_start
                self.record_timing("Read Data", read_time, self.total_records, f"FAILED: {str(read_error)}")
                raise

            # Phase 3: Clean data
            clean_start = time.time()
            try:
                self.cleaned_data = self.clean_data(self.data)
                clean_time = time.time() - clean_start
                self.record_timing("Clean Data", clean_time, self.total_records, "data cleaning")
                
            except Exception as clean_error:
                clean_time = time.time() - clean_start
                self.record_timing("Clean Data", clean_time, self.total_records, f"FAILED: {str(clean_error)}")
                raise
            

            # Phase 4: Execute analysis
            analysis_start = time.time()
            try:
                self.analysis_results = self.execute_analysis(self.cleaned_data)
                analysis_time = time.time() - analysis_start
                self.record_timing("Execute Analysis", analysis_time, self.total_records, "main analysis")
            except Exception as analysis_error:
                analysis_time = time.time() - analysis_start
                self.record_timing("Execute Analysis", analysis_time, self.total_records, f"FAILED: {str(analysis_error)}")
                raise  # Re-raise the exception to be caught by outer except block


            # Phase 5: Cleanup
            cleanup_start = time.time()
            try:
                self.cleanup()
                cleanup_time = time.time() - cleanup_start
                self.record_timing("Cleanup", cleanup_time, self.total_records, "resource cleanup")
            except Exception as cleanup_error:
                cleanup_time = time.time() - cleanup_start
                self.record_timing("Cleanup", cleanup_time, self.total_records, f"FAILED: {str(cleanup_error)}")
                raise

            

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
            self.record_timing("Total Execution", total_time, self.total_records,
                             "SUCCESS" if success else f"FAILED: {error_msg}")
            logger.info(f"\n{self.name} total execution time: {total_time:.2f} seconds")

            # Phase 6: Save results (after total execution time is recorded)
            try:
                self.save_timing_results_to_csv_hdfs(hdfs_path=self.hdfs_path)
            except Exception as save_error:
                logger.error(f"Failed to save timing results in finally block: {save_error}", exc_info=True)

        return {
            'total_time': total_time,
            'timing_results': self.timing_results,
            'analysis_results': self.analysis_results if success else None,
            'success': success,
            'error': error_msg
        }
