from pyspark.sql import SparkSession
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def create_data_subset(spark: SparkSession, input_path: str, output_path: str, sample_fraction: float):
    """
    Reads a Parquet dataset, takes a random sample, and writes it back to HDFS.

    Args:
        spark: The SparkSession object.
        input_path: The HDFS path to the source dataset.
        output_path: The HDFS path to write the sampled dataset to.
        sample_fraction: The fraction of data to sample (e.g., 0.01 for 1%).
    """
    logger.info(f"Reading data from: {input_path}")
    try:
        df = spark.read.parquet(input_path)
        
        original_count = df.count()
        logger.info(f"Original dataset has {original_count:,} records.")

        logger.info(f"Creating a sample with fraction: {sample_fraction}")
        # Using withReplacement=False to ensure we get at most the fraction specified
        subset_df = df.sample(withReplacement=False, fraction=sample_fraction)
        
        subset_count = subset_df.count()
        logger.info(f"Sampled subset has {subset_count:,} records.")

        logger.info(f"Writing subset to: {output_path}")
        # Writing in overwrite mode to allow for repeated runs
        subset_df.write.mode("overwrite").parquet(output_path)
        
        logger.info("Successfully created data subset.")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)

def main():
    """Main function to run the data subset creation process."""
    
    # --- Configuration ---
    INPUT_PATH = "hdfs:///traffic_data_partitioned/202508"
    # ---------------------

    spark = None
    try:
        spark = SparkSession.builder \
            .appName("Create Traffic Data Subset") \
            .getOrCreate()
        
        for i in range(1, 10):
            fraction = i / 10.0
            logger.info(f"Starting subset creation with sample fraction: {fraction}")
            output_path = f"hdfs:///202508_subset_{int(fraction * 100)}pct"
            create_data_subset(spark, INPUT_PATH, output_path, fraction)
    except Exception as e:
        logger.error(f"An error occurred in main: {e}", exc_info=True)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped.")

if __name__ == "__main__":
    main()
