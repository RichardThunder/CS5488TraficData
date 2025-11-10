from datetime import datetime
import logging
from pyspark.sql import SparkSession


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def initialize_spark(): -> SparkSession
    return  SparkSession.builder \
        .appName("DataValidation") \
        .master("local[*]") \
        .getOrCreate()


def load_data(spark: SparkSession, file_path: str) -> DataFrame:
    try:
        df = spark.read \
            .csv(file_path)
        return df
    except Exception as e:
        logger.error("Error read csv file %s", str(e))
    
def 
def main():
    logger.info("Data validation process started.")
    try:
        sparkSession = initialize_spark()
        logger.info("SparkSession created successfully.")
    except Exception as e:
        logger.error("Error initializing SparkSession: %s", str(e))
    
    
    

if __name__ == "__main__":
   
    main()
