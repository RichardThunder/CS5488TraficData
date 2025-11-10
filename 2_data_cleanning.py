import logging
from pyspark.sql import SparkSession, DataFrame, functions as F
from typing import Optional
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

##### 1. handle missing value
##### 2. handle outliers value
##### 3. data consistency & formatting check
##### 4.

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def initialize_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("DataValidation").master("local[*]").getOrCreate()
    )


def load_data(spark: SparkSession, file_path: str) -> Optional[DataFrame]:
    # Manually custom schema for performance
    traffic_data_schema = StructType(
        [
            StructField("detector_id", StringType(), True),  # Traffic detector ID
            StructField("direction", StringType(), True),  # Traffic direction
            StructField("lane_id", StringType(), True),  # Lane identifier
            StructField("speed", IntegerType(), True),  # Average speed (km/h)
            StructField("occupancy", IntegerType(), True),  # Lane occupancy (%)
            StructField("volume", IntegerType(), True),  # Vehicle count
            StructField("standard_deviation", DoubleType(), True),  # Speed std dev
            StructField("period_from", TimestampType(), True),  # Start time
            StructField("period_to", TimestampType(), True),  # End time
            StructField("date", StringType(), True),  # Measurement date
            StructField("District", StringType(), True),  # District name
            StructField("Road_EN", StringType(), True),  # Road name (English)
            StructField(
                "Road_TC", StringType(), True
            ),  # Road name (Traditional Chinese)
            StructField(
                "Road_SC", StringType(), True
            ),  # Road name (Simplified Chinese)
            StructField("Rotation", IntegerType(), True),  # Detector rotation angle
            StructField(
                "GeometryEasting", DoubleType(), True
            ),  # GPS coordinate (Easting)
            StructField(
                "GeometryNorthing", DoubleType(), True
            ),  # GPS coordinate (Northing)
        ]
    )
    try:
        df = spark.read.schema(traffic_data_schema).csv(file_path, header=True)
        logger.info("#" * 40, "Print Data Frame schema")
        logger.info("#" * 40, df.printSchema())
        return df
    except Exception as e:
        logger.error("Error read csv file %s", str(e))


def miss_value_check(df: DataFrame) -> Optional[DataFrame]:
    try:
        missing_count = [
            F.count(F.when(F.col(c).isNull(), c).alias(c)) for c in df.columns
        ]
        df_missing_summary = df.select(*missing_count)
        df_missing_summary.show()

        df_cleaned = df.na.drop()
        logger.info("#" * 40, "Data cleaning completed, missing vlaues were dropped.")
        return df_cleaned
    except Exception as e:
        logger.error("Data cleaning error %s", e)
        return None


def main():
    logger.info("#" * 40, "Data validation process started.")
    DATA_FILE_PATH = "traffic_data_partitioned/date=2025-08-31"

    try:
        sparkSession = initialize_spark()
        logger.info("#" * 40, "SparkSession created successfully.")
    except Exception as e:
        logger.error("Error initializing SparkSession: %s", str(e))
        return
    df = load_data(sparkSession, DATA_FILE_PATH)
    if df is None:
        logger.error(
            "Data loading failed (returned None). Aborting validation process."
        )
        sparkSession.stop()
        return

    df_clean = miss_value_check(df)
    if df_clean is None:
        logger.error("Data cleaning error.")
        return

    df_clean.show(20)


if __name__ == "__main__":
    main()


"""
方案 A: 使用 inferSchema=True (方便，但效率低)
Python

df = spark.read.csv("file.csv", header=True, inferSchema=True)
工作原理: Spark 会对文件进行两次扫描：

第一次扫描: 确定列名（如果 header=True）和推断数据类型。

第二次扫描: 读取数据并根据推断出的类型（例如 Integer 或 Double）进行解析和转换。

识别 NULL: 在推断类型时，Spark 也会自动判断哪些是空值表示，并尝试将其转换为该数据类型下的 NULL。例如，如果一列被推断为 IntegerType，而某个单元格为空，Spark 就会将其视为 NULL。

优点: 简单，无需手动编写 Schema。

缺点: 需要两次 I/O 操作，对于大型数据集来说效率较低。

方案 B: 手动指定 Schema (推荐，效率高)
Python

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

custom_schema = StructType([
    StructField("col1", IntegerType(), True),  # True 表示允许 NULL
    StructField("col2", StringType(), True),
    # ...
])

df = spark.read.schema(custom_schema).csv("file.csv", header=True)
工作原理: Spark 只扫描文件一次。它直接使用你提供的 Schema 来读取和解析数据。

识别 NULL: 通过指定具体的类型，Spark 知道如何将输入文件中的空/缺失标记（例如 CSV 中的 , ,）解析成该类型对应的 NULL 值。如果你将 nullable 设置为 True，Spark 知道该列可以容纳 NULL。

优点: 效率最高，尤其适用于生产环境中的大数据集。可以精确控制每列的类型和 NULL 属性。

"""
