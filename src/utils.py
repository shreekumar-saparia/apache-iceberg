from pyspark.sql import SparkSession
import time
from constants import *

def get_spark_session():
    spark = SparkSession.builder \
        .appName("IcebergLocalDevelopment") \
        .config('spark.jars.packages', ICEBERG_JAR) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", ICEBERG_WAREHOUSE) \
        .getOrCreate()

    return spark

def enable_sleep(seconds):
    time.sleep(seconds)

