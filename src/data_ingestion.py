"""
This file demonstrate data ingestion into the iceberg table
"""

from src.utils import get_spark_session

spark = get_spark_session()

table_ddl = """
        create table local.default.nyc_yellowtaxi_tripdata
          (
              VendorID bigint,
              tpep_pickup_datetime timestamp,
              tpep_dropoff_datetime timestamp,
              passenger_count double,
              trip_distance double,
              RatecodeID double,
              store_and_fwd_flag string,
              PULocationID bigint,
              DOLocationID bigint,
              payment_type bigint,
              fare_amount double,
              extra double,
              mta_tax double,
              tip_amount double,
              tolls_amount double,
              improvement_surcharge double,
              total_amount double,
              congestion_surcharge double,
              airport_fee double,
              cbd_congestion_fee double
		  )
        USING iceberg
        PARTITIONED BY (months(tpep_pickup_datetime))
    """

spark.sql(table_ddl)

df = spark.read.parquet("<path-to>/yellow_tripdata_2025-01.parquet")
#df.show()

df.write.format("iceberg").mode("overwrite").save("local.default.nyc_yellowtaxi_tripdata")

spark.sql(
    """
        select * from local.default.nyc_yellowtaxi_tripdata
    """
).show()