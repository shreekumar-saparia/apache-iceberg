"""
This file demonstrate data ingestion into the iceberg table
"""
table_ddl = """
        create table local.default.nyc_yellowtaxi_tripdata 
          (vendorid  				bigint, 
          tpep_pickup_datetime  		timestamp, 
          tpep_dropoff_datetime  		timestamp, 
          passenger_count  			double, 
          trip_distance  			double, 
          ratecodeid  				double, 
          store_and_fwd_flag  			string, 
          pulocationid  			bigint, 
          dolocationid  			bigint, 
          payment_type  			bigint, 
          fare_amount  				double, 
          extra  				double, 
          mta_tax  				double, 
          tip_amount  				double, 
          tolls_amount  			double, 
          improvement_surcharge  		double, 
          total_amount  			double, 
          congestion_surcharge  		double, 
          airport_fee  				double
		  ) 
        USING iceberg 
        PARTITIONED BY (months(tpep_pickup_datetime))
    """

spark = get_spark_session()
spark.sql(table_ddl)