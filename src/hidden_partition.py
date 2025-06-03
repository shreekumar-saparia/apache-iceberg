"""
This file will demonstrate hidden partition feature for apache iceberg
and how we can leverage partition transformation
https://iceberg.apache.org/spec/#partition-transforms
"""

from src.utils import get_spark_session

spark = get_spark_session()

partition_trans_table_ddl = """
    CREATE TABLE IF NOT EXISTS local.default.partition_transformation (
        id BIGINT,
        amount DOUBLE,
        region STRING,
        posted_date DATE
    )
    USING iceberg
    PARTITIONED BY (month(posted_date))
"""

spark.sql(partition_trans_table_ddl)

par_trans_insert_stmt = """
    INSERT INTO local.default.partition_transformation
    VALUES
    (1, 100.5, 'North', cast('2024-01-01' as date)),
    (2, 200.0, 'South', cast('2024-02-01' as date)),
    (3, 150.0, 'North', cast('2024-03-02' as date)),
    (4, 250.0, 'West', cast('2024-04-02' as date)),
    (5, 300.0, 'East', cast('2024-03-03' as date))
"""

spark.sql(par_trans_insert_stmt)

spark.sql("describe extended local.default.partition_transformation").show(truncate=False)

# This will skip all files and read only parquet files with month 2024-03
par_trans = """
    select * from local.default.partition_transformation
    where posted_date>='2024-03-01' and posted_date<='2024-03-30'
"""

spark.sql(par_trans).show()


# Adding partition column to the iceberg table

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS local.default.sales_data (
          id BIGINT,
          item STRING,
          category STRING,
          sale_date DATE
        )
        USING ICEBERG
        PARTITIONED BY (months(sale_date))
    """
)

spark.sql(
    """
        INSERT INTO local.default.sales_data VALUES
        (1, 'Laptop', 'Electronics',DATE '2024-01-15'),
        (2, 'Phone', 'Electronics',DATE '2024-02-10'),
        (3, 'Shirt', 'Clothing',DATE '2024-01-20')
    """
)

spark.sql("describe extended local.default.sales_data").show(truncate=False)


spark.sql("SELECT * FROM local.default.sales_data WHERE sale_date = '2024-01-15'").show()

# Alter table and add partition column

spark.sql(
    """
    ALTER TABLE local.default.sales_data
    ADD PARTITION FIELD category
    """
)

spark.sql("describe extended local.default.sales_data").show(truncate=False)

# Observe warehouse dir for this table after new partition column addition and new data insertion
spark.sql(
    """
    INSERT INTO local.default.sales_data VALUES
    (4, 'Tablet', 'Electronics',DATE '2024-03-05'),
    (5, 'Jacket', 'Clothing',DATE '2024-03-15')
    """
)

spark.sql("SELECT * FROM local.default.sales_data WHERE category = 'Electronics' AND sale_date = '2024-03-05'").show()
