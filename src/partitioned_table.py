"""
This file demonstrate partition tables in iceberg
"""
from src.utils import get_spark_session

spark = get_spark_session()

partition_table_ddl = """
    CREATE TABLE IF NOT EXISTS local.default.sales (
        id BIGINT,
        amount DOUBLE,
        region STRING,
        posted_date DATE
    )
    USING iceberg
    PARTITIONED BY (posted_date);
"""

spark.sql(partition_table_ddl)

insert_stmt = """
    INSERT INTO local.default.sales VALUES
    (1, 100.5, 'North', cast('2024-01-01' as date)),
    (2, 200.0, 'South', cast('2024-01-01' as date)),
    (3, 150.0, 'North', cast('2024-01-02' as date)),
    (4, 250.0, 'West', cast('2024-01-02' as date)),
    (5, 300.0, 'East', cast('2024-01-03' as date))
"""

spark.sql(insert_stmt)

df = spark.sql("SELECT * FROM local.default.sales")
df.show()

spark.sql("""
    SELECT file_path,partition
    FROM local.default.sales.files
    ORDER BY partition
""").show(truncate=False)

spark.sql("""
    SELECT file_path, partition
    FROM local.default.sales.files
    WHERE partition.posted_date = '2024-01-02'
""").show(truncate=False)

spark.sql("""
    SELECT *
    FROM local.default.sales
    WHERE posted_date = '2024-01-02'
""").explain(True)


# enable log level to DEBUG
spark.sparkContext.setLogLevel("DEBUG")

# observe in logs that filters is pushed to avoid full table scan
df = spark.sql("""SELECT *
                FROM local.default.sales
                WHERE posted_date = '2024-01-02'""")
df.show()

spark.sql("""
    INSERT INTO local.default.sales VALUES
    (6, 250.0, 'South', cast('2024-01-01' as date)),
    (7, 350.0, 'East', cast('2024-01-01' as date))
""")
df = spark.sql("SELECT * FROM local.default.sales ORDER BY id")
df.show()

spark.sql("""
    SELECT *
    FROM local.default.sales
    WHERE posted_date = '2024-01-01' AND region = 'North'
""").show(truncate=False)


