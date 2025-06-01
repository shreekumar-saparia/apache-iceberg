"""
This file demonstrate how we can create an iceberg table
"""
from src.utils import get_spark_session

spark = get_spark_session()

table_ddl = """
CREATE TABLE IF NOT EXISTS local.default.my_table (id int, name string) USING ICEBERG
"""

# create table with name my_table in default database and in local catalog
spark.sql(table_ddl)

insert_stmt = """
INSERT INTO local.default.my_table VALUES (1, 'Alice'), (2, 'Bob')
"""

spark.sql(insert_stmt)

df = spark.sql("SELECT * FROM local.default.my_table")
df.show()

update_stmt = """
UPDATE local.default.my_table SET name = 'David' WHERE id = 1
"""

spark.sql(update_stmt)
df = spark.sql("SELECT * FROM local.default.my_table")
df.show()



