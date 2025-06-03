"""
This file demonstrate schema evolution in iceberg table
"""
from src.utils import get_spark_session

spark = get_spark_session()

spark.sql(
    """
        CREATE TABLE IF NOT EXISTS local.default.customer_orders
        (
            order_id BIGINT,
            customer_name STRING,
            order_date DATE
        )
        USING ICEBERG
    """
)

spark.sql(
    """
        INSERT INTO local.default.customer_orders
        VALUES
        (1, 'Alice', DATE '2024-01-01'),
        (2, 'Bob', DATE '2024-01-02')
    """
)

spark.sql(
    """
        SELECT * FROM local.default.customer_orders
    """
).show()


# # Add new column total_amount
# # NULL values will be added as default value
spark.sql(
    """
        ALTER TABLE local.default.customer_orders
        ADD COLUMN total_amount DOUBLE
    """
)

spark.sql(
    """
        SELECT * FROM local.default.customer_orders
    """
).show()

spark.sql(
    """
    INSERT INTO local.default.customer_orders
    VALUES
    (3, 'Charlie', DATE '2024-01-03', 150.75),
    (4, 'Diana', DATE '2024-01-04', 200.00)
    """
)

spark.sql(
    """
        SELECT * FROM local.default.customer_orders
    """
).show()


# RENAME existing column

spark.sql(
    """
    ALTER TABLE local.default.customer_orders
    RENAME COLUMN order_date TO purchase_date
    """
)

spark.sql(
    """
        SELECT * FROM local.default.customer_orders
    """
).show()


# DROP existing column

spark.sql(
    """
    ALTER TABLE local.default.customer_orders
    DROP COLUMN customer_name
    """
)

spark.sql(
    """
        SELECT * FROM local.default.customer_orders
    """
).show()