"""
This file demonstrate iceberg table time travel feature
"""

from src.utils import get_spark_session

spark = get_spark_session()

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.default.time_travel_table (
        id BIGINT,
        name STRING
    ) USING iceberg
""")

spark.sql(
    """
        INSERT INTO local.default.time_travel_table VALUES (1, 'Alice'), (2, 'Bob')
    """
)
spark.sql(
    """
        SELECT * FROM local.default.time_travel_table
    """
).show()


spark.sql(
    """
        SELECT * FROM local.default.time_travel_table.snapshots
    """
).show()

spark.sql(
    """
        UPDATE local.default.time_travel_table VALUES SET name = 'David' WHERE id = 1
    """
)
spark.sql(
    """
        SELECT * FROM local.default.time_travel_table
    """
).show()

spark.sql(
    """
        SELECT * FROM local.default.time_travel_table.snapshots
    """
).show()

"""
+--------------------+-------------------+------------------+---------+--------------------+--------------------+
|        committed_at|        snapshot_id|         parent_id|operation|       manifest_list|             summary|
+--------------------+-------------------+------------------+---------+--------------------+--------------------+
|2025-06-02 19:56:...| 626083855622811499|              NULL|   append|/Users/.../...|{spark.app.id -> ...|
|2025-06-02 19:56:...|7490211042054103307|626083855622811499|overwrite|/Users/.../...|{spark.app.id -> ...|
+--------------------+-------------------+------------------+---------+--------------------+--------------------+
"""

spark.sql(
    """
        CALL local.system.rollback_to_snapshot('default.time_travel_table', 626083855622811499)
    """
).show()

spark.sql(
    """
        SELECT * FROM local.default.time_travel_table
    """
).show()


spark.sql(
    """
        SELECT * FROM local.default.time_travel_table.history
    """
).show(truncate=False)

"""
+-----------------------+-------------------+------------------+-------------------+
|made_current_at        |snapshot_id        |parent_id         |is_current_ancestor|
+-----------------------+-------------------+------------------+-------------------+
|2025-06-02 19:56:47.417|626083855622811499 |NULL              |true               |
|2025-06-02 19:56:49.166|7490211042054103307|626083855622811499|false              |
|2025-06-02 19:59:24.61 |626083855622811499 |NULL              |true               |
+-----------------------+-------------------+------------------+-------------------+
"""