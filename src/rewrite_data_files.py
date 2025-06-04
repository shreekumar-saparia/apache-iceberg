"""
This file illustrates the rewrite iceberg data files feature
"""
from src.utils import get_spark_session

spark = get_spark_session()

# format version 2 is for copy-on-write or merge-on-read
spark.sql(
    """
        CREATE TABLE IF NOT EXISTS local.default.rewrite_table (
            id BIGINT,
            name STRING,
            age INT
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
    """
)

spark.sql(
    """
        INSERT INTO local.default.rewrite_table VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 22)
    """
)

spark.sql(
    """
        SELECT * FROM local.default.rewrite_table
    """
).show()

spark.sql(
    """
        SELECT file_path, file_size_in_bytes
        FROM local.default.rewrite_table.files
    """
).show(truncate=False)

"""
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
|file_path                                                                                                                                                         |file_size_in_bytes|
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
|.../iceberg-warehouse/default/rewrite_table/data/00000-0-6f710ac1-754e-45f9-bee3-ce3f64615aaf-0-00001.parquet|867               |
|.../iceberg-warehouse/default/rewrite_table/data/00001-1-6f710ac1-754e-45f9-bee3-ce3f64615aaf-0-00001.parquet|853               |
|.../iceberg-warehouse/default/rewrite_table/data/00002-2-6f710ac1-754e-45f9-bee3-ce3f64615aaf-0-00001.parquet|881               |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
"""

spark.sql(
    """
        INSERT INTO local.default.rewrite_table VALUES
        (4, 'suraj', 23),
        (5, 'nachiket', 32),
        (6, 'mihir', 24);
    """
)

spark.sql(
    """
        SELECT file_path, file_size_in_bytes
        FROM local.default.rewrite_table.files
    """
).show(truncate=False)

"""
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
|file_path                                                                                                                                                         |file_size_in_bytes|
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
|.../iceberg-warehouse/default/rewrite_table/data/00000-5-0e94c467-5506-4b2d-917f-7bf589eb63fe-0-00001.parquet|867               |
|.../iceberg-warehouse/default/rewrite_table/data/00001-6-0e94c467-5506-4b2d-917f-7bf589eb63fe-0-00001.parquet|888               |
|.../iceberg-warehouse/default/rewrite_table/data/00002-7-0e94c467-5506-4b2d-917f-7bf589eb63fe-0-00001.parquet|867               |
|.../iceberg-warehouse/default/rewrite_table/data/00000-0-6f710ac1-754e-45f9-bee3-ce3f64615aaf-0-00001.parquet|867               |
|.../iceberg-warehouse/default/rewrite_table/data/00001-1-6f710ac1-754e-45f9-bee3-ce3f64615aaf-0-00001.parquet|853               |
|.../iceberg-warehouse/default/rewrite_table/data/00002-2-6f710ac1-754e-45f9-bee3-ce3f64615aaf-0-00001.parquet|881               |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
"""

spark.sql(
    """
        CALL local.system.rewrite_data_files('default.rewrite_table')
    """
)

spark.sql(
    """
        SELECT file_path, file_size_in_bytes
        FROM local.default.rewrite_table.files
    """
).show(truncate=False)

"""
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
|file_path                                                                                                                                                         |file_size_in_bytes|
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
|.../iceberg-warehouse/default/rewrite_table/data/00000-9-d806ca43-88b0-4a6a-8791-83ef75f56067-0-00001.parquet|974               |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
"""

spark.sql(
    """
        INSERT INTO local.default.rewrite_table VALUES 
        (7, 'suraj', 23),
        (8, 'nachiket', 32),
        (9, 'mihir', 24)
    """
)

spark.sql(
    """
        CALL local.system.rewrite_data_files(
          'default.rewrite_table', 
          'sort', 
          'id ASC',  
          MAP(
            'min-input-files', '4', 
            'target-file-size-bytes', '3000'
          )
        )
    """
)

spark.sql(
    """
        SELECT file_path, file_size_in_bytes 
        FROM local.default.rewrite_table.files
    """
).show(truncate=False)

"""
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
|file_path                                                                                                                                                          |file_size_in_bytes|
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
|.../iceberg-warehouse/default/rewrite_table/data/00000-16-398baf9d-b59d-488c-b79a-fd9549107727-0-00001.parquet|955               |
|.../iceberg-warehouse/default/rewrite_table/data/00001-17-398baf9d-b59d-488c-b79a-fd9549107727-0-00001.parquet|971               |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
"""

spark.sql(
    """
        SELECT file_path, lower_bounds, upper_bounds 
        FROM local.default.rewrite_table.files
    """
).show(truncate=False)
"""
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------+---------------------------------------------------------------------------+
|file_path                                                                                                                                                          |lower_bounds                                                               |upper_bounds                                                               |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------+---------------------------------------------------------------------------+
|.../iceberg-warehouse/default/rewrite_table/data/00000-16-398baf9d-b59d-488c-b79a-fd9549107727-0-00001.parquet|{1 -> [01 00 00 00 00 00 00 00], 2 -> [41 6C 69 63 65], 3 -> [16 00 00 00]}|{1 -> [05 00 00 00 00 00 00 00], 2 -> [73 75 72 61 6A], 3 -> [20 00 00 00]}|
|.../iceberg-warehouse/default/rewrite_table/data/00001-17-398baf9d-b59d-488c-b79a-fd9549107727-0-00001.parquet|{1 -> [06 00 00 00 00 00 00 00], 2 -> [6D 69 68 69 72], 3 -> [17 00 00 00]}|{1 -> [09 00 00 00 00 00 00 00], 2 -> [73 75 72 61 6A], 3 -> [20 00 00 00]}|
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------+---------------------------------------------------------------------------+

"""