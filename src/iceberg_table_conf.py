"""
This file demonstrate how we can see various configuration wrt iceberg table
"""

from src.utils import get_spark_session

spark = get_spark_session()

spark.sql("SHOW CATALOGS").show()

spark.sql("SHOW DATABASES IN local").show()

spark.sql("SELECT * FROM local.default.my_table.snapshots").show(20, False)

print("Catalog: {catalog}".format(catalog=spark.conf.get("spark.sql.catalog.local")))

print("Catalog type: {catalog_type}".format(catalog_type=spark.conf.get("spark.sql.catalog.local.type")))

print("Iceberg Warehouse: {iceberg_warehouse}".format(iceberg_warehouse=spark.conf.get("spark.sql.catalog.local.warehouse")))

latest_snapshot_id = spark.sql("SELECT snapshot_id FROM local.default.my_table.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0][0]
print("Latest Snapshot id: {}".format(latest_snapshot_id))

spark.sql(
    "SELECT * FROM local.default.my_table.manifests WHERE added_snapshot_id "
    "= {snapshot_id}".format(snapshot_id=latest_snapshot_id)
).show(20, False)


spark.sql("SELECT * FROM local.default.my_table.files").show(20, False)