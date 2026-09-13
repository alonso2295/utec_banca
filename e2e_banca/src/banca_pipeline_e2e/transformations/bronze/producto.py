from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *


BRONZE_CATALOG = spark.conf.get("bronze_catalog")


schema_producto_prestamo = StructType([
    StructField("codigo", StringType(),  False),
    StructField("descripcion", StringType(),    True),
    StructField("operation", StringType(),  True),
    StructField("event_ts", TimestampType(),  True)
])

@dp.table(
    name=f"{BRONZE_CATALOG}.core_banca.producto",
    comment="Eventos CDC de cambios en clientes (INSERT/UPDATE/DELETE).",
    table_properties={"quality": "raw"},
)
def raw_producto_prestamo():
    return (
        spark.readStream
            .format("cloudFiles")
            .schema(schema_producto_prestamo)
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation",
                    f"/Volumes/{BRONZE_CATALOG}/core_banca/vol_entry/_schemas/producto")
            .load(f"/Volumes/{BRONZE_CATALOG}/core_banca/vol_entry/catalogos/producto/")
            .withColumn("_source_metadata", F.col("_metadata"))
            .withColumn("_ingested_at", F.current_timestamp())
    )



