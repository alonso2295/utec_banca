from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *


schema_producto_prestamo = StructType([
    StructField("codigo", StringType(),  False),
    StructField("descripcion", StringType(),    True),
    StructField("operation", StringType(),  True),
    StructField("event_ts", TimestampType(),  True)
])

@dp.table(
    name="raw.sch_dataentry_tb.producto_prestamo",
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
                    "/Volumes/raw/sch_dataentry_tb/vol_dataentry/_schemas/producto_prestamo")
            .load("/Volumes/raw/sch_dataentry_tb/vol_dataentry/catalogos/producto_prestamo/")
            .withColumn("_source_metadata", F.col("_metadata"))
            .withColumn("_ingested_at", F.current_timestamp())
    )



