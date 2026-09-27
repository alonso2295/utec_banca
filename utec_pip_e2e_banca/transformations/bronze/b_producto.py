from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

schema_producto = StructType([
  StructField("codigo", StringType()),
  StructField("descripcion", StringType()),
  StructField("operation", StringType()),
  StructField("event_ts", TimestampType()),
])


@dp.table(
    name="desa_bronze.utec_banca_pe.bronze_producto",
    comment="Bronze table for producto"
)
def bronze_producto():
    return (
        spark.readStream
            .format("cloudFiles")
            .schema(schema_producto)
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudSchemaLocation", "/Volumes/desa_bronze/utec_banca_pe/vol_landing/banca/_schemas/producto")
            .load("/Volumes/desa_bronze/utec_banca_pe/vol_landing/banca/catalogos/producto/")
            .withColumn("_source_metadata", F.col("_metadata"))
            .withColumn("_ingestion_at", F.current_timestamp())
    )

