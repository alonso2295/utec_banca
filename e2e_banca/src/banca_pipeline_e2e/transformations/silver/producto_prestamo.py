from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *


BRONZE_CATALOG = spark.conf.get("bronze_catalog")
SILVER_CATALOG = spark.conf.get("silver_catalog")
GOLD_CATALOG = spark.conf.get("gold_catalog")


dp.create_streaming_table(
    name=f"{SILVER_CATALOG}.sch_catalogos_tb.m_producto_prestamo",
    comment="Catalogo de productos — estado actual (SCD Type 1, sin historia)"
)

dp.create_auto_cdc_flow(
    target=f"{SILVER_CATALOG}.sch_catalogos_tb.m_producto_prestamo",
    source="raw.sch_dataentry_tb.producto_prestamo",
    keys=["codigo"],
    sequence_by=F.col("event_ts"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["_source_metadata", "operation", "event_ts", "_ingested_at"],
    stored_as_scd_type=1
)

# Para la tabla historica aplicaremos SCD Type 2
dp.create_streaming_table(
    name=f"{SILVER_CATALOG}.sch_catalogos_tb.h_producto_prestamo",
    comment="Catalogo de productos con SCD Type 2, para ver los cambios durante la historia"
)

dp.create_auto_cdc_flow(
    target=f"{SILVER_CATALOG}.sch_catalogos_tb.h_producto_prestamo",
    source="raw.sch_dataentry_tb.producto_prestamo",
    keys=["codigo"],
    sequence_by=F.col("event_ts"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["_source_metadata", "operation", "event_ts", "_ingested_at"],
    stored_as_scd_type=2
)