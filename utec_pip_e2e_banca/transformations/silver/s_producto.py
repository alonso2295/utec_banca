from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *


dp.create_streaming_table(
    name=f"desa_silver.catalogos.m_producto",
    comment="Catalogo de productos — estado actual (SCD Type 1, sin historia)"
)

dp.create_auto_cdc_flow(
    target=f"desa_silver.catalogos.m_producto",
    source=f"desa_bronze.utec_banca_pe.bronze_producto",
    keys=["codigo"],
    sequence_by=F.col("event_ts"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["_source_metadata", "operation", "event_ts", "_ingestion_at"],
    stored_as_scd_type=1
)

# Para la tabla historica aplicaremos SCD Type 2
dp.create_streaming_table(
    name=f"desa_silver.catalogos.h_producto",
    comment="Catalogo de productos con SCD Type 2, para ver los cambios durante la historia"
)

dp.create_auto_cdc_flow(
    target=f"desa_silver.catalogos.h_producto",
    source=f"desa_bronze.utec_banca_pe.bronze_producto",
    keys=["codigo"],
    sequence_by=F.col("event_ts"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["_source_metadata", "operation", "event_ts", "_ingestion_at"],
    stored_as_scd_type=2
)