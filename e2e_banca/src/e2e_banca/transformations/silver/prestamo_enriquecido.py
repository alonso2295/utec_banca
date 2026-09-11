from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view()
def prestamo_enriched():
    """Vista temporal que enriquece prestamos con la descripcion del producto."""
    prestamo = spark.readStream.table("bronze.sch_app_utecbanca_tb.prestamo")
    producto = spark.read.table("silver.sch_catalogos_tb.m_producto_prestamo")
    return (
        prestamo.join(
            producto,
            prestamo["producto_codigo"] == producto["codigo"],
            "left"
        )
        .select(
            prestamo["prestamo_id"],
            prestamo["cliente_id"],
            producto["codigo"],
            producto["descripcion"].alias("producto"),
            prestamo["estado_prestamo_codigo"],
            prestamo["monto_original"],
            prestamo["tasa_anual"],
            prestamo["numero_cuotas"],
            prestamo["fecha_desembolso"],
            prestamo["fec_modificacion"],
        )
    )

dp.create_streaming_table(
    name="silver.sch_app_utecbanca_tb.h_prestamo",
    comment="Historico de prestamos enriquecido con producto — SCD Type 2"
)

dp.create_auto_cdc_flow(
    target="silver.sch_app_utecbanca_tb.h_prestamo",
    source="prestamo_enriched",
    keys=["prestamo_id"],
    sequence_by=F.col("fec_modificacion"),
    stored_as_scd_type=2
)

###################################
## IMPLEMENTACION CON SQL
###################################

# CREATE OR REFRESH TEMPORARY VIEW prestamo_enriched
# COMMENT 'Vista temporal que enriquece prestamos con la descripcion del producto.'
# AS SELECT
#   p.prestamo_id,
#   p.cliente_id,
#   mp.codigo,
#   mp.descripcion AS producto,
#   p.estado_prestamo_codigo,
#   p.monto_original,
#   p.tasa_anual,
#   p.numero_cuotas,
#   p.fecha_desembolso,
#   p.fec_modificacion
# FROM STREAM(bronze.sch_app_utecbanca_tb.prestamo) p
# LEFT JOIN silver.sch_catalogos_tb.m_producto_prestamo mp
#   ON p.producto_codigo = mp.codigo;

# CREATE OR REFRESH STREAMING TABLE silver.sch_app_utecbanca_tb.h_prestamo
# COMMENT 'Historico de prestamos enriquecido con producto — SCD Type 2';

# CREATE FLOW prestamo_cdc_flow AS AUTO CDC INTO silver.sch_app_utecbanca_tb.h_prestamo
# FROM STREAM(prestamo_enriched)
# KEYS (prestamo_id)
# SEQUENCE BY fec_modificacion
# STORED AS SCD TYPE 2;
