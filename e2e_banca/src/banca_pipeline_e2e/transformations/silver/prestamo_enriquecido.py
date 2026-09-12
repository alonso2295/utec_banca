
################################################################
## Tabla con el historico de estados de los registro de prestamo
################################################################

from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE_CATALOG = spark.conf.get("bronze_catalog")
SILVER_CATALOG = spark.conf.get("silver_catalog")
GOLD_CATALOG = spark.conf.get("gold_catalog")



@dp.materialized_view(
    name=f"{SILVER_CATALOG}.sch_app_utecbanca_tb.h_prestamo",
    comment="Historico de prestamos enriquecido con producto"
)
def h_prestamo():

    prestamo = spark.read.table(f"{BRONZE_CATALOG}.sch_app_utecbanca_tb.prestamo")

    producto = spark.read.table(
        f"{SILVER_CATALOG}.sch_catalogos_tb.m_producto_prestamo"
    )

    return (
        prestamo.alias("p")
        .join(
            producto.alias("prd"),
            F.col("p.producto_codigo") == F.col("prd.codigo"),
            "left"
        )
        .select(
            F.col("p.prestamo_id"),
            F.col("p.cliente_id"),
            F.col("p.producto_codigo"),
            F.col("prd.descripcion").alias("producto"),
            F.col("p.estado_prestamo_codigo"),
            F.col("p.monto_original"),
            F.col("p.tasa_anual"),
            F.col("p.numero_cuotas"),
            F.col("p.fecha_desembolso"),
            F.col("p.fec_modificacion"),

            # Mantiene la historia SCD2 generada en Raw
            F.col("p.__START_AT"),
            F.col("p.__END_AT")
        )
    )

##########################################################
## Tabla con el ultimo estado de los registro de prestamo
##########################################################
@dp.materialized_view(
    name=f"{SILVER_CATALOG}.sch_app_utecbanca_tb.m_prestamo",
    comment="Estado actual de prestamos enriquecido con producto"
)
def m_prestamo():

    prestamo = (
        spark.read.table(f"{BRONZE_CATALOG}.sch_app_utecbanca_tb.prestamo")
        .filter(F.col("__END_AT").isNull())
        .alias("p")
    )

    producto = (
        spark.read.table(f"{SILVER_CATALOG}.sch_catalogos_tb.m_producto_prestamo")
        .alias("prd")
    )

    return (
        prestamo
        .join(
            producto,
            F.col("p.producto_codigo") == F.col("prd.codigo"),
            "left"
        )
        .select(
            F.col("p.prestamo_id"),
            F.col("p.cliente_id"),
            F.col("p.producto_codigo"),
            F.col("prd.descripcion").alias("producto"),
            F.col("p.estado_prestamo_codigo"),
            F.col("p.monto_original"),
            F.col("p.tasa_anual"),
            F.col("p.numero_cuotas"),
            F.col("p.fecha_desembolso"),
            F.col("p.fec_modificacion")
        )
    )