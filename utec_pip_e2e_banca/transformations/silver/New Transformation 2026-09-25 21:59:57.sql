CREATE OR REFRESH MATERIALIZED VIEW desa_silver.catalogos.dummy_producto
AS
SELECT *
FROM desa_bronze.utec_banca_pe.bronze_producto