CREATE OR REFRESH MATERIALIZED VIEW ${silver_catalog}.clientes.m_cliente
    (
        CONSTRAINT tipdoc_no_nulo EXPECT (cod_tipodoc IS NOT NULL)
    ) 
COMMENT 'Maestra de clientes - estado actual'
AS
SELECT
    cliente_id
    ,tipo_documento_codigo as cod_tipodoc
    ,trim(nombres) as nombre
    ,trim(apellidos) as apellido
    ,fecha_nacimiento as fec_nacimiento
    ,fecha_alta as fec_alta_usuario
FROM ${bronze_catalog}.core_banca.cliente
WHERE __END_AT is null
