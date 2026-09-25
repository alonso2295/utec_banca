{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "cf676c83-de8c-4827-baa2-d972d70a7224",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Sesion 03 – Ingesta Incremental con Auto Loader\n",
    "## Certificacion Databricks Data Engineer Associate\n",
    "\n",
    "**Runtime minimo:** Databricks 13.3 LTS  \n",
    "**Plataforma:** Azure Databricks con Unity Catalog habilitado  \n",
    "**Dataset:** Ordenes de compra – Distribuidora automotriz (3 lotes CSV)\n",
    "\n",
    "### Pasos del laboratorio\n",
    "1. Configuracion de rutas y constantes\n",
    "2. Verificar archivos en el Volume\n",
    "3. Lote 1: pipeline base con esquema inferido y schema evolution activado\n",
    "4. Lote 2: nuevo campo `prioridad_envio` absorbido por `addNewColumns`\n",
    "5. Lote 3: campo totalmente fuera del esquema fijo capturado por `rescuedDataColumn`\n",
    "6. Historial Delta y estadisticas finales\n",
    "\n",
    "> **Diferencia clave entre Paso 4 y Paso 5:**  \n",
    "> Paso 4 usa esquema dinamico (`addNewColumns`): columnas nuevas se agregan automaticamente.  \n",
    "> Paso 5 usa esquema fijo (`StructType` explicito): campos fuera del esquema van a `_rescued_data`."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "bc94df18-781a-4dc1-9f09-143bc54978e4",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Instrucciones de carga de archivos\n",
    "\n",
    "Sube los archivos al Volume antes de ejecutar:\n",
    "\n",
    "```\n",
    "/Volumes/dbassociate/default/vol_landing/ordenes/lote1/ordenes_lote1_20240301.csv\n",
    "/Volumes/dbassociate/default/vol_landing/ordenes/lote2/ordenes_lote2_20240302.csv\n",
    "/Volumes/dbassociate/default/vol_landing/ordenes/lote3/ordenes_lote3_corrupto_20240303.csv\n",
    "```\n",
    "\n",
    "En clase: subir lote1 primero. Lote2 y lote3 se agregan en sus pasos correspondientes."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "1f5f7057-dcab-4c23-a3ca-bd76d1d8441e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Paso 1 – Configuracion de rutas y constantes"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "94f47bf0-e24f-45ce-8f32-f56882147004",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# =============================================================\n",
    "# RUTAS ESTANDAR DEL LABORATORIO\n",
    "# =============================================================\n",
    "\n",
    "CATALOGO        = \"dbassociate\"\n",
    "ESQUEMA         = \"default\"\n",
    "VOL_LANDING     = f\"/Volumes/{CATALOGO}/{ESQUEMA}/vol_landing\"\n",
    "\n",
    "SOURCE_PATH     = f\"{VOL_LANDING}/ordenes\"\n",
    "CHECKPOINT_PATH = f\"{VOL_LANDING}/checkpoints/ordenes_autoloader\"\n",
    "SCHEMA_PATH     = f\"{VOL_LANDING}/schema_store/ordenes_autoloader\"\n",
    "\n",
    "# Checkpoint separado para el pipeline de esquema fijo (Paso 5)\n",
    "CHECKPOINT_PATH_FIXED = f\"{VOL_LANDING}/checkpoints/ordenes_fixed_schema\"\n",
    "\n",
    "TABLA_DESTINO   = f\"`{CATALOGO}`.`{ESQUEMA}`.bronze_ordenes\"\n",
    "TABLA_CUARENTENA = f\"`{CATALOGO}`.`{ESQUEMA}`.bronze_ordenes_cuarentena\"\n",
    "\n",
    "print(f\"SOURCE_PATH          : {SOURCE_PATH}\")\n",
    "print(f\"CHECKPOINT_PATH      : {CHECKPOINT_PATH}\")\n",
    "print(f\"CHECKPOINT_PATH_FIXED: {CHECKPOINT_PATH_FIXED}\")\n",
    "print(f\"SCHEMA_PATH          : {SCHEMA_PATH}\")\n",
    "print(f\"TABLA_DESTINO        : {TABLA_DESTINO}\")\n",
    "print()\n",
    "print(\"ATENCION: checkpointLocation != schemaLocation. Nunca usar el mismo path para ambos.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "0f8bba40-1c9d-4556-bee2-ba5e2b972c0b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Paso 2 – Verificar archivos en el Volume"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "45f4f3d7-8804-45dd-ac9b-0657aec8e1cf",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "archivos = dbutils.fs.ls(SOURCE_PATH)\n",
    "for a in archivos:\n",
    "    print(a.path)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "ccfe9286-0e7c-4ba2-8004-9f3514a8f865",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Preview del lote 1\n",
    "preview = spark.read.option(\"header\", True).csv(f\"{SOURCE_PATH}/lote1/\")\n",
    "# preview = spark.read.option(\"header\", True).format(\"csv\").load(f\"{SOURCE_PATH}/lote1/\")\n",
    "\n",
    "print(f\"Filas: {preview.count()} | Columnas: {len(preview.columns)}\")\n",
    "print(preview.columns)\n",
    "#preview.limit(3).display()\n",
    "display(preview.limit(3))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "1bc9a212-acd7-44c0-a570-5f0768c570ba",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Paso 3 – Configurar y ejecutar pipeline: Lote 1\n",
    "\n",
    "Esquema inferido + `addNewColumns` + `rescuedDataColumn`.  \n",
    "`_metadata.file_name` es la forma correcta de capturar el nombre del archivo fuente en DBR 13.3+."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "e2dbc975-5a14-48d6-9062-b38d30ee856f",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import current_timestamp, col\n",
    "\n",
    "df_stream1 = (\n",
    "    spark.readStream\n",
    "    .format(\"cloudFiles\") #Volume -> ADLS/AWS S3\n",
    "    .option(\"cloudFiles.format\", \"csv\")\n",
    "    .option(\"cloudFiles.schemaLocation\", SCHEMA_PATH)\n",
    "    .option(\"cloudFiles.schemaEvolutionMode\", \"addNewColumns\")\n",
    "    .option(\"cloudFiles.maxFilesPerTrigger\", 10)\n",
    "    .option(\"rescuedDataColumn\", \"_rescued_data\")\n",
    "    .option(\"header\", \"true\")\n",
    "    .option(\"inferSchema\", \"true\")\n",
    "    .load(SOURCE_PATH)\n",
    "    .withColumn(\"_ingestion_timestamp\", current_timestamp())\n",
    "    .withColumn(\"_source_file\", col(\"_metadata.file_name\"))  # correcto en DBR 13.3+\n",
    ")\n",
    "\n",
    "print(\"Schema del stream (lote 1):\")\n",
    "df_stream1.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "26a53fe6-7bec-4851-9c07-b63c3554c74b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "spark.sql(f\"\"\"\n",
    "    CREATE TABLE IF NOT EXISTS {TABLA_DESTINO}\n",
    "    USING DELTA\n",
    "    COMMENT 'Tabla bronze de ordenes - ingesta con Auto Loader'\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "e385b161-74c5-475e-a3a5-708f22035abd",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "query1 = (\n",
    "    df_stream1.writeStream\n",
    "    .format(\"delta\")\n",
    "    .outputMode(\"append\")\n",
    "    .option(\"checkpointLocation\", CHECKPOINT_PATH)\n",
    "    .option(\"mergeSchema\", \"true\")  # necesario cuando el esquema puede evolucionar\n",
    "    .trigger(availableNow=True)\n",
    "    .toTable(f\"{CATALOGO}.{ESQUEMA}.bronze_ordenes\")\n",
    ")\n",
    "query1.awaitTermination()\n",
    "print(\"Lote 1 procesado\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "5429de84-18be-4bd5-b3a8-bce515878ddd",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "df_v1 = spark.table(f\"{CATALOGO}.{ESQUEMA}.bronze_ordenes\")\n",
    "print(f\"Filas en tabla: {df_v1.count()}\")\n",
    "print(f\"Columnas: {df_v1.columns}\")\n",
    "df_v1.display()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "4863098d-8bd6-4c9e-8ed0-59394c186277",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Paso 4 – Lote 2: schema evolution con `addNewColumns`\n",
    "\n",
    "**ACCION REQUERIDA:** Sube `ordenes_lote2_20240302.csv` a `{SOURCE_PATH}/lote2/`\n",
    "\n",
    "El Lote 2 tiene una columna nueva: `prioridad_envio` (NORMAL / EXPRESS / URGENTE).  \n",
    "Con `schemaEvolutionMode = addNewColumns`, Auto Loader detecta la columna y la agrega al esquema  \n",
    "y a la tabla Delta automaticamente. Las filas del Lote 1 quedaran con NULL en esa columna."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "04b44ec6-ec25-4d21-9a81-81e5932de624",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Verificar que lote2 esta disponible\n",
    "try:\n",
    "    for a in dbutils.fs.ls(f\"{SOURCE_PATH}/lote2/\"):\n",
    "        print(a.path)\n",
    "except:\n",
    "    print(f\"Sube el archivo a {SOURCE_PATH}/lote2/ antes de continuar\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "1e34623c-d258-487c-a85c-a0b32c9d5809",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "CHECKPOINT_PATH"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "height": "163.272705078125",
     "inputWidgets": {},
     "nuid": "06e0942d-9cff-4ae3-9534-7c03ce27360f",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": "",
     "width": "1158"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "select * from cloud_files_state('/Volumes/dbassociate/default/vol_landing/checkpoints/ordenes_autoloader')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "18bd8ab5-8458-457c-b787-77c3431cd815",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Relanzar con el mismo checkpoint: Auto Loader solo procesara lote2\n",
    "df_stream2 = (\n",
    "    spark.readStream\n",
    "    .format(\"cloudFiles\")\n",
    "    .option(\"cloudFiles.format\", \"csv\")\n",
    "    .option(\"cloudFiles.schemaLocation\", SCHEMA_PATH)\n",
    "    .option(\"cloudFiles.schemaEvolutionMode\", \"addNewColumns\")\n",
    "    .option(\"cloudFiles.maxFilesPerTrigger\", 10)\n",
    "    .option(\"rescuedDataColumn\", \"_rescued_data\")\n",
    "    .option(\"header\", \"true\")\n",
    "    .option(\"inferSchema\", \"true\")\n",
    "    .load(SOURCE_PATH)\n",
    "    .withColumn(\"_ingestion_timestamp\", current_timestamp())\n",
    "    .withColumn(\"_source_file\", col(\"_metadata.file_name\"))\n",
    ")\n",
    "\n",
    "query2 = (\n",
    "    df_stream2.writeStream\n",
    "    .format(\"delta\")\n",
    "    .outputMode(\"append\")\n",
    "    .option(\"checkpointLocation\", CHECKPOINT_PATH)\n",
    "    .option(\"mergeSchema\", \"true\")\n",
    "    .trigger(availableNow=True)\n",
    "    .toTable(f\"{CATALOGO}.{ESQUEMA}.bronze_ordenes\")\n",
    ")\n",
    "query2.awaitTermination()\n",
    "print(\"Lote 2 procesado\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "57ef33c6-7c67-41d6-a238-17f075f014d2",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "df_v2 = spark.table(f\"{CATALOGO}.{ESQUEMA}.bronze_ordenes\")\n",
    "print(f\"Total filas: {df_v2.count()}\")\n",
    "print(f\"Columnas: {df_v2.columns}\")\n",
    "print()\n",
    "# prioridad_envio debe ser NULL para lote1 y tener valor para lote2\n",
    "print(\"Distribucion de prioridad_envio por lote:\")\n",
    "df_v2.groupBy(\"lote_proceso\", \"prioridad_envio\").count().orderBy(\"lote_proceso\", \"prioridad_envio\").show(20)\n",
    "print()\n",
    "print(\"Verificacion de no reprocesamiento (lote1 no debe aparecer de nuevo):\")\n",
    "df_v2.groupBy(\"lote_proceso\").count().orderBy(\"lote_proceso\").show()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "7f98b883-261d-4cda-9641-1fc94b8868a5",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Paso 5 – Lote 3: esquema fijo y `rescuedDataColumn`\n",
    "\n",
    "**ACCION REQUERIDA:** Sube `ordenes_lote3_corrupto_20240303.csv` a `{SOURCE_PATH}/lote3/`\n",
    "\n",
    "Este paso usa un **esquema explicito fijo** (`StructType`), no inferencia dinamica.  \n",
    "El Lote 3 contiene un campo adicional: `campo_extra_inesperado`.  \n",
    "Con esquema fijo, ese campo no puede agregarse al esquema, por lo que Auto Loader  \n",
    "lo captura en la columna `_rescued_data` como JSON.\n",
    "\n",
    "> **Por que un checkpoint separado:**  \n",
    "> Este pipeline tiene una configuracion distinta (esquema fijo vs. inferido).  \n",
    "> Compartir checkpoint con el pipeline anterior causaria conflictos de estado.  \n",
    "> En produccion, pipelines con configuraciones distintas siempre tienen checkpoints distintos.\n",
    "\n",
    "> **Por que una tabla distinta:**  \n",
    "> La tabla `bronze_ordenes` ya tiene el esquema expandido por lote2.  \n",
    "> Para demostrar `rescuedDataColumn` limpiamente usamos `bronze_ordenes_cuarentena`  \n",
    "> con esquema fijo basado en lote1 (sin `prioridad_envio`)."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "a6d64287-5373-4afb-866b-77ff8782e021",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "try:\n",
    "    for a in dbutils.fs.ls(f\"{SOURCE_PATH}/\"):\n",
    "        print(a.path)\n",
    "except:\n",
    "    print(f\"Sube el archivo a {SOURCE_PATH}/lote3/ antes de continuar\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "d8565076-c238-400a-a390-460e10d31347",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType\n",
    "\n",
    "# Esquema fijo basado en lote1 (18 columnas conocidas, sin prioridad_envio)\n",
    "# Cualquier campo fuera de este esquema ira a _rescued_data\n",
    "schema_fijo = StructType([\n",
    "    StructField(\"orden_id\",                StringType(),  True),\n",
    "    StructField(\"lote_proceso\",            StringType(),  True),\n",
    "    StructField(\"fecha_orden\",             StringType(),  True),\n",
    "    StructField(\"cliente_id\",              StringType(),  True),\n",
    "    StructField(\"cliente_nombre\",          StringType(),  True),\n",
    "    StructField(\"ciudad_destino\",          StringType(),  True),\n",
    "    StructField(\"producto_sku\",            StringType(),  True),\n",
    "    StructField(\"producto_nombre\",         StringType(),  True),\n",
    "    StructField(\"categoria\",               StringType(),  True),\n",
    "    StructField(\"almacen_origen\",          StringType(),  True),\n",
    "    StructField(\"cantidad\",                IntegerType(), True),\n",
    "    StructField(\"precio_unitario\",         DoubleType(),  True),\n",
    "    StructField(\"descuento_pct\",           IntegerType(), True),\n",
    "    StructField(\"precio_final_unitario\",   DoubleType(),  True),\n",
    "    StructField(\"total_linea\",             DoubleType(),  True),\n",
    "    StructField(\"canal_venta\",             StringType(),  True),\n",
    "    StructField(\"estado_orden\",            StringType(),  True),\n",
    "    StructField(\"dias_entrega_prometidos\", IntegerType(), True),\n",
    "])\n",
    "\n",
    "# Con esquema fijo NO se usa schemaLocation ni schemaEvolutionMode\n",
    "# Los campos fuera del esquema (prioridad_envio, campo_extra_inesperado) van a _rescued_data\n",
    "df_stream3 = (\n",
    "    spark.readStream\n",
    "    .format(\"cloudFiles\")\n",
    "    .option(\"cloudFiles.format\", \"csv\")\n",
    "    .option(\"cloudFiles.maxFilesPerTrigger\", 10)\n",
    "    .option(\"rescuedDataColumn\", \"_rescued_data\")\n",
    "    .option(\"header\", \"true\")\n",
    "    .schema(schema_fijo)\n",
    "    .load(f\"{SOURCE_PATH}/lote3/\")  # solo lote3 para este pipeline\n",
    "    .withColumn(\"_ingestion_timestamp\", current_timestamp())\n",
    "    .withColumn(\"_source_file\", col(\"_metadata.file_name\"))\n",
    ")\n",
    "\n",
    "print(\"Schema del stream con esquema fijo:\")\n",
    "df_stream3.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "97f32903-279b-42ca-a04c-8f024b13d6c5",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "CHECKPOINT_PATH_FIXED"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "fbabb626-3270-4914-8283-fcb97a48b9ac",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "query3 = (\n",
    "    df_stream3.writeStream\n",
    "    .format(\"delta\")\n",
    "    .outputMode(\"append\")\n",
    "    .option(\"checkpointLocation\", CHECKPOINT_PATH_FIXED)\n",
    "    .option(\"mergeSchema\", \"true\")\n",
    "    .trigger(availableNow=True)\n",
    "    .toTable(f\"{CATALOGO}.{ESQUEMA}.bronze_ordenes_cuarentena\")\n",
    ")\n",
    "query3.awaitTermination()\n",
    "print(\"Lote 3 procesado\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "1d2574ec-d34f-45de-86af-fe6cc3769a5e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "df_cuarentena = spark.table(f\"{CATALOGO}.{ESQUEMA}.bronze_ordenes_cuarentena\")\n",
    "print(f\"Total filas en tabla cuarentena: {df_cuarentena.count()}\")\n",
    "print(f\"Columnas: {df_cuarentena.columns}\")\n",
    "print()\n",
    "\n",
    "# _rescued_data debe tener contenido: prioridad_envio y campo_extra_inesperado\n",
    "# ambos estan fuera del esquema fijo\n",
    "df_rescatados = df_cuarentena.filter(df_cuarentena[\"_rescued_data\"].isNotNull())\n",
    "print(f\"Filas con _rescued_data no nulo: {df_rescatados.count()}\")\n",
    "print()\n",
    "print(\"Contenido de _rescued_data (campos fuera del esquema fijo):\")\n",
    "df_rescatados.select(\"orden_id\", \"_rescued_data\").show(10, truncate=False)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "2df60610-c2a7-4d62-ac08-a6b85d2c6b13",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Paso 6 – Historial Delta y estadisticas finales"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "360c61ff-825d-473e-a38c-ff86ae02935e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Historial de escrituras en la tabla principal\n",
    "spark.sql(f\"DESCRIBE HISTORY {TABLA_DESTINO}\").select(\n",
    "    \"version\", \"timestamp\", \"operation\", \"operationParameters\"\n",
    ").show(10, truncate=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "b9c9f2aa-9617-4dd9-9f33-0f8b66fcccc0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "print(\"Resumen final – tabla bronze_ordenes (lotes 1 y 2):\")\n",
    "spark.sql(f\"\"\"\n",
    "    SELECT\n",
    "        lote_proceso,\n",
    "        COUNT(*) AS total_ordenes,\n",
    "        COUNT(DISTINCT cliente_id) AS clientes_unicos,\n",
    "        ROUND(SUM(total_linea), 2) AS facturacion_total,\n",
    "        SUM(CASE WHEN prioridad_envio IS NOT NULL THEN 1 ELSE 0 END) AS con_prioridad_envio\n",
    "    FROM {TABLA_DESTINO}\n",
    "    GROUP BY lote_proceso\n",
    "    ORDER BY lote_proceso\n",
    "\"\"\").show(truncate=False)\n",
    "\n",
    "print(\"Resumen – tabla bronze_ordenes_cuarentena (lote 3, esquema fijo):\")\n",
    "spark.sql(f\"\"\"\n",
    "    SELECT\n",
    "        lote_proceso,\n",
    "        COUNT(*) AS total_registros,\n",
    "        COUNT(_rescued_data) AS con_rescued_data\n",
    "    FROM {TABLA_CUARENTENA}\n",
    "    GROUP BY lote_proceso\n",
    "\"\"\").show(truncate=False)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "8f9ad878-073c-4fa2-a514-2575d52c0569",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Limpieza del entorno (opcional)\n",
    "\n",
    "Descomentar para reiniciar el laboratorio desde cero."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "1037c550-15d3-4bc1-80c3-2e0225664d6f",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# spark.sql(f\"DROP TABLE IF EXISTS {TABLA_DESTINO}\")\n",
    "# spark.sql(f\"DROP TABLE IF EXISTS {TABLA_CUARENTENA}\")\n",
    "# dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)\n",
    "# dbutils.fs.rm(CHECKPOINT_PATH_FIXED, recurse=True)\n",
    "# dbutils.fs.rm(SCHEMA_PATH, recurse=True)\n",
    "# print(\"Entorno limpiado\")\n",
    "\n",
    "print(\"Celda de limpieza lista (comentada).\")"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": null,
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "mostRecentlyExecutedCommandWithImplicitDF": {
     "commandId": 8355041768671346,
     "dataframes": [
      "_sqldf"
     ]
    },
    "pythonIndentUnit": 4
   },
   "notebookName": "sesion03_laboratorio",
   "widgets": {}
  },
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
