# Databricks notebook source
# MAGIC %md
# MAGIC # Topico Transacciones Tarjeta
# MAGIC
# MAGIC El ambiente se obtiene de la variable `ENVIRONMENT`. El tópico se recibe
# MAGIC como parámetro para reutilizar el notebook en distintas tareas.

# COMMAND ----------

import os
from pathlib import Path

dbutils.widgets.text("topic", "banco.transacciones_tarjeta", "Tópico Kafka")
dbutils.widgets.text("config_path", "", "kafka_streams/config/config.yml")

# environment = os.getenv("ENVIRONMENT", "").strip().lower()
environment = "dev"
topic = dbutils.widgets.get("topic").strip()
config_path_widget = dbutils.widgets.get("config_path").strip()

if not environment:
    raise ValueError("La variable de entorno ENVIRONMENT no está configurada.")
if not topic:
    raise ValueError("El parámetro topic es obligatorio.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga de las funciones de ingesta

# COMMAND ----------

# DBTITLE 1,Carga de módulo de ingesta
from kafka_streams.src.kafka_ingestion import *

# COMMAND ----------

# La ruta explícita es útil cuando config.yml está en Workspace Files.
# En un Repo, la ruta se obtiene a partir de la carpeta actual del notebook.
config_path = (
    config_path_widget
    if config_path_widget
    else str((Path.cwd().parent / "config" / "config.yml").resolve())
)

config = load_config(
    yaml_path=config_path,
    environment=environment,
    topic=topic,
)

print(f"Ambiente: {config['environment']}")
print(f"Tópico: {config['topic']}")
print(f"Destino: {get_target_table(config)}")
print(f"Checkpoint: {get_checkpoint(config)}")

# COMMAND ----------

query = run_ingestion(
    spark=spark,
    dbutils=dbutils,
    config=config,
)

print(f"Stream iniciado: {query.name}")
print(f"Identificador de consulta: {query.id}")

# COMMAND ----------

# Mantiene activa la tarea y propaga los errores del flujo al Job.
# Con available_now, espera hasta finalizar el lote incremental.
query.awaitTermination()