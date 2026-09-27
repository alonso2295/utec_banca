from copy import deepcopy
from pathlib import Path
import re
from typing import Any, Dict, Mapping

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.streaming import StreamingQuery


def _merge_dicts(
    base_config: Mapping[str, Any],
    specific_config: Mapping[str, Any],
) -> Dict[str, Any]:
    """Combina dos diccionarios sin modificar los objetos originales."""
    result = deepcopy(dict(base_config))
    for key, value in specific_config.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
            result[key] = _merge_dicts(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _safe_name(value: str) -> str:
    """Convierte el nombre de un tópico en un identificador seguro."""
    return re.sub(r"[^A-Za-z0-9_]", "_", value)


def load_config(yaml_path: str, environment: str, topic: str) -> Dict[str, Any]:
    """Carga y combina la configuración del ambiente y del tópico solicitado."""
    path = Path(yaml_path)
    if not path.exists():
        raise ValueError(f"No existe el archivo de configuración: {path}")

    with path.open("r", encoding="utf-8") as file:
        content = yaml.safe_load(file) or {}

    environments = content.get("environments", {})
    topics = content.get("topics", {})

    if environment not in environments:
        available = ", ".join(sorted(environments)) or "ninguno"
        raise ValueError(
            f"Ambiente no configurado: {environment}. Disponibles: {available}"
        )

    if topic not in topics:
        available = ", ".join(sorted(topics)) or "ninguno"
        raise ValueError(f"Tópico no configurado: {topic}. Disponibles: {available}")

    topic_config = topics[topic] or {}
    if not topic_config.get("enabled", True):
        raise ValueError(f"El tópico está deshabilitado: {topic}")

    environment_config = environments[environment]
    config = _merge_dicts(
        environment_config,
        environment_config.get("defaults", {}),
    )
    config = _merge_dicts(config, topic_config)
    config["environment"] = environment
    config["topic"] = topic

    validate_config(config)
    return config


def validate_config(config: Mapping[str, Any]) -> None:
    """Valida los campos mínimos antes de iniciar el flujo de streaming."""
    required_fields = (
        "kafka",
        "schema_registry",
        "storage",
        "target_table",
        "serialization",
        "starting_offsets",
        "max_offsets_per_trigger",
        "fail_on_data_loss",
        "output_mode",
        "avro",
        "trigger",
    )
    missing_fields = [field for field in required_fields if field not in config]
    if missing_fields:
        raise ValueError(f"Faltan campos obligatorios: {', '.join(missing_fields)}")

    if config["serialization"].lower() != "avro":
        raise ValueError("La implementación actual solo admite serialización Avro.")


def _get_secret(dbutils: Any, reference: Mapping[str, str]) -> str:
    """Obtiene un secreto mediante la referencia declarada en el YAML."""
    if "key" not in reference:
        raise ValueError("La referencia del secreto debe contener el campo 'key'.")
    return dbutils.secrets.get(**dict(reference))


def get_target_table(config: Mapping[str, Any]) -> str:
    """Construye el nombre completo de la tabla Delta de destino."""
    storage = config["storage"]
    return (
        f"{storage['catalog']}."
        f"{storage['schema']}."
        f"{config['target_table']}"
    )


def get_checkpoint(config: Mapping[str, Any]) -> str:
    """Obtiene el checkpoint explícito o genera uno a partir del tópico."""
    if config.get("checkpoint_location"):
        return config["checkpoint_location"]
    base_path = config["storage"]["checkpoint_base"].rstrip("/")
    return f"{base_path}/{_safe_name(config['topic'])}"


def get_query_name(config: Mapping[str, Any]) -> str:
    """Obtiene el nombre de consulta o genera uno a partir del tópico."""
    return config.get(
        "query_name",
        f"bronze_{_safe_name(config['topic'])}_kafka",
    )


def build_kafka_options(
    config: Mapping[str, Any], dbutils: Any
) -> Dict[str, str]:
    """Construye las opciones de conexión y autenticación de Kafka."""
    kafka = config["kafka"]
    password = _get_secret(dbutils, kafka["password_secret"])
    certificate = _get_secret(dbutils, kafka["ca_certificate_secret"])
    jaas = (
        f'{kafka["sasl_login_module"]} required '
        f'username="{kafka["username"]}" password="{password}";'
    )

    return {
        "kafka.bootstrap.servers": kafka["bootstrap_servers"],
        "subscribe": config["topic"],
        "startingOffsets": str(config["starting_offsets"]),
        "maxOffsetsPerTrigger": str(config["max_offsets_per_trigger"]),
        "failOnDataLoss": str(config["fail_on_data_loss"]).lower(),
        "kafka.security.protocol": kafka["security_protocol"],
        "kafka.sasl.mechanism": kafka["sasl_mechanism"],
        "kafka.sasl.jaas.config": jaas,
        "kafka.ssl.truststore.type": "PEM",
        "kafka.ssl.truststore.certificates": certificate,
    }


def read_kafka(
    spark: SparkSession, config: Mapping[str, Any], dbutils: Any
) -> DataFrame:
    """Crea el DataFrame de streaming para el tópico configurado."""
    reader = spark.readStream.format("kafka")
    options = build_kafka_options(config, dbutils)
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load()


def deserialize_avro(
    kafka_df: DataFrame,
    config: Mapping[str, Any],
    dbutils: Any,
) -> DataFrame:
    """Deserializa Avro y agrega metadatos técnicos de Kafka y de ingesta."""
    registry = config["schema_registry"]
    password = _get_secret(dbutils, registry["password_secret"])
    avro = config["avro"]
    topic = config["topic"]
    key_alias = config.get("key_alias", "kafka_key")
    subject = config.get("subject", f"{topic}-value")

    registry_options = {
        "confluent.schema.registry.basic.auth.credentials.source": "USER_INFO",
        "confluent.schema.registry.basic.auth.user.info": (
            f'{registry["username"]}:{password}'
        ),
        "mode": avro["mode"],
        "avroSchemaEvolutionMode": avro["schema_evolution_mode"],
    }

    decoded_df = kafka_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.col("key").cast("string").alias(key_alias),
        from_avro(
            data=F.col("value"),
            jsonFormatSchema=None,
            options=registry_options,
            subject=subject,
            schemaRegistryAddress=registry["url"],
        ).alias("event"),
    )

    return (
        decoded_df.select(
            "event.*",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "kafka_timestamp_type",
            key_alias,
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_environment", F.lit(config["environment"]))
    )


def write_delta(
    events_df: DataFrame, config: Mapping[str, Any]
) -> StreamingQuery:
    """Escribe el flujo en Delta con el checkpoint y trigger configurados."""
    writer = (
        events_df.writeStream.queryName(get_query_name(config))
        .format("delta")
        .outputMode(config["output_mode"])
        .option("checkpointLocation", get_checkpoint(config))
        .option("mergeSchema", str(config.get("merge_schema", True)).lower())
    )

    trigger = config["trigger"]
    if trigger["type"] == "processing_time":
        writer = writer.trigger(processingTime=trigger["interval"])
    elif trigger["type"] == "available_now":
        writer = writer.trigger(availableNow=True)
    else:
        raise ValueError("El trigger debe ser 'processing_time' o 'available_now'.")

    return writer.toTable(get_target_table(config))


def run_ingestion(
    spark: SparkSession, dbutils: Any, config: Mapping[str, Any]
) -> StreamingQuery:
    """Ejecuta en orden las etapas de lectura, deserialización y escritura."""
    kafka_df = read_kafka(spark, config, dbutils)
    events_df = deserialize_avro(kafka_df, config, dbutils)
    return write_delta(events_df, config)
