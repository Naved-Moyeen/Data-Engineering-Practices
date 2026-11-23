"""Read source files and write raw Delta snapshots."""
import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from .bronze_config import BRONZE_SCHEMA, get_enabled_sources
except ImportError:
    from bronze_config import BRONZE_SCHEMA, get_enabled_sources


def new_load_id() -> str:
    """Unique, sortable id for one bronze load run."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]


def load_metadata(df: DataFrame, source_path: str, load_id: str) -> DataFrame:
    """Add load metadata without changing source columns."""
    source_file = source_path.rstrip("/").split("/")[-1]
    return (
        df
        .withColumn("source_file", F.lit(source_file))
        .withColumn("load_id", F.lit(load_id))
        .withColumn("ingestion_ts", F.current_timestamp())
    )


def read_source(spark: SparkSession, config: dict) -> DataFrame:
    """Read one source exactly as configured."""
    reader = spark.read.format(config["source_format"].lower())
    for key, value in config.get("options", {}).items():
        reader = reader.option(key, value)
    return reader.load(config["source_path"])


def write_bronze_table(df: DataFrame, target_table: str) -> None:
    """Replace the Bronze table with the current source snapshot."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.columnMapping.mode", "name")
        .option("delta.minReaderVersion", "2")
        .option("delta.minWriterVersion", "5")
        .saveAsTable(target_table)
    )


def validate_source_configs(configs: list) -> None:
    """Check source settings before writing any Bronze tables."""
    names, targets = set(), set()
    allowed_formats = {"csv", "json", "excel"}
    identifier = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    for config in configs:
        required = {"source_name", "source_format", "source_path", "target_table"}
        missing = required - config.keys()
        if missing:
            raise ValueError(f"Source configuration is missing: {sorted(missing)}")

        for field in required:
            if not isinstance(config[field], str) or not config[field].strip():
                raise ValueError(f"Source configuration field {field!r} must be a non-empty string")

        name = config["source_name"].casefold()
        target = config["target_table"]
        source_format = config["source_format"].casefold()
        if not isinstance(config.get("options", {}), dict):
            raise ValueError(f"Reader options must be a dictionary for {name}")
        if name in names:
            raise ValueError(f"Duplicate source name: {config['source_name']}")
        if target in targets:
            raise ValueError(f"Duplicate target table: {target}")
        if source_format not in allowed_formats:
            raise ValueError(f"Unsupported source format: {source_format}")
        if not config["source_path"].strip():
            raise ValueError(f"Source path is empty for {config['source_name']}")
        if not target.startswith(BRONZE_SCHEMA + "."):
            raise ValueError(f"Target must be inside {BRONZE_SCHEMA}: {target}")
        if not all(identifier.fullmatch(part) for part in target.split(".")):
            raise ValueError(f"Invalid target table name: {target}")
        names.add(name)
        targets.add(target)


def run_bronze_load(spark: SparkSession, configs: list = None, load_id: str = None) -> dict:
    """Load configured source snapshots into Bronze Delta tables."""
    configs = configs if configs is not None else get_enabled_sources()
    validate_source_configs(configs)
    load_id = load_id or new_load_id()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

    summary = {"load_id": load_id, "tables": {}}
    for config in configs:
        raw = read_source(spark, config)
        stamped = load_metadata(raw, config["source_path"], load_id)
        write_bronze_table(stamped, config["target_table"])
        summary["tables"][config["source_name"]] = stamped.count()

    return summary
