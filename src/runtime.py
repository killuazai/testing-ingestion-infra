"""Runtime adapter for local VS Code and Databricks."""

from __future__ import annotations

import os
import re
import shutil
import uuid
from pathlib import Path

from src import config

IDENTIFIER = re.compile(r"^[A-Za-z0-9_-]+$")


def is_databricks() -> bool:
    """Return whether this process is running in Databricks Runtime."""
    return (
        bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))
        or Path("/databricks").is_dir()
    )


def local_data_root() -> Path:
    """Return the configurable local data root."""
    configured = os.environ.get("BUILDABIDA_DATA_ROOT")
    return (
        Path(configured).expanduser().resolve()
        if configured
        else config.REPOSITORY_ROOT / "data"
    )


def landing_root() -> Path:
    """Return the source landing root for the active runtime."""
    if is_databricks():
        return Path(f"/Volumes/{config.CATALOG}/{config.SOURCE_SCHEMA}/landing")
    return local_data_root() / "landing"


def table_root(schema: str) -> Path:
    """Return the local Parquet root for one schema."""
    return local_data_root() / "tables" / schema


def get_spark():
    """Return the active Spark session or create one local session."""
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if is_databricks():
        return SparkSession.builder.getOrCreate()

    warehouse = local_data_root() / "spark-warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    return (
        SparkSession.builder.master("local[*]")
        .appName("buildabida-ingestion")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.cpu_count() or 4)
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def parameter(name: str, default: str, label: str) -> str:
    """Read a Databricks widget or a BUILDABIDA_* environment variable."""
    if is_databricks():
        from databricks.sdk.runtime import dbutils

        if name not in dbutils.widgets.getAll():
            dbutils.widgets.text(name, default, label)
        return dbutils.widgets.get(name)
    return os.environ.get(f"BUILDABIDA_{name.upper()}", default)


def show(dataframe, rows: int = 20) -> None:
    """Render a DataFrame in Databricks or print it locally."""
    if is_databricks():
        from databricks.sdk.runtime import display

        display(dataframe)
    else:
        dataframe.show(rows, truncate=False)


def _safe_identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe table identifier: {value!r}")
    return value


def quoted_table(schema: str, table: str) -> str:
    """Return a safely quoted three-part table name."""
    catalog = _safe_identifier(config.CATALOG)
    schema = _safe_identifier(schema)
    table = _safe_identifier(table)
    return f"`{catalog}`.`{schema}`.`{table}`"


def table_label(schema: str, table: str) -> str:
    """Return the active table destination for logs."""
    if is_databricks():
        return quoted_table(schema, table)
    return str((table_root(schema) / _safe_identifier(table)).resolve())


def table_exists(schema: str, table: str, spark=None) -> bool:
    """Return whether one managed snapshot exists."""
    if is_databricks():
        active_spark = spark or get_spark()
        return active_spark.catalog.tableExists(quoted_table(schema, table))
    return (table_root(schema) / _safe_identifier(table)).is_dir()


def publish_table(dataframe, schema: str, table: str) -> None:
    """Atomically replace one Delta or local Parquet snapshot."""
    if is_databricks():
        (
            dataframe.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(quoted_table(schema, table))
        )
        return

    root = table_root(schema).resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = (root / _safe_identifier(table)).resolve()
    if target.parent != root:
        raise ValueError(f"Unsafe local table target: {target}")

    staging = root / f".{table}.staging-{uuid.uuid4().hex}"
    backup = root / f".{table}.backup-{uuid.uuid4().hex}"
    moved_existing = False
    try:
        dataframe.write.mode("overwrite").parquet(str(staging))
        if target.exists():
            target.rename(backup)
            moved_existing = True
        staging.rename(target)
    except OSError:
        if moved_existing and backup.exists() and not target.exists():
            backup.rename(target)
        raise
    else:
        if backup.exists():
            shutil.rmtree(backup)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def read_table(spark, schema: str, table: str):
    """Read one active managed snapshot."""
    if is_databricks():
        return spark.table(quoted_table(schema, table))
    return spark.read.parquet(str(table_root(schema) / _safe_identifier(table)))


def append_table(dataframe, schema: str, table: str) -> None:
    """Append run history without using row-by-row writes."""
    if is_databricks():
        dataframe.write.format("delta").mode("append").saveAsTable(
            quoted_table(schema, table)
        )
        return

    spark = dataframe.sparkSession
    if table_exists(schema, table, spark):
        current = read_table(spark, schema, table)
        dataframe = current.unionByName(dataframe, allowMissingColumns=False)
    publish_table(dataframe, schema, table)
