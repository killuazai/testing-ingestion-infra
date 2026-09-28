# Databricks notebook source
# MAGIC %md
# MAGIC # Set up Buildabida storage
# MAGIC Creates Unity Catalog objects in Databricks or local folders in VS Code.

# COMMAND ----------

import sys
from pathlib import Path


def repository_root() -> Path:
    starts = [Path.cwd()]
    if "__file__" in globals():
        starts.insert(0, Path(__file__).resolve().parent)
    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / "src").is_dir() and (candidate / "config").is_dir():
                return candidate
    raise RuntimeError("Could not find repository root")


ROOT = repository_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import config, runtime

# COMMAND ----------

if runtime.is_databricks():
    spark = runtime.get_spark()
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{config.CATALOG}`")
    spark.sql(f"USE CATALOG `{config.CATALOG}`")
    for schema, comment in [
        (config.SOURCE_SCHEMA, "Raw source files and responses"),
        (config.BRONZE_SCHEMA, "Source-shaped data plus ingestion lineage"),
        (config.SILVER_SCHEMA, "Clean, typed, deduplicated and conformed data"),
        ("03-gold", "Reserved for analysis-ready marts"),
        (config.VALIDATION_SCHEMA, "Data-quality results for every run"),
    ]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}` COMMENT {comment!r}")
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS `{config.SOURCE_SCHEMA}`.landing "
        "COMMENT 'Immutable source artifacts grouped by ingestion run'"
    )
    runtime.show(spark.sql("SHOW SCHEMAS"))
else:
    landing = runtime.landing_root()
    directories = [
        landing / "dpwh_projects",
        landing / "flood_control",
        landing / "psgc",
        landing / "population_2024",
        landing / "boundaries" / "bettergov",
        runtime.table_root(config.BRONZE_SCHEMA),
        runtime.table_root(config.SILVER_SCHEMA),
        runtime.table_root(config.VALIDATION_SCHEMA),
    ]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"READY {directory}")
