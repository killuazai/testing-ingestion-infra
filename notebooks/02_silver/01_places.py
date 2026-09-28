# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: places
# MAGIC Creates the current, unique PSGC place master.

# COMMAND ----------

import sys
from pathlib import Path


def repository_root() -> Path:
    starts = [Path.cwd()]
    if "__file__" in globals():
        starts.insert(0, Path(__file__).resolve().parent)
    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / "src").is_dir():
                return candidate
    raise RuntimeError("Could not find repository root")


ROOT = repository_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark import StorageLevel
from pyspark.sql import functions as F

from src import config, runtime

spark = runtime.get_spark()

# COMMAND ----------

bronze = runtime.read_table(spark, config.BRONZE_SCHEMA, config.TABLE_PSGC)
bronze.createOrReplaceTempView("bronze_psgc")
candidate = spark.sql(
    """
    SELECT
      psgc_code,
      trim(name) AS name,
      lower(trim(geographic_level)) AS geographic_level,
      nullif(trim(correspondence_code), '') AS correspondence_code,
      nullif(trim(old_name), '') AS old_name,
      nullif(trim(city_classification), '') AS city_classification,
      nullif(trim(income_classification), '') AS income_classification,
      nullif(trim(urban_rural), '') AS urban_rural,
      population_2024,
      upper(regexp_replace(trim(name), '[[:space:]]+', ' ')) AS normalized_name,
      run_id AS source_run_id,
      load_ts
    FROM bronze_psgc
    """
)
candidate.persist(StorageLevel.DISK_ONLY)
source_count = bronze.count()
stats = candidate.agg(
    F.count("*").alias("rows"),
    F.sum(
        F.when(
            F.col("psgc_code").isNull()
            | ~F.col("psgc_code").rlike(r"^[0-9]{10}$")
            | F.col("name").isNull()
            | (F.length("name") == 0),
            1,
        ).otherwise(0)
    ).alias("invalid_required"),
    F.countDistinct("psgc_code").alias("distinct_codes"),
).first()
if (
    stats.rows != source_count
    or stats.invalid_required
    or stats.distinct_codes != stats.rows
):
    candidate.unpersist()
    raise RuntimeError(f"Silver places stop check failed: {stats.asDict()}")

try:
    runtime.publish_table(candidate, config.SILVER_SCHEMA, config.TABLE_PLACES)
finally:
    candidate.unpersist()
print(
    f"SUCCESS {config.TABLE_PLACES}: {stats.rows} rows -> "
    f"{runtime.table_label(config.SILVER_SCHEMA, config.TABLE_PLACES)}"
)
