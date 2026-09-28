# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: population
# MAGIC Conforms official 2024 population rows to the current PSGC place master.

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

bronze = runtime.read_table(spark, config.BRONZE_SCHEMA, config.TABLE_POPULATION)
places = runtime.read_table(spark, config.SILVER_SCHEMA, config.TABLE_PLACES).select(
    "psgc_code",
    "name",
    "geographic_level",
)
bronze.createOrReplaceTempView("bronze_population")
places.createOrReplaceTempView("silver_places")
candidate = spark.sql(
    """
    SELECT /*+ BROADCAST(place) */
      population.psgc_code,
      place.name AS place_name,
      place.geographic_level,
      population.population,
      population.population_year,
      CASE
        WHEN place.psgc_code IS NOT NULL THEN 'MATCHED'
        ELSE 'NOT_IN_CURRENT_PSGC'
      END AS psgc_match_status,
      population.run_id AS source_run_id,
      population.load_ts
    FROM bronze_population population
    LEFT JOIN silver_places place
      ON population.psgc_code = place.psgc_code
    """
)
candidate.persist(StorageLevel.DISK_ONLY)
source_count = bronze.count()
stats = candidate.agg(
    F.count("*").alias("rows"),
    F.countDistinct("psgc_code").alias("distinct_codes"),
    F.sum(
        F.when(
            F.col("psgc_code").isNull() | (F.col("population") < 0),
            1,
        ).otherwise(0)
    ).alias("invalid_required"),
).first()
if (
    stats.rows != source_count
    or stats.distinct_codes != stats.rows
    or stats.invalid_required
):
    candidate.unpersist()
    raise RuntimeError(f"Silver population stop check failed: {stats.asDict()}")

try:
    runtime.publish_table(
        candidate,
        config.SILVER_SCHEMA,
        config.TABLE_SILVER_POPULATION,
    )
finally:
    candidate.unpersist()
print(
    f"SUCCESS {config.TABLE_SILVER_POPULATION}: {stats.rows} rows -> "
    f"{runtime.table_label(config.SILVER_SCHEMA, config.TABLE_SILVER_POPULATION)}"
)
