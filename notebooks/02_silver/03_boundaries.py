# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: boundaries
# MAGIC Applies reviewed PSGC code overrides and checks every shape against
# MAGIC current places.

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
from pyspark.sql import types as T

from src import config, runtime

spark = runtime.get_spark()

# COMMAND ----------

bronze = runtime.read_table(spark, config.BRONZE_SCHEMA, config.TABLE_BOUNDARIES)
places = runtime.read_table(spark, config.SILVER_SCHEMA, config.TABLE_PLACES).select(
    "psgc_code",
    "name",
    "geographic_level",
)
override_schema = T.StructType(
    [
        T.StructField("old_psgc_code", T.StringType(), True),
        T.StructField("current_psgc_code", T.StringType(), True),
        T.StructField("reason", T.StringType(), True),
        T.StructField("source_reference", T.StringType(), True),
    ]
)
overrides = (
    spark.read.option("header", True)
    .schema(override_schema)
    .csv(str(config.PSGC_OVERRIDE_PATH))
    .filter(F.length(F.trim("old_psgc_code")) > 0)
)
override_stats = overrides.agg(
    F.count("*").alias("rows"),
    F.countDistinct("old_psgc_code").alias("distinct_codes"),
).first()
if override_stats.rows != override_stats.distinct_codes:
    raise RuntimeError("PSGC override file has duplicate old_psgc_code values")

bronze.createOrReplaceTempView("bronze_boundaries")
places.createOrReplaceTempView("silver_places")
overrides.createOrReplaceTempView("psgc_overrides")
candidate = spark.sql(
    """
    WITH current_codes AS (
      SELECT /*+ BROADCAST(override) */
        boundary.*,
        coalesce(override.current_psgc_code, boundary.psgc_code)
          AS current_psgc_code,
        override.reason AS code_override_reason
      FROM bronze_boundaries boundary
      LEFT JOIN psgc_overrides override
        ON boundary.psgc_code = override.old_psgc_code
    )
    SELECT /*+ BROADCAST(place) */
      boundary.current_psgc_code AS psgc_code,
      boundary.psgc_code AS source_psgc_code,
      place.name AS place_name,
      coalesce(place.geographic_level, lower(boundary.administrative_level))
        AS administrative_level,
      boundary.geometry_json,
      boundary.properties_json,
      boundary.source_psgc_version,
      boundary.source_boundary_version,
      boundary.code_override_reason,
      CASE
        WHEN place.psgc_code IS NOT NULL THEN 'MATCHED'
        ELSE 'NOT_IN_CURRENT_PSGC'
      END AS psgc_match_status,
      boundary.run_id AS source_run_id,
      boundary.load_ts
    FROM current_codes boundary
    LEFT JOIN silver_places place
      ON boundary.current_psgc_code = place.psgc_code
    """
)
candidate.persist(StorageLevel.DISK_ONLY)
source_count = bronze.count()
stats = candidate.agg(
    F.count("*").alias("rows"),
    F.countDistinct("psgc_code").alias("distinct_codes"),
    F.sum(
        F.when(
            F.col("psgc_code").isNull() | F.col("geometry_json").isNull(),
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
    raise RuntimeError(f"Silver boundaries stop check failed: {stats.asDict()}")

try:
    runtime.publish_table(
        candidate,
        config.SILVER_SCHEMA,
        config.TABLE_SILVER_BOUNDARIES,
    )
finally:
    candidate.unpersist()
print(
    f"SUCCESS {config.TABLE_SILVER_BOUNDARIES}: {stats.rows} rows -> "
    f"{runtime.table_label(config.SILVER_SCHEMA, config.TABLE_SILVER_BOUNDARIES)}"
)
