# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: projects
# MAGIC Unifies project sources by contract ID and assigns city/municipality PSGC
# MAGIC by point-in-polygon.

# COMMAND ----------

import functools
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

from src import config, geospatial, ingestion, runtime

spark = runtime.get_spark()
project_contract = config.source_contracts()["projects"]
bounds = project_contract["philippines_bounds"]
project_rules = spark.createDataFrame(
    [
        (
            project_contract["flood_default_category"],
            float(bounds["minimum_latitude"]),
            float(bounds["maximum_latitude"]),
            float(bounds["minimum_longitude"]),
            float(bounds["maximum_longitude"]),
        )
    ],
    "flood_default_category string, minimum_latitude double, "
    "maximum_latitude double, minimum_longitude double, "
    "maximum_longitude double",
)
project_rules.createOrReplaceTempView("project_rules")

# COMMAND ----------

dpwh = runtime.read_table(spark, config.BRONZE_SCHEMA, config.TABLE_DPWH)
flood = runtime.read_table(spark, config.BRONZE_SCHEMA, config.TABLE_FLOOD)
places = runtime.read_table(spark, config.SILVER_SCHEMA, config.TABLE_PLACES).select(
    "psgc_code",
    "name",
    "geographic_level",
)
boundaries = runtime.read_table(
    spark,
    config.SILVER_SCHEMA,
    config.TABLE_SILVER_BOUNDARIES,
)

dpwh_stats = dpwh.agg(
    F.count("*").alias("rows"),
    F.countDistinct("contract_id").alias("distinct_keys"),
    F.sum(
        F.when(
            F.col("contract_id").isNull() | (F.length(F.trim("contract_id")) == 0),
            1,
        ).otherwise(0)
    ).alias("null_keys"),
).first()
if dpwh_stats.null_keys or dpwh_stats.rows != dpwh_stats.distinct_keys:
    raise RuntimeError(f"DPWH contract IDs are not unique: {dpwh_stats.asDict()}")

dpwh.createOrReplaceTempView("bronze_dpwh")
flood.createOrReplaceTempView("bronze_flood")
dpwh_contracts = spark.sql(
    """
    SELECT
      contract_id, description, category, status, reported_budget, amount_paid,
      progress, infra_year, program_name, source_of_funds, contractor, start_date,
      completion_date, reported_region, deo, latitude, longitude,
      run_id AS dpwh_source_run_id
    FROM bronze_dpwh
    """
)
flood_contracts = spark.sql(
    """
    SELECT
      contract_id,
      min(object_id) AS first_object_id,
      min_by(description, object_id) AS flood_description,
      min_by(infra_type, object_id) AS flood_infra_type,
      min_by(type_of_work, object_id) AS flood_type_of_work,
      min_by(reported_region, object_id) AS flood_reported_region,
      min_by(reported_province, object_id) AS flood_reported_province,
      min_by(reported_municipality, object_id) AS flood_reported_municipality,
      min_by(deo, object_id) AS flood_deo,
      min_by(abc, object_id) AS flood_abc,
      min_by(contract_cost, object_id) AS flood_contract_cost,
      min_by(infra_year, object_id) AS flood_infra_year,
      min_by(start_date, object_id) AS flood_start_date,
      min_by(completion_date, object_id) AS flood_completion_date,
      min_by(contractor, object_id) AS flood_contractor,
      min_by(latitude, object_id) AS flood_latitude,
      min_by(longitude, object_id) AS flood_longitude,
      count(*) AS flood_record_count,
      count(DISTINCT abc) AS flood_distinct_abc_count,
      count(DISTINCT contract_cost) AS flood_distinct_cost_count,
      max(run_id) AS flood_source_run_id
    FROM bronze_flood
    WHERE contract_id IS NOT NULL AND length(trim(contract_id)) > 0
    GROUP BY contract_id
    """
)
dpwh_contracts.createOrReplaceTempView("dpwh_contracts")
flood_contracts.createOrReplaceTempView("flood_contracts")
merged = spark.sql(
    """
    SELECT
      coalesce(dpwh.contract_id, flood.contract_id) AS contract_id,
      coalesce(dpwh.description, flood.flood_description) AS description,
      coalesce(
        dpwh.category,
        CASE WHEN flood.contract_id IS NOT NULL
          THEN rules.flood_default_category END
      ) AS category,
      dpwh.status,
      dpwh.reported_budget,
      flood.flood_abc,
      flood.flood_contract_cost,
      dpwh.amount_paid,
      dpwh.progress,
      coalesce(dpwh.infra_year, flood.flood_infra_year) AS infra_year,
      dpwh.program_name,
      dpwh.source_of_funds,
      coalesce(dpwh.contractor, flood.flood_contractor) AS contractor,
      coalesce(dpwh.start_date, flood.flood_start_date) AS start_date,
      coalesce(dpwh.completion_date, flood.flood_completion_date)
        AS completion_date,
      coalesce(dpwh.reported_region, flood.flood_reported_region)
        AS reported_region,
      flood.flood_reported_province AS reported_province,
      flood.flood_reported_municipality AS reported_municipality,
      coalesce(dpwh.deo, flood.flood_deo) AS deo,
      coalesce(dpwh.latitude, flood.flood_latitude) AS latitude,
      coalesce(dpwh.longitude, flood.flood_longitude) AS longitude,
      flood.flood_infra_type,
      flood.flood_type_of_work,
      coalesce(flood.flood_record_count, 0) AS flood_record_count,
      coalesce(
        flood.flood_distinct_abc_count > 1
          OR flood.flood_distinct_cost_count > 1,
        false
      ) AS flood_value_conflict_flag,
      dpwh.contract_id IS NOT NULL AS in_dpwh_source,
      flood.contract_id IS NOT NULL AS in_flood_source,
      CASE
        WHEN dpwh.contract_id IS NOT NULL AND flood.contract_id IS NOT NULL
          THEN 'BOTH'
        WHEN dpwh.contract_id IS NOT NULL THEN 'DPWH'
        ELSE 'FLOOD'
      END AS project_source,
      dpwh.dpwh_source_run_id,
      flood.flood_source_run_id
    FROM dpwh_contracts dpwh
    FULL OUTER JOIN flood_contracts flood
      ON dpwh.contract_id = flood.contract_id
    CROSS JOIN project_rules rules
    """
)

flood_only_count = flood_contracts.join(
    dpwh_contracts.select("contract_id"),
    "contract_id",
    "left_anti",
).count()
expected_projects = dpwh_stats.rows + flood_only_count
if merged.count() != expected_projects:
    raise RuntimeError(
        "Project full join did not reconcile to DPWH plus flood-only keys"
    )

# COMMAND ----------

boundary_contract = config.source_contracts()["boundaries"]
level_conditions = [
    F.lower("administrative_level").contains(level.casefold())
    for level in boundary_contract["match_levels"]
]
level_filter = functools.reduce(lambda left, right: left | right, level_conditions)
boundary_rows = (
    boundaries.filter(level_filter & (F.col("psgc_match_status") == "MATCHED"))
    .select("psgc_code", "geometry_json")
    .collect()
)
geocoded = geospatial.assign_psgc_codes(
    spark,
    merged,
    boundary_rows,
    int(boundary_contract["maximum_broadcast_features"]),
)

load_ts = ingestion.utc_now()
run_id = ingestion.make_run_id(load_ts)
geocoded.createOrReplaceTempView("geocoded_projects")
places.createOrReplaceTempView("silver_places")
candidate = spark.sql(
    """
    SELECT /*+ BROADCAST(place) */
      project.contract_id,
      project.description,
      project.category,
      project.status,
      project.reported_budget,
      project.flood_abc,
      project.flood_contract_cost,
      project.amount_paid,
      project.progress,
      project.infra_year,
      project.program_name,
      project.source_of_funds,
      project.contractor,
      project.start_date,
      project.completion_date,
      project.reported_region,
      project.reported_province,
      project.reported_municipality,
      project.deo,
      project.latitude,
      project.longitude,
      project.flood_infra_type,
      project.flood_type_of_work,
      project.flood_record_count,
      project.flood_value_conflict_flag,
      project.in_dpwh_source,
      project.in_flood_source,
      project.project_source,
      project.dpwh_source_run_id,
      project.flood_source_run_id,
      project.psgc_code,
      place.name AS place_name,
      place.geographic_level AS place_level,
      CASE
        WHEN project.latitude IS NULL OR project.longitude IS NULL
          THEN 'NO_COORDINATES'
        WHEN project.place_match_count > 1 THEN 'AMBIGUOUS_BOUNDARY'
        WHEN project.psgc_code IS NULL THEN 'NO_BOUNDARY_MATCH'
        WHEN place.psgc_code IS NULL THEN 'NOT_IN_CURRENT_PSGC'
        ELSE 'MATCHED'
      END AS psgc_match_status,
      project.amount_paid = 0 AS amount_paid_zero_flag,
      project.progress IS NOT NULL
        AND (project.progress < 0 OR project.progress > 100)
        AS progress_out_of_range_flag,
      project.latitude IS NOT NULL
        AND project.longitude IS NOT NULL
        AND (
          project.latitude < rules.minimum_latitude
          OR project.latitude > rules.maximum_latitude
          OR project.longitude < rules.minimum_longitude
          OR project.longitude > rules.maximum_longitude
        ) AS coordinate_outside_philippines_flag,
      project.amount_paid IS NOT NULL
        AND project.reported_budget IS NOT NULL
        AND project.amount_paid > project.reported_budget
        AS amount_paid_over_budget_flag
    FROM geocoded_projects project
    LEFT JOIN silver_places place ON project.psgc_code = place.psgc_code
    CROSS JOIN project_rules rules
    """
).select(
    "*",
    F.lit(load_ts).cast("timestamp").alias("silver_load_ts"),
    F.lit(run_id).alias("silver_run_id"),
)
candidate.persist(StorageLevel.DISK_ONLY)
stats = candidate.agg(
    F.count("*").alias("rows"),
    F.countDistinct("contract_id").alias("distinct_keys"),
    F.sum(
        F.when(
            F.col("contract_id").isNull() | (F.length(F.trim("contract_id")) == 0),
            1,
        ).otherwise(0)
    ).alias("null_keys"),
).first()
if (
    stats.rows != expected_projects
    or stats.distinct_keys != stats.rows
    or stats.null_keys
):
    candidate.unpersist()
    raise RuntimeError(f"Silver projects stop check failed: {stats.asDict()}")

try:
    runtime.publish_table(candidate, config.SILVER_SCHEMA, config.TABLE_PROJECTS)
finally:
    candidate.unpersist()
print(
    f"SUCCESS {config.TABLE_PROJECTS}: {stats.rows} rows -> "
    f"{runtime.table_label(config.SILVER_SCHEMA, config.TABLE_PROJECTS)}"
)
