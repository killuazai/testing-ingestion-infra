# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Silver
# MAGIC Persists every result, then stops for null keys, duplicate keys, or
# MAGIC row-count loss.

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

from pyspark.sql import functions as F
from pyspark.sql import types as T

from src import config, dq, ingestion, runtime

spark = runtime.get_spark()

# COMMAND ----------

projects = runtime.read_table(spark, config.SILVER_SCHEMA, config.TABLE_PROJECTS)
dpwh = runtime.read_table(spark, config.BRONZE_SCHEMA, config.TABLE_DPWH)
flood = runtime.read_table(spark, config.BRONZE_SCHEMA, config.TABLE_FLOOD).filter(
    F.col("contract_id").isNotNull() & (F.length(F.trim("contract_id")) > 0)
)

results = dq.evaluate(
    projects,
    [
        dq.Check("not_null", "contract_id", "contract_id IS NULL", "stop"),
        dq.Check(
            "between_0_and_100",
            "progress",
            "progress IS NOT NULL AND (progress < 0 OR progress > 100)",
            "flag",
        ),
        dq.Check(
            "inside_philippines_bounds",
            "latitude,longitude",
            "coordinate_outside_philippines_flag = true",
            "flag",
        ),
        dq.Check(
            "matched_to_current_place",
            "psgc_code",
            "psgc_match_status <> 'MATCHED'",
            "flag",
        ),
        dq.Check(
            "not_more_than_reported_budget",
            "amount_paid",
            "amount_paid_over_budget_flag = true",
            "flag",
        ),
        dq.Check(
            "zero_requires_review",
            "amount_paid",
            "amount_paid_zero_flag = true",
            "flag",
        ),
    ],
)

project_count = projects.count()
duplicate_rows = (
    projects.groupBy("contract_id")
    .count()
    .filter(F.col("count") > 1)
    .select(F.sum(F.col("count") - 1).alias("duplicate_rows"))
    .first()
    .duplicate_rows
    or 0
)
results.append(
    {
        "column": "contract_id",
        "check_name": "unique",
        "failed_rows": int(duplicate_rows),
        "total_rows": project_count,
        "percentage": 0.0
        if project_count == 0
        else duplicate_rows / project_count * 100,
        "severity": "stop",
        "status": "PASS" if duplicate_rows == 0 else "FAIL",
    }
)

dpwh_count = dpwh.count()
flood_only_count = (
    flood.select("contract_id")
    .distinct()
    .join(dpwh.select("contract_id"), "contract_id", "left_anti")
    .count()
)
expected_projects = dpwh_count + flood_only_count
row_difference = abs(project_count - expected_projects)
results.append(
    {
        "column": "row_count",
        "check_name": "bronze_to_silver_reconciliation",
        "failed_rows": row_difference,
        "total_rows": expected_projects,
        "percentage": 0.0
        if expected_projects == 0
        else row_difference / expected_projects * 100,
        "severity": "stop",
        "status": "PASS" if row_difference == 0 else "FAIL",
    }
)

# COMMAND ----------

run_id = ingestion.make_run_id()
checked_at = ingestion.utc_now()
result_rows = [
    (
        run_id,
        config.RUN_MODE,
        config.SILVER_SCHEMA,
        config.TABLE_PROJECTS,
        result["column"],
        result["check_name"],
        int(result["failed_rows"]),
        int(result["total_rows"]),
        float(result["percentage"]),
        result["severity"],
        result["status"],
        checked_at,
    )
    for result in results
]
result_schema = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("run_mode", T.StringType(), False),
        T.StructField("table_schema", T.StringType(), False),
        T.StructField("table_name", T.StringType(), False),
        T.StructField("column", T.StringType(), False),
        T.StructField("check_name", T.StringType(), False),
        T.StructField("failed_rows", T.LongType(), False),
        T.StructField("total_rows", T.LongType(), False),
        T.StructField("percentage", T.DoubleType(), False),
        T.StructField("severity", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("checked_at", T.TimestampType(), False),
    ]
)
results_df = spark.createDataFrame(result_rows, result_schema)
runtime.append_table(results_df, config.VALIDATION_SCHEMA, config.TABLE_DQ_RESULTS)
runtime.show(results_df.orderBy("severity", "check_name"), rows=len(results))
dq.require_no_failures(results)
print(f"SUCCESS Silver validation run {run_id}")
