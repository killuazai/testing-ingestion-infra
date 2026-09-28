# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: PSGC 2Q 2026
# MAGIC Reads the manually landed official publication workbook using a reviewed
# MAGIC header contract.

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

from src import config, ingestion, runtime

spark = runtime.get_spark()

# COMMAND ----------

source_directory = Path(
    runtime.parameter(
        "psgc_directory",
        str(runtime.landing_root() / "psgc"),
        "PSGC landing directory",
    )
)
requested_file = runtime.parameter(
    "psgc_file",
    "",
    "Official PSGC publication workbook file name",
).strip()
workbooks = ingestion.discover_files(source_directory, {".xlsx", ".xls"})

if requested_file:
    candidates = [path for path in workbooks if path.name == requested_file]
else:
    candidates = [
        path
        for path in workbooks
        if "publication" in path.name.casefold()
        and "summary" not in path.name.casefold()
    ]
if len(candidates) != 1:
    raise ValueError(
        "Select exactly one official PSGC publication workbook with "
        f"BUILDABIDA_PSGC_FILE. Found: {[path.name for path in workbooks]}"
    )
source_file = candidates[0]

contract = config.source_contracts()["psgc"]
frame, layout = ingestion.read_excel_contract(source_file, contract)
if frame.empty:
    raise RuntimeError("The PSGC workbook produced zero records")

load_ts = ingestion.utc_now()
run_id = ingestion.make_run_id(load_ts)
source_system = "Philippine Statistics Authority PSGC"
source_sha256 = ingestion.sha256(source_file.read_bytes())

source_df = spark.createDataFrame(frame)
candidate = source_df.select(
    F.lpad(
        F.regexp_replace(F.trim("psgc_code"), r"\.0$", ""),
        10,
        "0",
    ).alias("psgc_code"),
    F.trim("name").alias("name"),
    F.trim("geographic_level").alias("geographic_level"),
    F.when(
        F.length(F.trim("correspondence_code")) > 0,
        F.lpad(
            F.regexp_replace(F.trim("correspondence_code"), r"\.0$", ""),
            9,
            "0",
        ),
    ).alias("correspondence_code"),
    F.trim("old_name").alias("old_name"),
    F.trim("city_classification").alias("city_classification"),
    F.trim("income_classification").alias("income_classification"),
    F.trim("urban_rural").alias("urban_rural"),
    F.regexp_replace(F.trim("population_2024"), ",", "")
    .cast("long")
    .alias("population_2024"),
    F.col("source_row_number").cast("long"),
    F.col("source_record_json"),
    F.lit(source_file.name).alias("source_file"),
    F.lit(layout["sheet_name"]).alias("source_sheet"),
    F.lit(layout["header_row"]).cast("int").alias("source_header_row"),
    F.lit(source_sha256).alias("source_sha256"),
    F.lit(contract["release"]).alias("source_release"),
    F.lit(load_ts).cast("timestamp").alias("load_ts"),
    F.lit(run_id).alias("run_id"),
    F.lit(source_system).alias("source_system"),
    F.lit(config.PSGC_PAGE_URL).alias("source_url"),
)
candidate.persist(StorageLevel.DISK_ONLY)
stats = candidate.agg(
    F.count("*").alias("rows"),
    F.sum(
        F.when(
            ~F.col("psgc_code").rlike(r"^[0-9]{10}$")
            | F.col("name").isNull()
            | (F.length("name") == 0),
            1,
        ).otherwise(0)
    ).alias("invalid_required"),
).first()
if stats.rows != len(frame) or stats.invalid_required:
    candidate.unpersist()
    raise RuntimeError(f"PSGC Bronze contract failed: {stats.asDict()}")

try:
    runtime.publish_table(candidate, config.BRONZE_SCHEMA, config.TABLE_PSGC)
finally:
    candidate.unpersist()

written = runtime.read_table(spark, config.BRONZE_SCHEMA, config.TABLE_PSGC).count()
if written != len(frame):
    raise RuntimeError(f"PSGC Bronze wrote {written}; expected {len(frame)}")
print(
    f"SUCCESS {config.TABLE_PSGC}: {written} rows -> "
    f"{runtime.table_label(config.BRONZE_SCHEMA, config.TABLE_PSGC)}"
)
